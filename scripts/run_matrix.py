#!/usr/bin/env python3
"""Run the FastAPI free-threading benchmark matrix.

The runner intentionally starts one Uvicorn process per condition. It validates
the runtime endpoint before every load run so accidental GIL re-enablement is
captured next to the result data.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RESULTS_ROOT = ROOT / "bench" / "results"
DEFAULT_K6_SCRIPT = ROOT / "bench" / "k6" / "sync-constant-arrival.js"


@dataclass(frozen=True)
class Condition:
    label: str
    python: str
    gil: str | None
    require_gil_disabled: bool = False


def parse_condition(value: str) -> Condition:
    """Parse label=python[,gil=0|1][,require_gil_disabled=true]."""
    if "=" not in value:
        raise argparse.ArgumentTypeError("condition must look like label=python[,gil=0|1]")

    label, rest = value.split("=", 1)
    parts = [part.strip() for part in rest.split(",") if part.strip()]
    if not label or not parts:
        raise argparse.ArgumentTypeError("condition label and python command are required")

    python = parts[0]
    gil: str | None = None
    require_gil_disabled = False
    for part in parts[1:]:
        if part.startswith("gil="):
            gil = part.split("=", 1)[1]
            if gil not in {"0", "1"}:
                raise argparse.ArgumentTypeError("gil must be 0 or 1")
        elif part == "require_gil_disabled=true":
            require_gil_disabled = True
        else:
            raise argparse.ArgumentTypeError(f"unknown condition option: {part}")

    return Condition(label=label, python=python, gil=gil, require_gil_disabled=require_gil_disabled)


def default_conditions() -> list[Condition]:
    return [
        Condition("py312-gil", "python3.12", None),
        Condition("py314-gil", "python3.14", None),
        Condition("py314t-gil-on", "python3.14t", "1"),
        Condition("py314t-gil-off", "python3.14t", "0", require_gil_disabled=True),
    ]


def get_json(url: str, timeout: float = 2.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_server(base_url: str, timeout_s: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            get_json(f"{base_url}/health")
            return get_json(f"{base_url}/runtime")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"server did not become healthy within {timeout_s}s: {last_error}")


def stop_process(proc: subprocess.Popen[Any]) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def start_server(
    condition: Condition,
    *,
    port: int,
    thread_tokens: int,
    workers: int,
    dataset_size: int,
    cpu_rounds: int,
    log_path: Path,
) -> subprocess.Popen[Any]:
    env = os.environ.copy()
    env["THREAD_TOKENS"] = str(thread_tokens)
    env["BENCH_DATASET_SIZE"] = str(dataset_size)
    env["BENCH_CPU_ROUNDS"] = str(cpu_rounds)
    env["PYTHONUNBUFFERED"] = "1"
    if condition.gil is not None:
        env["PYTHON_GIL"] = condition.gil

    cmd = [
        condition.python,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--workers",
        str(workers),
        "--loop",
        "asyncio",
        "--http",
        "h11",
        "--no-access-log",
    ]
    log_file = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        cmd,
        cwd=ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )


def run_k6(
    *,
    k6_bin: str,
    script: Path,
    base_url: str,
    target_rps: int,
    duration: str,
    preallocated_vus: int,
    max_vus: int,
    route_profile: str,
    dataset_size: int,
    request_timeout: str,
    summary_path: Path,
    quiet: bool,
) -> None:
    env = os.environ.copy()
    env.update(
        {
            "BASE_URL": base_url,
            "TARGET_RPS": str(target_rps),
            "RATE": str(target_rps),
            "DURATION": duration,
            "PREALLOCATED_VUS": str(preallocated_vus),
            "PRE_ALLOCATED_VUS": str(preallocated_vus),
            "MAX_VUS": str(max_vus),
            "ROUTE_PROFILE": route_profile,
            "DATASET_SIZE": str(dataset_size),
            "LARGE_DATASET_SIZE": str(min(dataset_size, 1000)),
            "REQUEST_TIMEOUT": request_timeout,
        }
    )
    cmd = [
        k6_bin,
        "run",
    ]
    if quiet:
        cmd.append("--quiet")
    cmd.extend(
        [
        "--summary-export",
        str(summary_path),
        str(script),
        ]
    )
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--condition", action="append", type=parse_condition)
    parser.add_argument("--thread-tokens", nargs="+", type=int, default=[1, 2, 4, 8, 16, 32, 64])
    parser.add_argument("--target-rps", nargs="+", type=int, default=[100, 250, 500, 1000])
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--duration", default="3m")
    parser.add_argument("--preallocated-vus", type=int, default=200)
    parser.add_argument("--max-vus", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dataset-size", type=int, default=100_000)
    parser.add_argument("--cpu-rounds", type=int, default=8_000)
    parser.add_argument("--request-timeout", default="60s")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--load-base-url",
        default=None,
        help="Base URL used by the load generator; defaults to the local server URL.",
    )
    parser.add_argument("--k6", default="k6")
    parser.add_argument("--k6-quiet", action="store_true")
    parser.add_argument("--k6-script", type=Path, default=DEFAULT_K6_SCRIPT)
    parser.add_argument("--route-profile", default="sync-cpu")
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    conditions = args.condition or default_conditions()
    run_id = time.strftime("%Y%m%d-%H%M%S")
    out_dir = args.out or RESULTS_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "run_id": run_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "conditions": [asdict(condition) for condition in conditions],
        "thread_tokens": args.thread_tokens,
        "target_rps": args.target_rps,
        "repeat": args.repeat,
        "duration": args.duration,
        "workers": args.workers,
        "dataset_size": args.dataset_size,
        "cpu_rounds": args.cpu_rounds,
        "request_timeout": args.request_timeout,
        "route_profile": args.route_profile,
    }
    write_json(out_dir / "manifest.json", manifest)

    base_url = f"http://127.0.0.1:{args.port}"
    load_base_url = args.load_base_url or base_url
    failures: list[dict[str, Any]] = []

    for condition in conditions:
        for thread_tokens in args.thread_tokens:
            for target_rps in args.target_rps:
                for repeat in range(1, args.repeat + 1):
                    run_name = (
                        f"{condition.label}__threads-{thread_tokens}"
                        f"__rps-{target_rps}__rep-{repeat}"
                    )
                    run_dir = out_dir / run_name
                    run_dir.mkdir(parents=True, exist_ok=True)
                    print(f"[matrix] starting {run_name}", flush=True)

                    proc = start_server(
                        condition,
                        port=args.port,
                        thread_tokens=thread_tokens,
                        workers=args.workers,
                        dataset_size=args.dataset_size,
                        cpu_rounds=args.cpu_rounds,
                        log_path=run_dir / "server.log",
                    )
                    try:
                        metadata = {
                            "run_id": run_name,
                            "variant": condition.label,
                            "condition": asdict(condition),
                            "thread_tokens": thread_tokens,
                            "target_rps": target_rps,
                            "repeat": repeat,
                            "workers": args.workers,
                            "dataset_size": args.dataset_size,
                            "cpu_rounds": args.cpu_rounds,
                            "request_timeout": args.request_timeout,
                            "route_profile": args.route_profile,
                            "k6_script": str(args.k6_script),
                        }
                        write_json(run_dir / "metadata.json", metadata)
                        runtime = wait_for_server(base_url, args.startup_timeout)
                        write_json(run_dir / "runtime.json", runtime)
                        gil_requirement_failed = (
                            condition.require_gil_disabled
                            and runtime.get("gil_enabled") is not False
                        )
                        if gil_requirement_failed:
                            raise RuntimeError(
                                "condition requires disabled GIL, but /runtime did not confirm it"
                            )

                        run_k6(
                            k6_bin=args.k6,
                            script=args.k6_script,
                            base_url=load_base_url,
                            target_rps=target_rps,
                            duration=args.duration,
                            preallocated_vus=args.preallocated_vus,
                            max_vus=args.max_vus,
                            route_profile=args.route_profile,
                            dataset_size=args.dataset_size,
                            request_timeout=args.request_timeout,
                            summary_path=run_dir / "k6-summary.json",
                            quiet=args.k6_quiet,
                        )
                    except Exception as exc:
                        failure = {"run": run_name, "error": repr(exc)}
                        failures.append(failure)
                        write_json(run_dir / "failure.json", failure)
                        print(f"[matrix] failed {run_name}: {exc}", file=sys.stderr, flush=True)
                    finally:
                        stop_process(proc)

    write_json(out_dir / "failures.json", failures)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
