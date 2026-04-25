#!/usr/bin/env python3
"""Run a host-native k6 saturation frontier for sync FastAPI CPU endpoints."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict
from pathlib import Path
from typing import Any

try:
    from scripts.host_native_common import (
        append_jsonl,
        sample_process,
        validate_host_native_k6,
        validate_loopback_base_url,
        wall_timeout_seconds,
        write_json,
    )
    from scripts.run_matrix import (
        DEFAULT_K6_SCRIPT,
        RESULTS_ROOT,
        Condition,
        default_conditions,
        parse_condition,
        start_server,
        stop_process,
        wait_for_server,
    )
except ModuleNotFoundError:  # pragma: no cover - supports direct execution from scripts/.
    from host_native_common import (
        append_jsonl,
        sample_process,
        validate_host_native_k6,
        validate_loopback_base_url,
        wall_timeout_seconds,
        write_json,
    )
    from run_matrix import (
        DEFAULT_K6_SCRIPT,
        RESULTS_ROOT,
        Condition,
        default_conditions,
        parse_condition,
        start_server,
        stop_process,
        wait_for_server,
    )


def condition_requires_gil_off(condition: Condition) -> bool:
    return (
        condition.require_gil_disabled
        or condition.gil == "0"
        or "gil-off" in condition.label.lower()
        or "gil_off" in condition.label.lower()
    )


def validate_runtime_for_condition(condition: Condition, runtime: dict[str, Any]) -> None:
    if condition_requires_gil_off(condition) and runtime.get("gil_enabled") is not False:
        raise RuntimeError("condition requires disabled GIL, but /runtime did not confirm it")


def get_json(url: str, timeout: float = 2.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def metric_value(metrics: dict[str, Any], metric: str, field: str) -> float | None:
    payload = metrics.get(metric)
    if not isinstance(payload, dict):
        return None
    values = payload.get("values")
    value = values.get(field) if isinstance(values, dict) else payload.get(field)
    if value is None and field == "rate":
        value = payload.get("value")
    return float(value) if isinstance(value, (int, float)) else None


def extract_k6_metrics(summary: dict[str, Any]) -> dict[str, float]:
    metrics = summary.get("metrics")
    if not isinstance(metrics, dict):
        return {}
    fields = {
        "requests_per_sec": ("http_reqs", "rate"),
        "request_count": ("http_reqs", "count"),
        "duration_p50_ms": ("http_req_duration", "med"),
        "duration_p95_ms": ("http_req_duration", "p(95)"),
        "duration_p99_ms": ("http_req_duration", "p(99)"),
        "duration_max_ms": ("http_req_duration", "max"),
        "failed_requests_rate": ("http_req_failed", "rate"),
        "checks_rate": ("checks", "rate"),
        "dropped_iterations": ("dropped_iterations", "count"),
        "vus_max": ("vus_max", "value"),
    }
    extracted: dict[str, float] = {}
    for output, (metric, field) in fields.items():
        value = metric_value(metrics, metric, field)
        if value is not None:
            extracted[output] = value
    if "duration_p99_ms" not in extracted:
        fallback = extracted.get("duration_max_ms") or extracted.get("duration_p95_ms")
        if fallback is not None:
            extracted["duration_p99_ms"] = fallback
    extracted.setdefault("dropped_iterations", 0.0)
    return extracted


def classify_cell(
    metrics: dict[str, float],
    *,
    target_rps: int,
    min_achieved_ratio: float,
    max_failed_rate: float,
    p99_slo_ms: float,
) -> dict[str, Any]:
    achieved = metrics.get("requests_per_sec", 0.0)
    failed = metrics.get("failed_requests_rate", 1.0)
    checks = metrics.get("checks_rate", 0.0)
    dropped = metrics.get("dropped_iterations", 0.0)
    p99 = metrics.get("duration_p99_ms", float("inf"))
    valid = (
        achieved >= target_rps * min_achieved_ratio
        and failed <= max_failed_rate
        and checks >= 1.0
        and dropped == 0
        and p99 <= p99_slo_ms
    )
    reasons: list[str] = []
    if achieved < target_rps * min_achieved_ratio:
        reasons.append("low_achieved_rps")
    if failed > max_failed_rate:
        reasons.append("http_failures")
    if checks < 1.0:
        reasons.append("check_failures")
    if dropped != 0:
        reasons.append("dropped_iterations")
    if p99 > p99_slo_ms:
        reasons.append("p99_slo")
    return {"valid": valid, "classification": "valid" if valid else "saturated", "reasons": reasons}


def count_tcp_connections(pid: int) -> int | None:
    completed = subprocess.run(
        ["lsof", "-Pan", "-p", str(pid), "-iTCP"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return max(0, len(lines) - 1)


def telemetry_row(app_pid: int, k6_pid: int | None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "timestamp": time.time(),
        "app": sample_process(app_pid),
        "app_tcp_connections": count_tcp_connections(app_pid),
    }
    app_cpu = row["app"].get("cpu_percent")
    if isinstance(app_cpu, (int, float)):
        row["app_effective_cores"] = float(app_cpu) / 100.0
    if k6_pid is not None:
        row["k6"] = sample_process(k6_pid)
        k6_cpu = row["k6"].get("cpu_percent")
        if isinstance(k6_cpu, (int, float)):
            row["k6_effective_cores"] = float(k6_cpu) / 100.0
    return row


def summarize_telemetry(rows: list[dict[str, Any]]) -> dict[str, Any]:
    app_cores = [
        float(row["app_effective_cores"])
        for row in rows
        if isinstance(row.get("app_effective_cores"), (int, float))
    ]
    k6_cores = [
        float(row["k6_effective_cores"])
        for row in rows
        if isinstance(row.get("k6_effective_cores"), (int, float))
    ]
    app_rss = [
        float(row["app"].get("rss_kb"))
        for row in rows
        if isinstance(row.get("app"), dict) and isinstance(row["app"].get("rss_kb"), (int, float))
    ]
    return {
        "samples": len(rows),
        "app_effective_cores_mean": sum(app_cores) / len(app_cores) if app_cores else None,
        "app_effective_cores_max": max(app_cores) if app_cores else None,
        "k6_effective_cores_mean": sum(k6_cores) / len(k6_cores) if k6_cores else None,
        "k6_effective_cores_max": max(k6_cores) if k6_cores else None,
        "app_rss_kb_max": max(app_rss) if app_rss else None,
    }


def run_k6_cell(
    *,
    k6_bin: str,
    script: Path,
    base_url: str,
    target_rps: int,
    duration: str,
    request_timeout: str,
    preallocated_vus: int,
    max_vus: int,
    route_profile: str,
    dataset_size: int,
    summary_path: Path,
    stdout_path: Path,
    stderr_path: Path,
    telemetry_path: Path,
    app_pid: int,
    telemetry_interval: float,
    hard_timeout_s: float,
) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(
        {
            "BASE_URL": base_url,
            "TARGET_RPS": str(target_rps),
            "RATE": str(target_rps),
            "DURATION": duration,
            "REQUEST_TIMEOUT": request_timeout,
            "PREALLOCATED_VUS": str(preallocated_vus),
            "PRE_ALLOCATED_VUS": str(preallocated_vus),
            "MAX_VUS": str(max_vus),
            "ROUTE_PROFILE": route_profile,
            "DATASET_SIZE": str(dataset_size),
            "LARGE_DATASET_SIZE": str(min(dataset_size, 1000)),
        }
    )
    cmd = [
        k6_bin,
        "run",
        "--quiet",
        "--summary-export",
        str(summary_path),
        str(script),
    ]
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    telemetry_rows: list[dict[str, Any]] = []
    started = time.time()
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w",
        encoding="utf-8",
    ) as stderr:
        proc = subprocess.Popen(
            cmd,
            cwd=RESULTS_ROOT.parents[1],
            env=env,
            stdout=stdout,
            stderr=stderr,
            text=True,
            start_new_session=True,
        )
        timed_out = False
        deadline = time.monotonic() + hard_timeout_s
        while True:
            row = telemetry_row(app_pid, proc.pid)
            telemetry_rows.append(row)
            append_jsonl(telemetry_path, row)
            if proc.poll() is not None:
                break
            if time.monotonic() >= deadline:
                timed_out = True
                try:
                    os.killpg(proc.pid, 15)
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        os.killpg(proc.pid, 9)
                    except Exception:
                        proc.kill()
                    proc.wait(timeout=5)
                break
            time.sleep(telemetry_interval)
        returncode = proc.returncode

    return {
        "cmd": cmd,
        "returncode": returncode,
        "timed_out": timed_out,
        "started_at": started,
        "finished_at": time.time(),
        "elapsed_s": time.time() - started,
        "telemetry": summarize_telemetry(telemetry_rows),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--condition", action="append", type=parse_condition)
    parser.add_argument("--thread-tokens", nargs="+", type=int, default=[1, 2, 4, 6, 8, 12, 16])
    parser.add_argument("--target-rps", nargs="+", type=int, default=[1, 2, 3, 4, 6, 8, 12, 16])
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--duration", default="30s")
    parser.add_argument("--request-timeout", default="5s")
    parser.add_argument("--preallocated-vus", type=int, default=32)
    parser.add_argument("--max-vus", type=int, default=64)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dataset-size", type=int, default=20_000)
    parser.add_argument("--cpu-rounds", type=int, default=800_000)
    parser.add_argument("--route-profile", default="sync-cpu")
    parser.add_argument("--k6", default="/opt/homebrew/bin/k6")
    parser.add_argument("--k6-script", type=Path, default=DEFAULT_K6_SCRIPT)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.add_argument("--telemetry-interval", type=float, default=1.0)
    parser.add_argument("--p99-slo-ms", type=float, default=1000.0)
    parser.add_argument("--max-failed-rate", type=float, default=0.001)
    parser.add_argument("--min-achieved-ratio", type=float, default=0.95)
    parser.add_argument("--stop-on-saturation", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    k6_bin = validate_host_native_k6(args.k6)
    base_url = validate_loopback_base_url(args.base_url)
    hard_timeout_s = wall_timeout_seconds(args.duration, args.request_timeout, grace_seconds=15.0)
    conditions = args.condition or default_conditions()
    run_id = time.strftime("frontier-%Y%m%d-%H%M%S")
    out_dir = args.out or RESULTS_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "run_id": run_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "conditions": [asdict(condition) for condition in conditions],
        "thread_tokens": args.thread_tokens,
        "target_rps": args.target_rps,
        "repeat": args.repeat,
        "duration": args.duration,
        "request_timeout": args.request_timeout,
        "hard_timeout_s": hard_timeout_s,
        "workers": args.workers,
        "dataset_size": args.dataset_size,
        "cpu_rounds": args.cpu_rounds,
        "route_profile": args.route_profile,
        "k6": k6_bin,
        "k6_script": str(args.k6_script),
        "base_url": base_url,
        "p99_slo_ms": args.p99_slo_ms,
    }
    write_json(out_dir / "manifest.json", manifest)

    failures: list[dict[str, Any]] = []
    for condition in conditions:
        for thread_tokens in args.thread_tokens:
            saturated = False
            for target_rps in args.target_rps:
                if saturated and args.stop_on_saturation:
                    break
                valid_repeats = 0
                for repeat in range(1, args.repeat + 1):
                    run_name = (
                        f"{condition.label}__threads-{thread_tokens}"
                        f"__rps-{target_rps}__rep-{repeat}"
                    )
                    run_dir = out_dir / run_name
                    run_dir.mkdir(parents=True, exist_ok=True)
                    print(f"[frontier] starting {run_name}", flush=True)
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
                        runtime_before = wait_for_server(base_url, args.startup_timeout)
                        validate_runtime_for_condition(condition, runtime_before)
                        write_json(run_dir / "runtime-before.json", runtime_before)
                        k6_run = run_k6_cell(
                            k6_bin=k6_bin,
                            script=args.k6_script,
                            base_url=base_url,
                            target_rps=target_rps,
                            duration=args.duration,
                            request_timeout=args.request_timeout,
                            preallocated_vus=args.preallocated_vus,
                            max_vus=args.max_vus,
                            route_profile=args.route_profile,
                            dataset_size=args.dataset_size,
                            summary_path=run_dir / "k6-summary.json",
                            stdout_path=run_dir / "k6.stdout.log",
                            stderr_path=run_dir / "k6.stderr.log",
                            telemetry_path=run_dir / "telemetry.jsonl",
                            app_pid=proc.pid,
                            telemetry_interval=args.telemetry_interval,
                            hard_timeout_s=hard_timeout_s,
                        )
                        runtime_after = get_json(f"{base_url}/runtime")
                        validate_runtime_for_condition(condition, runtime_after)
                        write_json(run_dir / "runtime-after.json", runtime_after)
                        summary = {}
                        if (run_dir / "k6-summary.json").exists():
                            with (run_dir / "k6-summary.json").open("r", encoding="utf-8") as file:
                                summary = json.load(file)
                        metrics = extract_k6_metrics(summary)
                        classification = classify_cell(
                            metrics,
                            target_rps=target_rps,
                            min_achieved_ratio=args.min_achieved_ratio,
                            max_failed_rate=args.max_failed_rate,
                            p99_slo_ms=args.p99_slo_ms,
                        )
                        if classification["valid"]:
                            valid_repeats += 1
                        else:
                            saturated = True

                        result = {
                            "run_id": run_name,
                            "condition": asdict(condition),
                            "thread_tokens": thread_tokens,
                            "target_rps": target_rps,
                            "repeat": repeat,
                            "runtime_before": runtime_before,
                            "runtime_after": runtime_after,
                            "k6_run": k6_run,
                            "metrics": metrics,
                            "classification": classification,
                            "telemetry": k6_run["telemetry"],
                        }
                        write_json(run_dir / "cell-summary.json", result)
                        append_jsonl(out_dir / "frontier-results.jsonl", result)
                        if k6_run["timed_out"] or k6_run["returncode"] not in (0, None):
                            failures.append(
                                {
                                    "run": run_name,
                                    "error": "k6 timed out or returned non-zero",
                                    "k6_run": k6_run,
                                }
                            )
                    except Exception as exc:
                        failure = {"run": run_name, "error": repr(exc)}
                        failures.append(failure)
                        write_json(run_dir / "failure.json", failure)
                        print(f"[frontier] failed {run_name}: {exc}", file=sys.stderr, flush=True)
                    finally:
                        stop_process(proc)

                if args.stop_on_saturation and valid_repeats == 0:
                    saturated = True

    write_json(out_dir / "failures.json", failures)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
