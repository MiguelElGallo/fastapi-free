#!/usr/bin/env python3
"""Run host-local FastAPI proof batches that measure Python thread parallelism."""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import asdict
from pathlib import Path
from typing import Any

try:
    from scripts.host_native_common import append_csv_row, append_jsonl, write_json
    from scripts.run_matrix import (
        RESULTS_ROOT,
        Condition,
        default_conditions,
        parse_condition,
        start_server,
        stop_process,
        wait_for_server,
    )
except ModuleNotFoundError:  # pragma: no cover - supports direct execution from scripts/.
    from host_native_common import append_csv_row, append_jsonl, write_json
    from run_matrix import (
        RESULTS_ROOT,
        Condition,
        default_conditions,
        parse_condition,
        start_server,
        stop_process,
        wait_for_server,
    )


CSV_FIELDS = [
    "run_id",
    "condition",
    "python",
    "gil",
    "thread_tokens",
    "participants",
    "repeat",
    "batch_id",
    "rounds",
    "successes",
    "failures",
    "elapsed_s",
    "wall_ns",
    "sum_thread_cpu_ns",
    "parallelism_ratio",
    "participants_completed",
    "participants_failed",
    "runtime_python",
    "runtime_py_gil_disabled",
    "runtime_gil_enabled",
]


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


def request_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return json.loads(response.read().decode("utf-8"))


def run_batch(
    *,
    base_url: str,
    participants: int,
    rounds: int,
    timeout_seconds: float,
    ttl_seconds: float,
    request_timeout_s: float,
) -> dict[str, Any]:
    batch = request_json(
        "POST",
        f"{base_url}/proof/batches",
        payload={
            "participants": participants,
            "rounds": rounds,
            "timeout_seconds": timeout_seconds,
            "ttl_seconds": ttl_seconds,
        },
        timeout_s=request_timeout_s,
    )
    batch_id = batch["batch_id"]
    client_barrier = threading.Barrier(participants + 1)
    lock = threading.Lock()
    responses: list[dict[str, Any]] = []
    errors: list[str] = []

    def worker(slot: int) -> None:
        client_barrier.wait(timeout=timeout_seconds)
        started = time.perf_counter()
        try:
            payload = request_json(
                "GET",
                f"{base_url}/proof/batches/{batch_id}/work/{slot}",
                timeout_s=request_timeout_s,
            )
            payload["client_elapsed_ms"] = (time.perf_counter() - started) * 1000
            with lock:
                responses.append(payload)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            with lock:
                errors.append(f"slot={slot} HTTP {exc.code}: {body[:300]}")
        except Exception as exc:
            with lock:
                errors.append(f"slot={slot} {type(exc).__name__}: {exc}")

    threads = [
        threading.Thread(target=worker, args=(slot,), daemon=True)
        for slot in range(participants)
    ]
    for thread in threads:
        thread.start()

    started = time.perf_counter()
    client_barrier.wait(timeout=timeout_seconds)
    for thread in threads:
        thread.join(timeout=request_timeout_s + timeout_seconds)
    elapsed_s = time.perf_counter() - started

    summary = request_json(
        "GET",
        f"{base_url}/proof/batches/{batch_id}",
        timeout_s=request_timeout_s,
    )
    try:
        request_json("DELETE", f"{base_url}/proof/batches/{batch_id}", timeout_s=request_timeout_s)
    except Exception:
        pass

    failures = len(errors) + int(summary.get("participants_failed") or 0)
    successes = participants - failures
    return {
        "batch_id": batch_id,
        "participants": participants,
        "rounds": rounds,
        "elapsed_s": elapsed_s,
        "successes": successes,
        "failures": failures,
        "client_responses": sorted(responses, key=lambda item: item.get("slot", -1)),
        "client_errors": errors,
        "summary": summary,
    }


def flatten_result_for_csv(result: dict[str, Any]) -> dict[str, Any]:
    runtime = result["runtime"]
    batch = result["batch"]
    summary = batch["summary"]
    return {
        "run_id": result["run_id"],
        "condition": result["condition"]["label"],
        "python": result["condition"]["python"],
        "gil": result["condition"]["gil"],
        "thread_tokens": result["thread_tokens"],
        "participants": result["participants"],
        "repeat": result["repeat"],
        "batch_id": batch["batch_id"],
        "rounds": batch["rounds"],
        "successes": batch["successes"],
        "failures": batch["failures"],
        "elapsed_s": batch["elapsed_s"],
        "wall_ns": summary.get("wall_ns"),
        "sum_thread_cpu_ns": summary.get("sum_thread_cpu_ns"),
        "parallelism_ratio": summary.get("parallelism_ratio"),
        "participants_completed": summary.get("participants_completed"),
        "participants_failed": summary.get("participants_failed"),
        "runtime_python": runtime.get("python"),
        "runtime_py_gil_disabled": runtime.get("py_gil_disabled"),
        "runtime_gil_enabled": runtime.get("gil_enabled"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--condition", action="append", type=parse_condition)
    parser.add_argument("--thread-tokens", nargs="+", type=int, default=[16])
    parser.add_argument("--participants", nargs="+", type=int, default=[1, 2, 4, 8, 12, 16])
    parser.add_argument("--repeat", type=int, default=30)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dataset-size", type=int, default=20_000)
    parser.add_argument("--rounds", type=int, default=800_000)
    parser.add_argument("--barrier-timeout", type=float, default=10.0)
    parser.add_argument("--batch-ttl", type=float, default=120.0)
    parser.add_argument("--request-timeout", type=float, default=20.0)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    positive_value_groups = (
        ("thread token", args.thread_tokens),
        ("participant", args.participants),
    )
    for value_name, values in positive_value_groups:
        if any(value < 1 for value in values):
            parser.error(f"{value_name} values must be greater than zero")
    if args.repeat < 1:
        parser.error("--repeat must be greater than zero")
    if args.rounds < 1:
        parser.error("--rounds must be greater than zero")

    conditions = args.condition or default_conditions()
    run_id = time.strftime("parallel-proof-%Y%m%d-%H%M%S")
    out_dir = args.out or RESULTS_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / "results.jsonl"
    csv_path = out_dir / "results.csv"
    base_url = f"http://127.0.0.1:{args.port}"

    manifest = {
        "run_id": run_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "conditions": [asdict(condition) for condition in conditions],
        "thread_tokens": args.thread_tokens,
        "participants": args.participants,
        "repeat": args.repeat,
        "workers": args.workers,
        "dataset_size": args.dataset_size,
        "rounds": args.rounds,
        "barrier_timeout": args.barrier_timeout,
        "batch_ttl": args.batch_ttl,
        "request_timeout": args.request_timeout,
        "base_url": base_url,
        "results_jsonl": str(jsonl_path),
        "results_csv": str(csv_path),
    }
    write_json(out_dir / "manifest.json", manifest)

    failures: list[dict[str, Any]] = []
    for condition in conditions:
        for thread_tokens in args.thread_tokens:
            cell_name = f"{condition.label}__threads-{thread_tokens}"
            cell_dir = out_dir / cell_name
            cell_dir.mkdir(parents=True, exist_ok=True)
            print(f"[parallel-proof] starting server {cell_name}", flush=True)
            proc = start_server(
                condition,
                port=args.port,
                thread_tokens=thread_tokens,
                workers=args.workers,
                dataset_size=args.dataset_size,
                cpu_rounds=args.rounds,
                log_path=cell_dir / "server.log",
            )
            try:
                runtime = wait_for_server(base_url, args.startup_timeout)
                validate_runtime_for_condition(condition, runtime)
                write_json(cell_dir / "runtime.json", runtime)

                for participants in args.participants:
                    for repeat in range(1, args.repeat + 1):
                        proof_run_id = (
                            f"{condition.label}__threads-{thread_tokens}"
                            f"__participants-{participants}__rep-{repeat}"
                        )
                        print(f"[parallel-proof] running {proof_run_id}", flush=True)
                        batch = run_batch(
                            base_url=base_url,
                            participants=participants,
                            rounds=args.rounds,
                            timeout_seconds=args.barrier_timeout,
                            ttl_seconds=args.batch_ttl,
                            request_timeout_s=args.request_timeout,
                        )
                        runtime_after = request_json(
                            "GET",
                            f"{base_url}/runtime",
                            timeout_s=args.request_timeout,
                        )
                        validate_runtime_for_condition(condition, runtime_after)
                        result = {
                            "run_id": proof_run_id,
                            "condition": asdict(condition),
                            "thread_tokens": thread_tokens,
                            "participants": participants,
                            "repeat": repeat,
                            "runtime": runtime,
                            "runtime_after": runtime_after,
                            "batch": batch,
                            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        }
                        append_jsonl(jsonl_path, result)
                        append_csv_row(csv_path, flatten_result_for_csv(result), CSV_FIELDS)
                        if batch["failures"]:
                            failures.append(
                                {
                                    "run": proof_run_id,
                                    "error": f"{batch['failures']} proof failures",
                                    "samples": batch["client_errors"][:10],
                                }
                            )
            except Exception as exc:
                failure = {"run": cell_name, "error": repr(exc)}
                failures.append(failure)
                write_json(cell_dir / "failure.json", failure)
                print(f"[parallel-proof] failed {cell_name}: {exc}", file=sys.stderr, flush=True)
            finally:
                stop_process(proc)

    write_json(out_dir / "failures.json", failures)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
