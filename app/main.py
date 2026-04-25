import json
import os
import platform
import resource
import sys
import sysconfig
import threading
import time
from contextlib import asynccontextmanager
from importlib import metadata
from typing import Any

import anyio.to_thread
from fastapi import Body, FastAPI, HTTPException, Response

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
except ImportError:  # pragma: no cover - exercised only when the optional dependency is absent.
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    Counter = None
    Histogram = None
    generate_latest = None


DATASET_SIZE = int(os.getenv("BENCH_DATASET_SIZE", "10000"))
LARGE_DATASET_SIZE = max(
    1,
    min(DATASET_SIZE, int(os.getenv("BENCH_LARGE_DATASET_SIZE", "1000"))),
)
DEFAULT_THREAD_TOKENS = 40
CPU_ROUNDS = int(os.getenv("BENCH_CPU_ROUNDS", "8000"))
PROOF_BATCH_TTL_SECONDS = float(os.getenv("BENCH_PROOF_BATCH_TTL_SECONDS", "30"))
PROOF_BARRIER_TIMEOUT_SECONDS = float(os.getenv("BENCH_PROOF_BARRIER_TIMEOUT_SECONDS", "2"))
PROOF_MAX_PARTICIPANTS = int(os.getenv("BENCH_PROOF_MAX_PARTICIPANTS", "64"))
PROOF_BATCH_BODY = Body(default=None)


def _make_record(index: int) -> dict[str, Any]:
    key = f"key-{index}"
    return {
        "key": key,
        "index": index,
        "name": f"record-{index}",
        "enabled": index % 3 != 0,
        "score": (index * 17) % 10_000,
        "tags": [f"tag-{index % 7}", f"bucket-{index % 31}"],
    }


DATA: dict[str, dict[str, Any]] = {
    f"key-{index}": _make_record(index) for index in range(DATASET_SIZE)
}

LARGE_DATA: dict[str, dict[str, Any]] = {
    f"key-{index}": {
        **_make_record(index),
        "payload": {
            "values": [(index + offset) % 997 for offset in range(96)],
            "text": (f"payload-{index}-" * 24)[:512],
        },
    }
    for index in range(LARGE_DATASET_SIZE)
}

PREENCODED_DATA: dict[str, bytes] = {
    key: json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    for key, value in DATA.items()
}

_cache_lock = threading.Lock()
_locked_cache: dict[str, dict[str, Any]] = {}
_cow_lock = threading.Lock()
_cow_snapshot: dict[str, dict[str, Any]] = dict(DATA)
_thread_tokens: int | None = None
_otel_enabled = False
_proof_lock = threading.Lock()
_proof_batches: dict[str, dict[str, Any]] = {}
_proof_batch_sequence = 0

if Counter is not None and Histogram is not None:
    REQUEST_COUNT = Counter(
        "fastapi_benchmark_requests_total",
        "Total HTTP requests served by the benchmark app.",
        ("method", "path", "status"),
    )
    REQUEST_LATENCY = Histogram(
        "fastapi_benchmark_request_seconds",
        "HTTP request latency for the benchmark app.",
        ("method", "path"),
    )
else:
    REQUEST_COUNT = None
    REQUEST_LATENCY = None


def _dependency_versions() -> dict[str, str | None]:
    packages = ("fastapi", "starlette", "pydantic", "anyio")
    versions: dict[str, str | None] = {}
    for package in packages:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def _prometheus_enabled() -> bool:
    requested = (
        os.getenv("BENCH_PROMETHEUS_ENABLED") == "1"
        or os.getenv("PROMETHEUS_ENABLED") == "1"
    )
    return requested and generate_latest is not None


def _parse_thread_tokens(value: str | None) -> int | None:
    if value is None or value == "":
        return None

    try:
        tokens = int(value)
    except ValueError as exc:
        raise RuntimeError("THREAD_TOKENS must be an integer") from exc

    if tokens < 1:
        raise RuntimeError("THREAD_TOKENS must be greater than zero")
    return tokens


def _record_for(key: str, source: dict[str, dict[str, Any]]) -> dict[str, Any]:
    try:
        return source[key]
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="key not found") from exc


def _cpu_transform(record: dict[str, Any], rounds: int = CPU_ROUNDS) -> dict[str, Any]:
    mask = (1 << 64) - 1
    seed_values = [
        int(record["index"]),
        int(record["score"]),
        len(record["name"]),
        *(ord(char) for char in record["key"]),
        *(ord(char) for tag in record["tags"] for char in tag),
    ]
    state = 0x9E3779B185EBCA87 ^ seed_values[0]
    checksum = 0

    for index in range(rounds):
        state ^= seed_values[index % len(seed_values)]
        state ^= (state << 13) & mask
        state ^= state >> 7
        state ^= (state << 17) & mask
        state = (state + index + 0xA0761D6478BD642F) & mask
        checksum = (checksum + ((state ^ (index * 0x9E3779B1)) & mask)) & mask

    return {
        "key": record["key"],
        "index": record["index"],
        "checksum": checksum,
        "digest": f"{state:016x}{checksum:016x}",
        "rounds": rounds,
    }


def _copy_on_write_transform(record: dict[str, Any]) -> dict[str, Any]:
    clone = dict(record)
    clone["tags"] = list(record["tags"])
    clone["score"] = int(clone["score"]) + len(clone["tags"])
    clone["copy_on_write"] = True
    return clone


def _aggregate_proof_spans(spans: list[dict[str, Any]]) -> dict[str, Any]:
    ordered_spans = sorted(spans, key=lambda span: int(span["slot"]))
    completed = [
        span
        for span in ordered_spans
        if span.get("error") is None
        and span.get("perf_counter_start_ns") is not None
        and span.get("perf_counter_end_ns") is not None
    ]
    if completed:
        wall_start_ns = min(int(span["perf_counter_start_ns"]) for span in completed)
        wall_end_ns = max(int(span["perf_counter_end_ns"]) for span in completed)
        wall_ns = max(0, wall_end_ns - wall_start_ns)
    else:
        wall_ns = 0

    sum_thread_cpu_ns = sum(
        max(
            0,
            int(span["thread_time_end_ns"]) - int(span["thread_time_start_ns"]),
        )
        for span in completed
        if span.get("thread_time_start_ns") is not None
        and span.get("thread_time_end_ns") is not None
    )
    parallelism_ratio = sum_thread_cpu_ns / wall_ns if wall_ns else 0.0
    return {
        "wall_ns": wall_ns,
        "sum_thread_cpu_ns": sum_thread_cpu_ns,
        "parallelism_ratio": parallelism_ratio,
        "participants_completed": len(completed),
        "spans": ordered_spans,
    }


def _positive_int_from_payload(
    payload: dict[str, Any],
    field: str,
    default: int,
    *,
    maximum: int | None = None,
) -> int:
    raw_value = payload.get(field, default)
    if isinstance(raw_value, bool):
        raise HTTPException(status_code=400, detail=f"{field} must be an integer")
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be an integer") from exc

    if value < 1:
        raise HTTPException(status_code=400, detail=f"{field} must be greater than zero")
    if maximum is not None and value > maximum:
        raise HTTPException(status_code=400, detail=f"{field} must be <= {maximum}")
    return value


def _positive_float_from_payload(payload: dict[str, Any], field: str, default: float) -> float:
    raw_value = payload.get(field, default)
    if isinstance(raw_value, bool):
        raise HTTPException(status_code=400, detail=f"{field} must be a number")
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be a number") from exc

    if value <= 0:
        raise HTTPException(status_code=400, detail=f"{field} must be greater than zero")
    return value


def _cleanup_stale_proof_batches(now_ns: int | None = None) -> list[str]:
    checked_at_ns = now_ns if now_ns is not None else time.perf_counter_ns()
    removed: list[tuple[str, dict[str, Any]]] = []
    with _proof_lock:
        for batch_id, batch in list(_proof_batches.items()):
            if int(batch["expires_at_ns"]) <= checked_at_ns:
                removed.append((batch_id, _proof_batches.pop(batch_id)))

    for _, batch in removed:
        batch["barrier"].abort()
    return [batch_id for batch_id, _ in removed]


def _proof_batch_payload(batch: dict[str, Any]) -> dict[str, Any]:
    return {
        "batch_id": batch["batch_id"],
        "participants": batch["participants"],
        "rounds": batch["rounds"],
        "timeout_seconds": batch["timeout_seconds"],
        "ttl_seconds": batch["ttl_seconds"],
        "created_at_ns": batch["created_at_ns"],
        "expires_at_ns": batch["expires_at_ns"],
        "participants_completed": len(batch["results"]),
        "keys": list(batch["keys"]),
    }


async def _configure_threadpool(tokens: int | None = None) -> int:
    global _thread_tokens

    configured = tokens if tokens is not None else _parse_thread_tokens(os.getenv("THREAD_TOKENS"))
    if configured is None:
        configured = DEFAULT_THREAD_TOKENS

    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = configured
    _thread_tokens = configured
    return configured


def create_app() -> FastAPI:
    global _otel_enabled

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await _configure_threadpool()
        yield

    application = FastAPI(title="FastAPI benchmark application", lifespan=lifespan)
    prometheus_enabled = _prometheus_enabled()

    if os.getenv("OTEL_ENABLED") == "1":
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        except ImportError as exc:
            raise RuntimeError(
                "Install the otel dependency group to enable OTEL_ENABLED=1"
            ) from exc
        FastAPIInstrumentor.instrument_app(application)
        _otel_enabled = True

    if prometheus_enabled and REQUEST_COUNT is not None and REQUEST_LATENCY is not None:

        @application.middleware("http")
        async def metrics_middleware(request, call_next):
            started = time.perf_counter()
            response = await call_next(request)
            elapsed = time.perf_counter() - started
            route = request.scope.get("route")
            path = route.path if route else request.url.path
            REQUEST_COUNT.labels(request.method, path, str(response.status_code)).inc()
            REQUEST_LATENCY.labels(request.method, path).observe(elapsed)
            return response

        @application.get("/metrics", include_in_schema=False)
        def metrics() -> Response:
            return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/runtime")
    def runtime() -> dict[str, Any]:
        gil_check = getattr(sys, "_is_gil_enabled", None)
        gil_enabled = gil_check() if gil_check is not None else None
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return {
            "python": platform.python_version(),
            "python_version_full": sys.version,
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
            "pid": os.getpid(),
            "py_gil_disabled": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
            "gil_enabled": gil_enabled,
            "thread_tokens": _thread_tokens,
            "cpu_rounds": CPU_ROUNDS,
            "active_threads": threading.active_count(),
            "max_rss_kb": rss_kb,
            "dataset_size": len(DATA),
            "large_dataset_size": len(LARGE_DATA),
            "preencoded_dataset_size": len(PREENCODED_DATA),
            "dependencies": _dependency_versions(),
            "prometheus_enabled": prometheus_enabled,
            "otel_enabled": _otel_enabled,
        }

    @application.get("/lookup/{key}")
    def lookup(key: str) -> dict[str, Any]:
        return _record_for(key, DATA)

    @application.get("/lookup-large/{key}")
    def lookup_large(key: str) -> dict[str, Any]:
        return _record_for(key, LARGE_DATA)

    @application.get("/lookup-preencoded/{key}")
    def lookup_preencoded(key: str) -> Response:
        try:
            payload = PREENCODED_DATA[key]
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="key not found") from exc
        return Response(payload, media_type="application/json")

    @application.get("/cpu/{key}")
    def cpu(key: str) -> dict[str, Any]:
        return _cpu_transform(_record_for(key, DATA))

    @application.get("/cow/{key}")
    def cow(key: str) -> dict[str, Any]:
        snapshot = _cow_snapshot
        return _copy_on_write_transform(_record_for(key, snapshot))

    @application.put("/cow/{key}")
    def cow_update(key: str) -> dict[str, Any]:
        global _cow_snapshot

        record = _copy_on_write_transform(_record_for(key, _cow_snapshot))
        with _cow_lock:
            next_snapshot = dict(_cow_snapshot)
            next_snapshot[key] = record
            _cow_snapshot = next_snapshot
        return {"updated": key, "snapshot_size": len(_cow_snapshot)}

    @application.get("/locked-cache/{key}")
    def locked_cache(key: str) -> dict[str, Any]:
        with _cache_lock:
            cached = _locked_cache.get(key)
            if cached is None:
                cached = _cpu_transform(_record_for(key, DATA), rounds=125)
                cached["cached"] = False
                _locked_cache[key] = cached
                return cached
            return {**cached, "cached": True}

    @application.post("/proof/batches")
    def create_proof_batch(payload: dict[str, Any] | None = PROOF_BATCH_BODY) -> dict[str, Any]:
        global _proof_batch_sequence

        _cleanup_stale_proof_batches()
        batch_config = payload or {}
        participants = _positive_int_from_payload(
            batch_config,
            "participants",
            2,
            maximum=PROOF_MAX_PARTICIPANTS,
        )
        rounds = _positive_int_from_payload(batch_config, "rounds", CPU_ROUNDS)
        timeout_seconds = _positive_float_from_payload(
            batch_config,
            "timeout_seconds",
            PROOF_BARRIER_TIMEOUT_SECONDS,
        )
        ttl_seconds = _positive_float_from_payload(
            batch_config,
            "ttl_seconds",
            PROOF_BATCH_TTL_SECONDS,
        )
        created_at_ns = time.perf_counter_ns()
        keys = [f"key-{slot % len(DATA)}" for slot in range(participants)]

        with _proof_lock:
            _proof_batch_sequence += 1
            batch_id = f"batch-{_proof_batch_sequence}"
            batch = {
                "batch_id": batch_id,
                "participants": participants,
                "rounds": rounds,
                "timeout_seconds": timeout_seconds,
                "ttl_seconds": ttl_seconds,
                "created_at_ns": created_at_ns,
                "expires_at_ns": created_at_ns + int(ttl_seconds * 1_000_000_000),
                "barrier": threading.Barrier(participants, timeout=timeout_seconds),
                "keys": keys,
                "results": {},
            }
            _proof_batches[batch_id] = batch

        return _proof_batch_payload(batch)

    @application.get("/proof/batches/{batch_id}/work/{slot}")
    def proof_batch_work(batch_id: str, slot: int) -> dict[str, Any]:
        _cleanup_stale_proof_batches()
        with _proof_lock:
            batch = _proof_batches.get(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if slot < 0 or slot >= int(batch["participants"]):
            raise HTTPException(status_code=400, detail="slot out of range")

        key = batch["keys"][slot]
        thread_id = threading.get_ident()
        result: dict[str, Any] = {
            "batch_id": batch_id,
            "slot": slot,
            "key": key,
            "thread_id": thread_id,
            "perf_counter_start_ns": None,
            "perf_counter_end_ns": None,
            "thread_time_start_ns": None,
            "thread_time_end_ns": None,
            "checksum": None,
            "digest": None,
            "error": None,
        }

        try:
            batch["barrier"].wait(timeout=float(batch["timeout_seconds"]))
            perf_counter_start_ns = time.perf_counter_ns()
            thread_time_start_ns = time.thread_time_ns()
            transformed = _cpu_transform(_record_for(key, DATA), rounds=int(batch["rounds"]))
            thread_time_end_ns = time.thread_time_ns()
            perf_counter_end_ns = time.perf_counter_ns()
            result.update(
                {
                    "perf_counter_start_ns": perf_counter_start_ns,
                    "perf_counter_end_ns": perf_counter_end_ns,
                    "thread_time_start_ns": thread_time_start_ns,
                    "thread_time_end_ns": thread_time_end_ns,
                    "checksum": transformed["checksum"],
                    "digest": transformed["digest"],
                }
            )
        except threading.BrokenBarrierError:
            captured_ns = time.perf_counter_ns()
            captured_thread_ns = time.thread_time_ns()
            result.update(
                {
                    "perf_counter_start_ns": captured_ns,
                    "perf_counter_end_ns": captured_ns,
                    "thread_time_start_ns": captured_thread_ns,
                    "thread_time_end_ns": captured_thread_ns,
                    "error": "barrier timeout",
                }
            )
        except Exception as exc:  # pragma: no cover - defensive recordkeeping for proof runs.
            captured_ns = time.perf_counter_ns()
            captured_thread_ns = time.thread_time_ns()
            result.update(
                {
                    "perf_counter_start_ns": captured_ns,
                    "perf_counter_end_ns": captured_ns,
                    "thread_time_start_ns": captured_thread_ns,
                    "thread_time_end_ns": captured_thread_ns,
                    "error": type(exc).__name__,
                }
            )

        with _proof_lock:
            current_batch = _proof_batches.get(batch_id)
            if current_batch is not None:
                current_batch["results"][slot] = result
        return result

    @application.get("/proof/batches/{batch_id}")
    def get_proof_batch(batch_id: str) -> dict[str, Any]:
        _cleanup_stale_proof_batches()
        with _proof_lock:
            batch = _proof_batches.get(batch_id)
            if batch is None:
                raise HTTPException(status_code=404, detail="batch not found")
            spans = list(batch["results"].values())
            payload = _proof_batch_payload(batch)

        return {**payload, **_aggregate_proof_spans(spans)}

    @application.delete("/proof/batches/{batch_id}")
    def delete_proof_batch(batch_id: str) -> dict[str, Any]:
        with _proof_lock:
            batch = _proof_batches.pop(batch_id, None)
        if batch is None:
            raise HTTPException(status_code=404, detail="batch not found")

        batch["barrier"].abort()
        return {"batch_id": batch_id, "deleted": True}

    @application.get("/control/thread-tokens")
    async def get_thread_tokens() -> dict[str, int | None]:
        return {"thread_tokens": _thread_tokens}

    @application.put("/control/thread-tokens/{tokens}")
    async def set_thread_tokens(tokens: int) -> dict[str, int]:
        if tokens < 1:
            raise HTTPException(status_code=400, detail="tokens must be greater than zero")
        configured = await _configure_threadpool(tokens)
        return {"thread_tokens": configured}

    return application


app = create_app()
