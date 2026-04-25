# Benchmark Methodology

## Question

Does Python `3.14t` with the GIL disabled improve FastAPI `0.136.0`
performance for sync `def` endpoints serving in-memory data?

This benchmark deliberately does not test async I/O scalability. Async routes
can be added as controls, but the primary test surface is Starlette's sync
endpoint thread pool.

## Primary Hypothesis

For pure-Python CPU-visible work in one Uvicorn process, Python `3.14t` with
`PYTHON_GIL=0` should allow more request handlers to execute Python bytecode in
parallel than regular GIL builds.

## Conditions

Run all four conditions whenever possible:

| Label | Interpreter | GIL state | Purpose |
|---|---|---|---|
| `py312-gil` | `python3.12` or `python3.13` | enabled | Older baseline |
| `py314-gil` | `python3.14` | enabled | Python-version control |
| `py314t-gil-on` | `python3.14t` | forced enabled | Free-threaded build overhead |
| `py314t-gil-off` | `python3.14t` | forced disabled | Free-threaded result |

The runner rejects `py314t-gil-off` runs unless `/runtime` reports
`"gil_enabled": false`.

For the primary claim, prefer `py314t-gil-on` versus `py314t-gil-off` when the
regular `python3.14` patch level does not match the `python3.14t` patch level.
That isolates runtime GIL state inside the same free-threaded build.

## Workloads

| Route | Type | Purpose |
|---|---|---|
| `/lookup/{key}` | sync | Small immutable in-memory dictionary lookup |
| `/lookup-large/{key}` | sync | Larger nested payload and JSON serialization |
| `/lookup-preencoded/{key}` | sync | Pre-encoded JSON bytes to isolate lookup/server overhead |
| `/cpu/{key}` | sync | Pure-Python CPU transform; main free-threading signal |
| `/cow/{key}` | sync | Copy-on-write snapshot read |
| `/locked-cache/{key}` | sync | Lock-contended cache control |
| `/proof/batches/*` | sync | Internal simultaneous batch proof for thread CPU/wall ratio |

The `sync-cpu` k6 profile targets only `/cpu/{key}`. Use that first when
looking for a free-threading effect.

The proof endpoints are the mechanism evidence. A proof batch reports
`parallelism_ratio = sum(thread_time_ns deltas) / wall_ns`. GIL-enabled runs
should stay close to `1`; a GIL-disabled run should rise above `1` as
participants increase.

## Controls

- Uvicorn uses `--workers 1` in the primary matrix.
- Event loop is stdlib `asyncio`.
- HTTP parser is `h11`.
- Access logs are disabled.
- AnyIO thread tokens are explicit and swept.
- Native optional serializers are not used in the primary route code.
- Dataset is built at process startup and served from memory.
- Primary proof and frontier runs use host-native tools only: `127.0.0.1`,
  host k6, host Uvicorn, and host process telemetry. Docker/Rancher runs are
  exploratory only.
- Prometheus middleware is disabled by default. Enable it only when measuring
  telemetry overhead separately.

## Run Order

Use randomized or blocked run ordering when collecting real numbers. At minimum:

1. Run the mechanism proof with `scripts/run_parallel_proof.py`.
2. Confirm `py314t-gil-off` reports `parallelism_ratio` well above `1`.
3. Run the host-native k6 frontier with `scripts/run_frontier.py`.
4. Use `sync-read` and `locked-cache` as negative/control workloads if needed.
5. Repeat final frontier cells at least five times near the saturation boundary.
6. Generate the report from proof JSONL and frontier cell summaries.

## Saturation Rule

Label a point as saturated when one of these is true:

- achieved RPS is below 95% of offered RPS,
- p99 latency exceeds the chosen service objective,
- HTTP error rate exceeds 0.1%,
- k6 dropped iterations are nonzero,
- CPU is saturated and throughput stops increasing.

Do not summarize the whole experiment with one max-RPS number. Use
latency-throughput curves and thread-token scaling graphs.

## Valid Claim Shape

Use this form:

> Under the `sync-cpu` workload, with these dependency versions and runtime
> checks confirming the GIL stayed disabled, Python `3.14t` with `PYTHON_GIL=0`
> changed throughput by X% and p99 latency by Y% relative to Python `3.14`
> regular.

Avoid saying "`3.14t` makes FastAPI faster" without the workload and runtime
constraints.
