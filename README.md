# FastAPI Free-Threading Benchmark

This repository benchmarks FastAPI `0.136.0` on sync `def` endpoints under regular
CPython and Python `3.14t` free-threaded mode. The benchmark is intentionally not
an async/I/O test. It is designed to answer whether one FastAPI process can use
multiple Python threads better when the GIL is disabled.

## Why sync endpoints

FastAPI runs regular `def` endpoints in Starlette/AnyIO's worker thread pool.
That is the path where Python `3.14t` can expose parallel execution of Python
bytecode. The `/cpu/{key}` route uses a pure-Python integer transform so native
extensions do not hide the signal.

## Runtime matrix

Use at least these conditions:

| Label | Runtime | Purpose |
|---|---|---|
| `py312-gil` | Python 3.12 or 3.13 | Older baseline |
| `py314-gil` | Python 3.14 regular | Python-version control |
| `py314t-gil-on` | Python 3.14t with `PYTHON_GIL=1` | Free-threaded build overhead |
| `py314t-gil-off` | Python 3.14t with `PYTHON_GIL=0` | Actual free-threaded run |

The `/runtime` endpoint records `python -VV`-equivalent details,
`Py_GIL_DISABLED`, `sys._is_gil_enabled()`, worker count, and thread settings.

## Install

With `uv`:

```bash
uv sync --group dev --group analysis
```

Add `--group otel` when you want OpenTelemetry tracing:

```bash
uv sync --group dev --group analysis --group otel
```

For free-threaded Python:

```bash
uv python install 3.14t
uv venv --python 3.14t .venv-314t
```

## Run the app manually

```bash
THREAD_TOKENS=16 PYTHON_GIL=0 python3.14t -m uvicorn app.main:app \
  --host 127.0.0.1 \
  --port 8000 \
  --workers 1 \
  --loop asyncio \
  --http h11 \
  --no-access-log
```

Then check:

```bash
curl http://127.0.0.1:8000/runtime
```

## Run k6 matrix

Install k6 first, then:

```bash
uv run python scripts/run_matrix.py \
  --condition py314=python3.14 \
  --condition py314t-off=python3.14t,gil=0,require_gil_disabled=true \
  --route-profile sync-cpu \
  --thread-tokens 1 2 4 8 16 32 \
  --target-rps 100 250 500 1000 \
  --repeat 5 \
  --duration 3m
```

Results are written under `bench/results/<timestamp>/`.

For a heavier run that makes each `/cpu/{key}` request about 100x more
expensive than the default pilot workload, add:

```bash
--cpu-rounds 800000
```

For a literal 100x longer version of the 20 second pilot cells, use:

```bash
--duration 2000s
```

## Prove thread freedom without containers

The primary evidence should be host-native. Do not use Rancher Desktop, Docker
k6, `host.docker.internal`, or compose networking for the proof run.

First run the mechanism proof. This sends simultaneous requests to sync proof
endpoints and reports:

```text
parallelism_ratio = sum(worker thread CPU time) / batch wall time
```

GIL-enabled runtimes should stay near `1.0`. Python `3.14t` with
`PYTHON_GIL=0` should rise above `1.0` when multiple proof participants run.

```bash
uv run python scripts/run_parallel_proof.py \
  --condition py314t-gil-on=.venv-314t/bin/python,gil=1 \
  --condition py314t-gil-off=.venv-314t/bin/python,gil=0,require_gil_disabled=true \
  --thread-tokens 16 \
  --participants 1 2 4 8 12 16 \
  --repeat 30 \
  --rounds 800000
```

Then run the user-facing saturation frontier with host-native k6:

```bash
uv run python scripts/run_frontier.py \
  --condition py314t-gil-on=.venv-314t/bin/python,gil=1 \
  --condition py314t-gil-off=.venv-314t/bin/python,gil=0,require_gil_disabled=true \
  --thread-tokens 1 2 4 6 8 12 16 \
  --target-rps 1 2 3 4 6 8 12 16 \
  --duration 30s \
  --request-timeout 5s \
  --max-vus 64 \
  --cpu-rounds 800000 \
  --k6 /opt/homebrew/bin/k6
```

The frontier runner rejects Docker-backed k6 launchers and non-loopback URLs.
It records k6 summaries, `/runtime` before and after each cell, and per-second
host process telemetry for the app and k6.

Useful k6 route profiles:

| Profile | Routes |
|---|---|
| `sync-cpu` | `/cpu/{key}` only |
| `sync-read` | lookup, large lookup, pre-encoded JSON, copy-on-write read |
| `sync-cache` | lock-contended cache route |
| `sync-mixed` | mixed read, CPU, copy-on-write, and cache routes |

## Telemetry

Prometheus and Grafana are optional and are not primary evidence for the
free-threading proof. The benchmark app disables Prometheus middleware by
default to avoid lock/label overhead. Enable it explicitly only for telemetry
overhead experiments:

```bash
BENCH_PROMETHEUS_ENABLED=1 THREAD_TOKENS=16 python -m uvicorn app.main:app
```

Start Prometheus and Grafana only for exploratory dashboards:

```bash
cd telemetry
docker compose up -d prometheus grafana
```

Grafana is served at `http://localhost:3000`; Prometheus is served at
`http://localhost:9090`. The FastAPI app exposes Prometheus metrics at
`/metrics`.

Set `OTEL_ENABLED=1` when starting the app to enable OpenTelemetry FastAPI
instrumentation. Keep it disabled for the primary benchmark unless tracing
overhead is part of the experiment.

To send k6 metrics to Prometheus:

```bash
cd telemetry
docker compose --profile bench run --rm k6
```

## Generate report

```bash
uv run python analysis/generate_k6_report.py \
  --results-dir bench/results \
  --output-dir analysis/report
```

For the host-native proof campaign:

```bash
uv run python analysis/generate_proof_report.py \
  --results-dir bench/results \
  --output-dir analysis/proof-report
```

## Methodology guardrails

- Do not claim "FastAPI is faster on 3.14t" from async-only routes.
- Do not compare only Python `3.14t` against Python `3.12`; include Python
  `3.14` regular and `3.14t` with the GIL forced on.
- Do not use Uvicorn multi-process workers for the primary free-threading test.
  Multi-process workers are a useful production baseline, but they hide the
  single-interpreter threading effect.
- Treat any run where `/runtime` shows the GIL enabled as invalid for the
  `py314t-gil-off` condition.
- Keep optional native extensions out of the primary matrix unless their
  free-threading support is verified.
- Primary evidence must use host-native k6 and `127.0.0.1`; Docker/Rancher
  runs are exploratory only because container CPU limits and networking can
  distort the result.
