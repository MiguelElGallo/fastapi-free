# FastAPI Free-Threading Benchmark

Small benchmark harness for FastAPI `0.136.0` on sync `def` endpoints with
Python `3.14t` free-threading. The goal is to show whether one FastAPI process
can run CPU-bound request work in parallel when the GIL is disabled.

## 1. Install

```bash
uv sync --group dev --group analysis
uv python install 3.14t
uv venv --python 3.14t .venv-314t
```

Install host-native k6 if you want the frontier test:

```bash
brew install k6
```

## 2. Run The Mechanism Proof

```bash
uv run python scripts/run_parallel_proof.py \
  --condition py314t-gil-on=.venv-314t/bin/python,gil=1 \
  --condition py314t-gil-off=.venv-314t/bin/python,gil=0,require_gil_disabled=true \
  --thread-tokens 16 \
  --participants 1 2 4 8 \
  --repeat 5 \
  --rounds 800000 \
  --out bench/results/proof-run
```

## 3. Run The k6 Frontier

```bash
uv run python scripts/run_frontier.py \
  --condition py314t-gil-on=.venv-314t/bin/python,gil=1 \
  --condition py314t-gil-off=.venv-314t/bin/python,gil=0,require_gil_disabled=true \
  --thread-tokens 8 \
  --target-rps 1 2 3 4 6 \
  --repeat 1 \
  --duration 10s \
  --request-timeout 5s \
  --max-vus 64 \
  --preallocated-vus 16 \
  --cpu-rounds 800000 \
  --k6 /opt/homebrew/bin/k6 \
  --base-url http://127.0.0.1:8121 \
  --port 8121 \
  --out bench/results/proof-run/frontier
```

## 4. Generate The Report

```bash
uv run python analysis/generate_proof_report.py \
  --results-dir bench/results/proof-run \
  --output-dir analysis/proof-report
```

Open:

```bash
open analysis/proof-report/report.md
```
