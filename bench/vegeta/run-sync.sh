#!/usr/bin/env sh
set -eu

TARGETS_FILE="${TARGETS_FILE:-bench/vegeta/sync-targets.txt}"
RATE="${RATE:-50}"
DURATION="${DURATION:-2m}"
OUTPUT="${OUTPUT:-bench/vegeta/results-sync.bin}"

vegeta attack \
  -targets="$TARGETS_FILE" \
  -rate="$RATE" \
  -duration="$DURATION" \
  -name="sync-endpoints" \
  | tee "$OUTPUT" \
  | vegeta report

vegeta plot "$OUTPUT" > "${OUTPUT%.bin}.html"
