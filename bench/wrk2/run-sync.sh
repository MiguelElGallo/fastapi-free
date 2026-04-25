#!/usr/bin/env sh
set -eu

URL="${URL:-http://localhost:8000/cpu/key-1}"
WRK_BIN="${WRK_BIN:-wrk}"
THREADS="${THREADS:-4}"
CONNECTIONS="${CONNECTIONS:-64}"
DURATION="${DURATION:-2m}"
RATE="${RATE:-100}"

"$WRK_BIN" \
  -t"$THREADS" \
  -c"$CONNECTIONS" \
  -d"$DURATION" \
  -R"$RATE" \
  --latency \
  "$URL"
