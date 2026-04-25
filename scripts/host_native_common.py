#!/usr/bin/env python3
"""Shared helpers for host-native benchmark runners."""

from __future__ import annotations

import csv
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import time
import urllib.parse
from pathlib import Path
from typing import Any


class HostNativeValidationError(ValueError):
    """Raised when a runner argument would leave the host-native boundary."""


_DURATION_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s|m|h)")
_DURATION_MULTIPLIERS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}


def parse_k6_duration_seconds(value: str) -> float:
    """Parse a k6-style duration such as ``500ms``, ``30s``, or ``1m30s``."""
    text = value.strip()
    if not text:
        raise ValueError("duration must not be empty")

    total = 0.0
    position = 0
    for match in _DURATION_RE.finditer(text):
        if match.start() != position:
            raise ValueError(f"invalid duration: {value!r}")
        total += float(match.group("value")) * _DURATION_MULTIPLIERS[match.group("unit")]
        position = match.end()

    if position != len(text) or total <= 0:
        raise ValueError(f"invalid duration: {value!r}")
    return total


def wall_timeout_seconds(duration: str, request_timeout: str, grace_seconds: float = 15.0) -> float:
    """Return the hard wall timeout for a k6 cell."""
    return (
        parse_k6_duration_seconds(duration)
        + parse_k6_duration_seconds(request_timeout)
        + grace_seconds
    )


def validate_loopback_base_url(base_url: str) -> str:
    """Require a URL that targets the host-local FastAPI server via 127.0.0.1."""
    parsed = urllib.parse.urlparse(base_url)
    hostname = (parsed.hostname or "").lower()
    if hostname == "host.docker.internal":
        raise HostNativeValidationError("host.docker.internal is not allowed for host-native runs")
    if hostname != "127.0.0.1":
        raise HostNativeValidationError("host-native runs must target 127.0.0.1")
    if parsed.scheme not in {"http", "https"}:
        raise HostNativeValidationError("base URL must use http or https")
    if not parsed.port:
        raise HostNativeValidationError("base URL must include an explicit port")
    return base_url.rstrip("/")


def validate_host_native_k6(k6_bin: str) -> str:
    """Reject docker-backed k6 launchers and return the original executable string."""
    tokens = shlex.split(k6_bin)
    if len(tokens) != 1:
        raise HostNativeValidationError("--k6 must be a single host-native executable path")

    candidate = tokens[0]
    names_to_check = [Path(candidate).name.lower(), candidate.lower()]
    resolved = shutil.which(candidate)
    if resolved:
        names_to_check.extend([Path(resolved).name.lower(), resolved.lower()])

    docker_backed = any(
        name == "docker" or "k6-docker" in name or "docker" in Path(name).name
        for name in names_to_check
    )
    if docker_backed:
        raise HostNativeValidationError("docker and k6-docker launchers are not allowed")

    return candidate


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def append_jsonl(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(data, sort_keys=True))
        file.write("\n")


def append_csv_row(path: Path, row: dict[str, Any], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    should_write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        if should_write_header:
            writer.writeheader()
        writer.writerow(row)


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile_value
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def stop_process_tree(proc: subprocess.Popen[Any], timeout: float = 10.0) -> None:
    """Terminate a subprocess, preferring its process group when available."""
    if proc.poll() is not None:
        return

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError:
        proc.terminate()

    try:
        proc.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except OSError:
        proc.kill()
    proc.wait(timeout=timeout)


def run_process_with_timeout(
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    timeout_s: float,
) -> dict[str, Any]:
    """Run a command with a hard timeout and return execution metadata."""
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.time()
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr:
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=stdout,
            stderr=stderr,
            text=True,
            start_new_session=True,
        )
        timed_out = False
        try:
            returncode = proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            stop_process_tree(proc)
            returncode = proc.returncode

    finished = time.time()
    return {
        "cmd": cmd,
        "returncode": returncode,
        "timed_out": timed_out,
        "timeout_s": timeout_s,
        "started_at": started,
        "finished_at": finished,
        "elapsed_s": finished - started,
        "pid": proc.pid,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
    }


def _sample_process_with_ps(pid: int) -> dict[str, Any]:
    cmd = ["ps", "-o", "pid=,ppid=,pcpu=,pmem=,rss=,vsz=,etime=,command=", "-p", str(pid)]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if completed.returncode != 0 or not completed.stdout.strip():
        return {"pid": pid, "alive": False}

    line = completed.stdout.strip().splitlines()[0]
    parts = line.split(None, 7)
    if len(parts) < 8:
        return {"pid": pid, "alive": True, "raw": line}

    return {
        "pid": int(parts[0]),
        "ppid": int(parts[1]),
        "cpu_percent": float(parts[2]),
        "mem_percent": float(parts[3]),
        "rss_kb": int(parts[4]),
        "vsz_kb": int(parts[5]),
        "etime": parts[6],
        "command": parts[7],
        "alive": True,
        "source": "ps",
    }


def sample_process(pid: int) -> dict[str, Any]:
    """Sample basic process telemetry using psutil when present, else ``ps``."""
    try:
        import psutil  # type: ignore[import-not-found]
    except ImportError:
        return _sample_process_with_ps(pid)

    try:
        process = psutil.Process(pid)
        memory = process.memory_info()
        return {
            "pid": process.pid,
            "ppid": process.ppid(),
            "cpu_percent": process.cpu_percent(interval=None),
            "mem_percent": process.memory_percent(),
            "rss_kb": memory.rss // 1024,
            "vsz_kb": memory.vms // 1024,
            "status": process.status(),
            "command": " ".join(process.cmdline()),
            "alive": process.is_running(),
            "source": "psutil",
        }
    except Exception as exc:  # pragma: no cover - depends on process timing.
        return {"pid": pid, "alive": False, "error": repr(exc), "source": "psutil"}
