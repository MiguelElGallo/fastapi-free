#!/usr/bin/env python3
"""Generate aggregate benchmark reports from k6 JSON summary files.

The script expects k6 summaries under bench/results by default. It also looks
for JSON metadata files near each summary and folds any simple scalar metadata
into grouping labels and the report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUMMARY_NAME_HINTS = ("summary", "k6")
METADATA_NAME_HINTS = ("meta", "metadata", "run")
GROUP_CANDIDATES = (
    "variant",
    "condition.label",
    "route_profile",
    "thread_tokens",
    "target_rps",
    "workers",
    "suite",
    "benchmark",
    "test",
    "scenario",
    "endpoint",
    "route",
    "profile",
    "implementation",
)

pd: Any = None
plt: Any = None


@dataclass(frozen=True)
class RunRecord:
    path: Path
    run_id: str
    label: str
    metadata: dict[str, Any]
    metrics: dict[str, float]


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path} did not contain a JSON object")
    return data


def flatten_scalars(value: Any, prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    if isinstance(value, dict):
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            flat.update(flatten_scalars(child, child_prefix))
    elif isinstance(value, (str, int, float, bool)) or value is None:
        flat[prefix] = value
    return flat


def is_probable_summary(path: Path, data: dict[str, Any] | None = None) -> bool:
    name = path.name.lower()
    if any(hint in name for hint in SUMMARY_NAME_HINTS):
        return True
    if data is None:
        try:
            data = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            return False
    metrics = data.get("metrics")
    return isinstance(metrics, dict) and any(key.startswith("http_req") for key in metrics)


def metadata_search_keys(summary_path: Path) -> set[str]:
    stem = summary_path.stem.lower()
    keys = {stem}
    for token in ("summary", "k6", "results", "result"):
        keys.add(stem.replace(token, "").strip("-_."))
    keys.update(part for part in stem.replace("-", "_").split("_") if part)
    return {key for key in keys if key}


def is_metadata_path(path: Path) -> bool:
    name = path.name.lower()
    return path.suffix.lower() == ".json" and any(hint in name for hint in METADATA_NAME_HINTS)


def find_metadata(summary_path: Path, all_json_paths: list[Path]) -> dict[str, Any]:
    embedded: dict[str, Any] = {}
    try:
        summary = load_json(summary_path)
    except (OSError, ValueError, json.JSONDecodeError):
        summary = {}
    for key in ("metadata", "meta", "run_metadata", "run"):
        if isinstance(summary.get(key), dict):
            embedded.update(flatten_scalars(summary[key], key))

    keys = metadata_search_keys(summary_path)
    candidates: list[Path] = []
    for path in all_json_paths:
        if path == summary_path or not is_metadata_path(path):
            continue
        if path.parent == summary_path.parent:
            candidates.append(path)
            continue
        lowered = path.stem.lower()
        if any(key in lowered for key in keys):
            candidates.append(path)

    metadata = dict(embedded)
    for path in sorted(set(candidates)):
        try:
            raw = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        for key, value in flatten_scalars(raw).items():
            metadata.setdefault(key, value)
    return metadata


def metric_value(metrics: dict[str, Any], metric: str, field: str) -> float | None:
    payload = metrics.get(metric)
    if not isinstance(payload, dict):
        return None
    values = payload.get("values")
    if isinstance(values, dict):
        value = values.get(field)
    else:
        value = payload.get(field)
    if value is None and field == "rate":
        value = payload.get("value")
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    return None


def extract_metrics(summary: dict[str, Any]) -> dict[str, float]:
    raw_metrics = summary.get("metrics")
    if not isinstance(raw_metrics, dict):
        return {}

    fields = {
        "duration_avg_ms": ("http_req_duration", "avg"),
        "duration_med_ms": ("http_req_duration", "med"),
        "duration_p90_ms": ("http_req_duration", "p(90)"),
        "duration_p95_ms": ("http_req_duration", "p(95)"),
        "duration_max_ms": ("http_req_duration", "max"),
        "waiting_p95_ms": ("http_req_waiting", "p(95)"),
        "blocked_p95_ms": ("http_req_blocked", "p(95)"),
        "requests_per_sec": ("http_reqs", "rate"),
        "request_count": ("http_reqs", "count"),
        "iterations_per_sec": ("iterations", "rate"),
        "iteration_count": ("iterations", "count"),
        "checks_rate": ("checks", "rate"),
        "data_received_bytes": ("data_received", "count"),
        "data_sent_bytes": ("data_sent", "count"),
        "failed_requests_rate": ("http_req_failed", "rate"),
    }

    extracted: dict[str, float] = {}
    for output_name, (metric, field) in fields.items():
        value = metric_value(raw_metrics, metric, field)
        if value is not None:
            extracted[output_name] = value
    return extracted


def value_from_metadata(metadata: dict[str, Any], key: str) -> Any:
    if key in metadata:
        return metadata[key]
    suffix = f".{key}"
    for meta_key, value in metadata.items():
        if meta_key.endswith(suffix):
            return value
    return None


def infer_label(path: Path, metadata: dict[str, Any]) -> str:
    variant = value_from_metadata(metadata, "variant")
    thread_tokens = value_from_metadata(metadata, "thread_tokens")
    target_rps = value_from_metadata(metadata, "target_rps")
    has_matrix_metadata = (
        variant not in (None, "")
        and thread_tokens not in (None, "")
        and target_rps not in (None, "")
    )
    if has_matrix_metadata:
        parts = [str(variant), f"threads={thread_tokens}", f"rps={target_rps}"]
        workers = value_from_metadata(metadata, "workers")
        route_profile = value_from_metadata(metadata, "route_profile")
        if workers not in (None, "", 1, "1"):
            parts.append(f"workers={workers}")
        if route_profile not in (None, "", "sync-cpu"):
            parts.append(str(route_profile))
        return " ".join(parts)

    parts: list[str] = []
    for key in GROUP_CANDIDATES:
        value = value_from_metadata(metadata, key)
        if value not in (None, ""):
            parts.append(f"{key}={value}")
    if parts:
        return ", ".join(parts)
    if path.parent.name.lower() not in ("results", "bench"):
        return path.parent.name
    stem = path.stem
    for token in ("summary", "k6", "result", "results"):
        stem = stem.replace(token, "")
    return stem.strip("-_.") or path.stem


def stable_run_id(path: Path) -> str:
    digest = hashlib.sha1(str(path).encode("utf-8")).hexdigest()[:10]
    return f"{path.stem}-{digest}"


def read_runs(results_dir: Path) -> list[RunRecord]:
    all_json_paths = sorted(path for path in results_dir.rglob("*.json") if path.is_file())
    runs: list[RunRecord] = []
    for path in all_json_paths:
        try:
            data = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            if "did not contain a JSON object" not in str(exc):
                print(f"Skipping unreadable JSON {path}: {exc}")
            continue
        if not is_probable_summary(path, data):
            continue
        metrics = extract_metrics(data)
        if not metrics:
            print(f"Skipping {path}: no supported k6 metrics found")
            continue
        metadata = find_metadata(path, all_json_paths)
        runs.append(
            RunRecord(
                path=path,
                run_id=str(value_from_metadata(metadata, "run_id") or stable_run_id(path)),
                label=infer_label(path, metadata),
                metadata=metadata,
                metrics=metrics,
            )
        )
    return runs


def import_dependencies() -> None:
    global pd, plt
    try:
        import matplotlib.pyplot as imported_plt
        import pandas as imported_pd
    except ModuleNotFoundError as exc:
        missing = exc.name or "required package"
        raise SystemExit(
            f"Missing Python dependency: {missing}. "
            "Install pandas and matplotlib to generate reports."
        ) from exc
    pd = imported_pd
    plt = imported_plt


def bootstrap_ci(
    values: list[float],
    iterations: int,
    seed: int,
) -> tuple[float | None, float | None]:
    clean = [float(value) for value in values if math.isfinite(value)]
    if len(clean) < 3:
        return None, None
    rng = random.Random(seed)
    medians: list[float] = []
    for _ in range(iterations):
        sample = [rng.choice(clean) for _ in clean]
        medians.append(float(pd.Series(sample).median()))
    medians.sort()
    lower_index = max(0, int(0.025 * len(medians)) - 1)
    upper_index = min(len(medians) - 1, int(0.975 * len(medians)))
    return medians[lower_index], medians[upper_index]


def build_frames(
    runs: list[RunRecord],
    bootstrap_iterations: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for run in runs:
        row: dict[str, Any] = {
            "run_id": run.run_id,
            "label": run.label,
            "summary_path": str(run.path),
        }
        row.update(run.metrics)
        for key, value in run.metadata.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                row[f"meta.{key}"] = value
        rows.append(row)

    run_df = pd.DataFrame(rows)
    if run_df.empty:
        return run_df, pd.DataFrame()

    metric_columns = [
        column
        for column in run_df.columns
        if column not in ("run_id", "label", "summary_path")
        and not column.startswith("meta.")
        and pd.api.types.is_numeric_dtype(run_df[column])
    ]
    summary_rows: list[dict[str, Any]] = []
    for label, group in run_df.groupby("label", dropna=False):
        base: dict[str, Any] = {"label": label, "runs": int(len(group))}
        for metric in metric_columns:
            values = pd.to_numeric(group[metric], errors="coerce").dropna()
            if values.empty:
                continue
            ci_low, ci_high = bootstrap_ci(
                values.tolist(),
                iterations=bootstrap_iterations,
                seed=int(hashlib.sha1(f"{label}:{metric}".encode()).hexdigest()[:8], 16),
            )
            base[f"{metric}.median"] = float(values.median())
            base[f"{metric}.mean"] = float(values.mean())
            base[f"{metric}.min"] = float(values.min())
            base[f"{metric}.max"] = float(values.max())
            base[f"{metric}.ci95_low"] = ci_low
            base[f"{metric}.ci95_high"] = ci_high
        summary_rows.append(base)

    return run_df, pd.DataFrame(summary_rows).sort_values(["label"]).reset_index(drop=True)


def format_number(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if isinstance(value, (int, float)):
        if abs(value) >= 100:
            return f"{value:,.1f}"
        if abs(value) >= 10:
            return f"{value:,.2f}"
        return f"{value:,.3f}"
    return str(value)


def metric_title(metric: str) -> str:
    return metric.replace("_", " ").replace(" per sec", "/sec").title()


def plot_metric(summary_df: pd.DataFrame, metric: str, output_dir: Path) -> Path | None:
    median_col = f"{metric}.median"
    if median_col not in summary_df.columns:
        return None
    plot_df = summary_df[["label", median_col, f"{metric}.ci95_low", f"{metric}.ci95_high"]].dropna(
        subset=[median_col]
    )
    if plot_df.empty:
        return None
    plot_df = plot_df.sort_values(median_col)
    yerr = None
    low_col = f"{metric}.ci95_low"
    high_col = f"{metric}.ci95_high"
    has_error_bars = (
        low_col in plot_df.columns
        and high_col in plot_df.columns
        and plot_df[low_col].notna().any()
    )
    if has_error_bars:
        lows = (plot_df[median_col] - plot_df[low_col]).clip(lower=0)
        highs = (plot_df[high_col] - plot_df[median_col]).clip(lower=0)
        yerr = [lows.to_numpy(), highs.to_numpy()]

    height = max(3.5, min(10.0, 0.45 * len(plot_df) + 1.8))
    _, axis = plt.subplots(figsize=(10, height))
    axis.barh(plot_df["label"], plot_df[median_col], xerr=yerr, color="#4477aa", alpha=0.9)
    axis.set_xlabel(metric_title(metric))
    axis.set_ylabel("")
    axis.set_title(f"Median {metric_title(metric)}")
    axis.grid(axis="x", alpha=0.25)
    plt.tight_layout()
    output_path = output_dir / f"{metric}.png"
    plt.savefig(output_path, dpi=160)
    plt.close()
    return output_path


def emit_charts(summary_df: pd.DataFrame, output_dir: Path) -> list[Path]:
    chart_dir = output_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)
    metrics = [
        "requests_per_sec",
        "duration_med_ms",
        "duration_p95_ms",
        "failed_requests_rate",
        "checks_rate",
    ]
    paths: list[Path] = []
    for metric in metrics:
        path = plot_metric(summary_df, metric, chart_dir)
        if path is not None:
            paths.append(path)
    return paths


def markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "_No data._"
    available = [column for column in columns if column in df.columns]
    if not available:
        return "_No matching columns._"
    table = df[available].copy()
    for column in table.columns:
        if pd.api.types.is_numeric_dtype(table[column]):
            table[column] = table[column].map(format_number)
    table = table.fillna("").astype(str)
    header = "| " + " | ".join(table.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(table.columns)) + " |"
    body = ["| " + " | ".join(row) + " |" for row in table.itertuples(index=False, name=None)]
    return "\n".join([header, separator, *body])


def emit_report(
    run_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    chart_paths: list[Path],
    output_dir: Path,
    results_dir: Path,
) -> Path:
    report_path = output_dir / "report.md"
    lines = [
        "# k6 Benchmark Report",
        "",
        f"Source results: `{results_dir}`",
        f"Runs parsed: `{len(run_df)}`",
        f"Groups: `{summary_df['label'].nunique() if not summary_df.empty else 0}`",
        "",
        "Bootstrap confidence intervals are included when a group has at least three runs.",
        "",
    ]
    if chart_paths:
        lines.extend(["## Charts", ""])
        for path in chart_paths:
            rel = path.relative_to(output_dir)
            lines.append(f"![{path.stem}]({rel.as_posix()})")
            lines.append("")

    lines.extend(
        [
            "## Aggregate Metrics",
            "",
            markdown_table(
                summary_df,
                [
                    "label",
                    "runs",
                    "requests_per_sec.median",
                    "requests_per_sec.ci95_low",
                    "requests_per_sec.ci95_high",
                    "duration_med_ms.median",
                    "duration_med_ms.ci95_low",
                    "duration_med_ms.ci95_high",
                    "duration_p95_ms.median",
                    "duration_p95_ms.ci95_low",
                    "duration_p95_ms.ci95_high",
                    "failed_requests_rate.median",
                    "checks_rate.median",
                ],
            ),
            "",
            "## Parsed Runs",
            "",
            markdown_table(
                run_df,
                [
                    "label",
                    "run_id",
                    "requests_per_sec",
                    "duration_med_ms",
                    "duration_p95_ms",
                    "failed_requests_rate",
                    "checks_rate",
                    "summary_path",
                ],
            ),
            "",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def write_outputs(
    run_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    output_dir: Path,
    results_dir: Path,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    runs_csv = output_dir / "runs.csv"
    summary_csv = output_dir / "summary.csv"
    run_df.to_csv(runs_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)
    chart_paths = emit_charts(summary_df, output_dir)
    report_path = emit_report(run_df, summary_df, chart_paths, output_dir, results_dir)
    return [runs_csv, summary_csv, *chart_paths, report_path]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("bench/results"),
        help="Directory containing k6 JSON summaries and run metadata.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("analysis/report"),
        help="Directory for generated CSVs, charts, and Markdown report.",
    )
    parser.add_argument(
        "--bootstrap-iterations",
        type=int,
        default=2000,
        help="Bootstrap resamples per metric/group when at least three runs exist.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.bootstrap_iterations < 100:
        raise SystemExit("--bootstrap-iterations must be at least 100")
    if not args.results_dir.exists():
        raise SystemExit(f"Results directory does not exist: {args.results_dir}")

    import_dependencies()
    runs = read_runs(args.results_dir)
    if not runs:
        raise SystemExit(f"No k6 JSON summaries found under {args.results_dir}")

    run_df, summary_df = build_frames(runs, args.bootstrap_iterations)
    outputs = write_outputs(run_df, summary_df, args.output_dir, args.results_dir)
    print(f"Parsed {len(run_df)} runs into {len(summary_df)} groups")
    for path in outputs:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
