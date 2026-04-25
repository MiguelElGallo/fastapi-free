#!/usr/bin/env python3
"""Generate reports for host-native free-threading proof campaigns."""

from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path
from typing import Any

pd: Any = None
plt: Any = None


def import_dependencies() -> None:
    global pd, plt
    try:
        import matplotlib.pyplot as imported_plt
        import pandas as imported_pd
    except ModuleNotFoundError as exc:
        missing = exc.name or "required package"
        raise SystemExit(
            f"Missing Python dependency: {missing}. "
            "Run `uv sync --group analysis` before generating reports."
        ) from exc
    pd = imported_pd
    plt = imported_plt


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"{path} did not contain a JSON object")
    return value


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                value = json.loads(line)
                if isinstance(value, dict):
                    rows.append(value)
    return rows


def find_parallel_results(results_dir: Path) -> list[Path]:
    return sorted(path for path in results_dir.rglob("results.jsonl") if path.is_file())


def find_frontier_results(results_dir: Path) -> list[Path]:
    return sorted(path for path in results_dir.rglob("cell-summary.json") if path.is_file())


def proof_rows(results_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in find_parallel_results(results_dir):
        for result in read_jsonl(path):
            batch = result.get("batch", {})
            summary = batch.get("summary", {})
            condition = result.get("condition", {})
            rows.append(
                {
                    "source": str(path),
                    "run_id": result.get("run_id"),
                    "condition": condition.get("label"),
                    "python": condition.get("python"),
                    "gil": condition.get("gil"),
                    "thread_tokens": result.get("thread_tokens"),
                    "participants": result.get("participants"),
                    "repeat": result.get("repeat"),
                    "batch_id": batch.get("batch_id"),
                    "rounds": batch.get("rounds"),
                    "successes": batch.get("successes"),
                    "failures": batch.get("failures"),
                    "wall_ns": summary.get("wall_ns"),
                    "sum_thread_cpu_ns": summary.get("sum_thread_cpu_ns"),
                    "parallelism_ratio": summary.get("parallelism_ratio"),
                    "participants_completed": summary.get("participants_completed"),
                    "participants_failed": summary.get("participants_failed"),
                    "spans": summary.get("spans", []),
                }
            )
    return pd.DataFrame(rows)


def frontier_rows(results_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in find_frontier_results(results_dir):
        result = load_json(path)
        condition = result.get("condition", {})
        metrics = result.get("metrics", {})
        classification = result.get("classification", {})
        telemetry = result.get("telemetry", {})
        rows.append(
            {
                "source": str(path),
                "run_id": result.get("run_id"),
                "condition": condition.get("label"),
                "python": condition.get("python"),
                "gil": condition.get("gil"),
                "thread_tokens": result.get("thread_tokens"),
                "target_rps": result.get("target_rps"),
                "repeat": result.get("repeat"),
                "valid": classification.get("valid"),
                "classification": classification.get("classification"),
                "reasons": ",".join(classification.get("reasons", [])),
                "requests_per_sec": metrics.get("requests_per_sec"),
                "request_count": metrics.get("request_count"),
                "duration_p95_ms": metrics.get("duration_p95_ms"),
                "duration_p99_ms": metrics.get("duration_p99_ms"),
                "failed_requests_rate": metrics.get("failed_requests_rate"),
                "checks_rate": metrics.get("checks_rate"),
                "dropped_iterations": metrics.get("dropped_iterations"),
                "app_effective_cores_mean": telemetry.get("app_effective_cores_mean"),
                "app_effective_cores_max": telemetry.get("app_effective_cores_max"),
                "k6_effective_cores_mean": telemetry.get("k6_effective_cores_mean"),
                "app_rss_kb_max": telemetry.get("app_rss_kb_max"),
            }
        )
    return pd.DataFrame(rows)


def bootstrap_ci(values: list[float], iterations: int = 1000) -> tuple[float | None, float | None]:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if len(clean) < 3:
        return None, None
    rng = random.Random(44)
    samples = []
    for _ in range(iterations):
        draw = [rng.choice(clean) for _ in clean]
        samples.append(float(pd.Series(draw).median()))
    samples.sort()
    return samples[int(0.025 * len(samples))], samples[int(0.975 * len(samples))]


def summarize_proof(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    grouped = df.groupby(["condition", "thread_tokens", "participants"], dropna=False)
    for keys, group in grouped:
        values = pd.to_numeric(group["parallelism_ratio"], errors="coerce").dropna().tolist()
        ci_low, ci_high = bootstrap_ci(values)
        rows.append(
            {
                "condition": keys[0],
                "thread_tokens": keys[1],
                "participants": keys[2],
                "runs": len(group),
                "parallelism_ratio_median": float(pd.Series(values).median()) if values else None,
                "parallelism_ratio_mean": float(pd.Series(values).mean()) if values else None,
                "parallelism_ratio_ci95_low": ci_low,
                "parallelism_ratio_ci95_high": ci_high,
                "failures": int(pd.to_numeric(group["failures"], errors="coerce").fillna(0).sum()),
            }
        )
    return pd.DataFrame(rows).sort_values(["condition", "thread_tokens", "participants"])


def summarize_frontier(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    valid = df[df["valid"] == True].copy()  # noqa: E712
    if valid.empty:
        return pd.DataFrame()
    rows = []
    grouped = valid.groupby(["condition", "thread_tokens"], dropna=False)
    for keys, group in grouped:
        max_rps = pd.to_numeric(group["target_rps"], errors="coerce").max()
        frontier = group[pd.to_numeric(group["target_rps"], errors="coerce") == max_rps]
        rows.append(
            {
                "condition": keys[0],
                "thread_tokens": keys[1],
                "max_sustainable_rps": float(max_rps),
                "runs_at_frontier": len(frontier),
                "achieved_rps_median": float(
                    pd.to_numeric(frontier["requests_per_sec"], errors="coerce").median()
                ),
                "p99_ms_median": float(
                    pd.to_numeric(frontier["duration_p99_ms"], errors="coerce").median()
                ),
                "app_effective_cores_median": float(
                    pd.to_numeric(frontier["app_effective_cores_mean"], errors="coerce").median()
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["condition", "thread_tokens"])


def ensure_chart_dir(output_dir: Path) -> Path:
    chart_dir = output_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)
    return chart_dir


def plot_parallelism(summary: pd.DataFrame, chart_dir: Path) -> Path | None:
    if summary.empty:
        return None
    fig, axis = plt.subplots(figsize=(10, 6))
    for label, group in summary.groupby("condition"):
        group = group.sort_values("participants")
        axis.plot(group["participants"], group["parallelism_ratio_median"], marker="o", label=label)
    axis.axhline(1.0, color="#666666", linestyle="--", linewidth=1)
    axis.set_xlabel("Concurrent proof participants")
    axis.set_ylabel("sum(thread CPU time) / wall time")
    axis.set_title("Server-side Python Thread Parallelism")
    axis.grid(alpha=0.25)
    axis.legend()
    fig.tight_layout()
    path = chart_dir / "parallelism_ratio.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def plot_gantt(proof_df: pd.DataFrame, chart_dir: Path) -> Path | None:
    if proof_df.empty:
        return None
    candidates = proof_df.sort_values("participants", ascending=False)
    row = next((item for _, item in candidates.iterrows() if item.get("spans")), None)
    if row is None:
        return None
    spans = [span for span in row["spans"] if span.get("error") is None]
    if not spans:
        return None
    start = min(int(span["perf_counter_start_ns"]) for span in spans)
    fig, axis = plt.subplots(figsize=(10, max(3, 0.35 * len(spans) + 1.5)))
    sorted_spans = sorted(spans, key=lambda item: item["slot"])
    for index, span in enumerate(sorted_spans):
        x = (int(span["perf_counter_start_ns"]) - start) / 1_000_000
        width = (int(span["perf_counter_end_ns"]) - int(span["perf_counter_start_ns"])) / 1_000_000
        axis.barh(index, width, left=x, color="#228833")
    axis.set_yticks(range(len(spans)), [f"slot {span['slot']}" for span in sorted_spans])
    axis.set_xlabel("Milliseconds since first compute start")
    axis.set_title(f"Worker Thread Overlap: {row['condition']} participants={row['participants']}")
    axis.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    path = chart_dir / "thread_span_gantt.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def plot_frontier(summary: pd.DataFrame, chart_dir: Path) -> list[Path]:
    if summary.empty:
        return []
    paths: list[Path] = []
    fig, axis = plt.subplots(figsize=(10, 6))
    for label, group in summary.groupby("condition"):
        group = group.sort_values("thread_tokens")
        axis.plot(group["thread_tokens"], group["max_sustainable_rps"], marker="o", label=label)
    axis.set_xlabel("AnyIO thread tokens")
    axis.set_ylabel("Max sustainable offered RPS")
    axis.set_title("Saturation Frontier")
    axis.grid(alpha=0.25)
    axis.legend()
    fig.tight_layout()
    path = chart_dir / "max_sustainable_rps.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    paths.append(path)

    fig, axis = plt.subplots(figsize=(10, 6))
    for label, group in summary.groupby("condition"):
        axis.scatter(
            group["achieved_rps_median"],
            group["app_effective_cores_median"],
            label=label,
            s=60,
        )
    axis.set_xlabel("Achieved RPS at frontier")
    axis.set_ylabel("App effective cores")
    axis.set_title("CPU Core Utilization vs Throughput")
    axis.grid(alpha=0.25)
    axis.legend()
    fig.tight_layout()
    path = chart_dir / "effective_cores_vs_rps.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    paths.append(path)
    return paths


def speedup_table(frontier_summary: pd.DataFrame) -> pd.DataFrame:
    if frontier_summary.empty:
        return pd.DataFrame()
    pivot = frontier_summary.pivot_table(
        index="thread_tokens",
        columns="condition",
        values="max_sustainable_rps",
        aggfunc="max",
    )
    candidates = [
        ("py314t-gil-off", "py314t-gil-on"),
        ("py314t-off", "py314t-on"),
        ("py314t_gil_off", "py314t_gil_on"),
    ]
    rows = []
    for off_label, on_label in candidates:
        if off_label in pivot.columns and on_label in pivot.columns:
            for thread_tokens, values in pivot[[off_label, on_label]].dropna().iterrows():
                baseline = float(values[on_label])
                current = float(values[off_label])
                rows.append(
                    {
                        "thread_tokens": thread_tokens,
                        "gil_off_condition": off_label,
                        "gil_on_condition": on_label,
                        "gil_off_rps": current,
                        "gil_on_rps": baseline,
                        "speedup": current / baseline if baseline else None,
                        "speedup_percent": ((current / baseline) - 1.0) * 100 if baseline else None,
                    }
                )
    return pd.DataFrame(rows)


def markdown_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df.empty:
        return "_No data._"
    table = df.head(max_rows).copy()
    for column in table.columns:
        if pd.api.types.is_numeric_dtype(table[column]):
            table[column] = table[column].map(
                lambda value: "" if pd.isna(value) else f"{float(value):.3f}"
            )
    table = table.fillna("").astype(str)
    header = "| " + " | ".join(table.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(table.columns)) + " |"
    body = ["| " + " | ".join(row) + " |" for row in table.itertuples(index=False, name=None)]
    return "\n".join([header, separator, *body])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, default=Path("bench/results"))
    parser.add_argument("--output-dir", type=Path, default=Path("analysis/proof-report"))
    args = parser.parse_args()

    import_dependencies()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    chart_dir = ensure_chart_dir(args.output_dir)

    proof_df = proof_rows(args.results_dir)
    frontier_df = frontier_rows(args.results_dir)
    proof_summary = summarize_proof(proof_df)
    frontier_summary = summarize_frontier(frontier_df)
    speedups = speedup_table(frontier_summary)

    proof_df.to_csv(args.output_dir / "proof_runs.csv", index=False)
    proof_summary.to_csv(args.output_dir / "proof_summary.csv", index=False)
    frontier_df.to_csv(args.output_dir / "frontier_runs.csv", index=False)
    frontier_summary.to_csv(args.output_dir / "frontier_summary.csv", index=False)
    speedups.to_csv(args.output_dir / "speedups.csv", index=False)

    chart_paths = [
        path
        for path in [
            plot_parallelism(proof_summary, chart_dir),
            plot_gantt(proof_df, chart_dir),
            *plot_frontier(frontier_summary, chart_dir),
        ]
        if path is not None
    ]

    lines = [
        "# Host-Native Free-Threading Proof Report",
        "",
        f"Source results: `{args.results_dir}`",
        f"Proof rows: `{len(proof_df)}`",
        f"Frontier rows: `{len(frontier_df)}`",
        "",
        "## Charts",
        "",
    ]
    for path in chart_paths:
        rel = path.relative_to(args.output_dir)
        lines.append(f"![{path.stem}]({rel.as_posix()})")
        lines.append("")

    lines.extend(
        [
            "## Mechanism Proof Summary",
            "",
            markdown_table(proof_summary),
            "",
            "## Frontier Summary",
            "",
            markdown_table(frontier_summary),
            "",
            "## GIL-Off Speedups",
            "",
            markdown_table(speedups),
            "",
        ]
    )
    (args.output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
