import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df[df["success"].astype(str).str.lower().isin(["true", "1"])]

    for col in ["n", "trial", "disk_reads", "disk_writes", "total_io", "time_ms", "row_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["n", "total_io", "time_ms"])

    return df


def make_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["index_type", "operation", "n"], as_index=False)
        .agg(
            avg_disk_reads=("disk_reads", "mean"),
            avg_disk_writes=("disk_writes", "mean"),
            avg_total_io=("total_io", "mean"),
            avg_time_ms=("time_ms", "mean"),
            avg_row_count=("row_count", "mean"),
            trials=("trial", "count"),
        )
        .sort_values(["operation", "n", "index_type"])
    )

    return summary


def plot_metric(summary: pd.DataFrame, operation: str, metric: str, ylabel: str, output_path: Path) -> None:
    data = summary[summary["operation"] == operation]

    if data.empty:
        return

    plt.figure(figsize=(9, 5))

    for index_type in sorted(data["index_type"].unique()):
        subset = data[data["index_type"] == index_type].sort_values("n")
        plt.plot(subset["n"], subset[metric], marker="o", label=index_type)

    plt.title(f"{ylabel} - {operation}")
    plt.xlabel("Tamaño del dataset (n)")
    plt.ylabel(ylabel)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_bar(summary: pd.DataFrame, operation: str, metric: str, ylabel: str, output_path: Path) -> None:
    data = summary[summary["operation"] == operation]

    if data.empty:
        return

    pivot = data.pivot(index="n", columns="index_type", values=metric)

    ax = pivot.plot(kind="bar", figsize=(9, 5))
    ax.set_title(f"{ylabel} - {operation}")
    ax.set_xlabel("Tamaño del dataset (n)")
    ax.set_ylabel(ylabel)
    ax.grid(True, axis="y")

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def generate_markdown_summary(summary: pd.DataFrame, output_path: Path) -> None:
    lines = []

    lines.append("# Resultados experimentales\n")
    lines.append("Las métricas reportadas corresponden al promedio por operación.\n")
    lines.append("- `avg_total_io` = páginas leídas + páginas escritas.\n")
    lines.append("- `avg_time_ms` = tiempo de ejecución promedio en milisegundos.\n\n")

    for operation in summary["operation"].unique():
        data = summary[summary["operation"] == operation]
        lines.append(f"## {operation}\n")
        lines.append(data.to_markdown(index=False))
        lines.append("\n\n")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--results",
        required=True,
        help="Ruta a nypd_benchmark_results.csv",
    )

    parser.add_argument(
        "--out-dir",
        default="experimental_results/plots",
        help="Carpeta de salida para gráficos",
    )

    args = parser.parse_args()

    results_path = Path(args.results)
    out_dir = Path(args.out_dir)

    out_dir.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(results_path)
    df = clean_numeric(raw)
    summary = make_summary(df)

    summary_path = out_dir.parent / "nypd_benchmark_summary.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8")

    markdown_path = out_dir.parent / "nypd_benchmark_summary.md"
    generate_markdown_summary(summary, markdown_path)

    operations = [
        "bulk_load_from_csv",
        "insert",
        "search",
        "range_search",
        "spatial_range_search",
        "spatial_knn",
    ]

    for operation in operations:
        plot_metric(
            summary,
            operation,
            "avg_time_ms",
            "Tiempo promedio (ms)",
            out_dir / f"line_time_{operation}.png",
        )

        plot_metric(
            summary,
            operation,
            "avg_total_io",
            "Accesos a disco promedio (read + write)",
            out_dir / f"line_io_{operation}.png",
        )

        plot_bar(
            summary,
            operation,
            "avg_time_ms",
            "Tiempo promedio (ms)",
            out_dir / f"bar_time_{operation}.png",
        )

        plot_bar(
            summary,
            operation,
            "avg_total_io",
            "Accesos a disco promedio (read + write)",
            out_dir / f"bar_io_{operation}.png",
        )

    print("Gráficos generados en:", out_dir)
    print("Resumen CSV:", summary_path)
    print("Resumen Markdown:", markdown_path)


if __name__ == "__main__":
    main()