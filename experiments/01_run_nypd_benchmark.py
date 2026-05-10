import argparse
import csv
import shutil
import sys
from pathlib import Path
from typing import Any


# Permite importar backend/ aunque este script esté en experiments/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.db_engine import DBEngine  # noqa: E402


COLUMNS = [
    ("arrest_key", "int"),
    ("arrest_date", "str"),
    ("pd_cd", "int"),
    ("ky_cd", "int"),
    ("ofns_desc", "str"),
    ("law_cat_cd", "str"),
    ("arrest_boro", "str"),
    ("arrest_precinct", "int"),
    ("age_group", "str"),
    ("perp_sex", "str"),
    ("perp_race", "str"),
    ("longitude", "float"),
    ("latitude", "float"),
]


NORMAL_INDEXES = ["sequential", "bplus", "extendible_hash"]


def sql_literal(value: Any, col_type: str) -> str:
    if value is None:
        value = ""

    value = str(value).strip()

    if col_type == "int":
        if value == "":
            return "0"
        return str(int(float(value)))

    if col_type == "float":
        if value == "":
            return "0.0"
        return str(float(value))

    # Evitamos problemas con comillas simples en el parser.
    value = value.replace("'", " ")
    return f"'{value}'"


def result_to_dict(result: Any) -> dict[str, Any]:
    if hasattr(result, "to_dict"):
        data = result.to_dict()
    elif isinstance(result, dict):
        data = result
    else:
        data = result.__dict__

    reads = int(data.get("disk_reads", data.get("reads", 0)) or 0)
    writes = int(data.get("disk_writes", data.get("writes", 0)) or 0)

    return {
        "success": data.get("success", True),
        "message": data.get("message", ""),
        "row_count": data.get("row_count", len(data.get("rows", []) or [])),
        "disk_reads": reads,
        "disk_writes": writes,
        "total_io": reads + writes,
        "time_ms": float(data.get("time_ms", 0) or 0),
        "used_index": data.get("used_index", ""),
        "error": data.get("error", ""),
    }


def read_nypd_reference_rows(csv_path: Path, n: int) -> dict[str, Any]:
    """
    Lee el CSV de forma secuencial, sin cargarlo completo.
    Obtiene:
    - una fila media para search
    - un rango de claves
    - una fila base para INSERT
    - un punto longitude/latitude para R-Tree
    """
    target_mid = max(0, n // 2)
    target_range_end = min(n - 1, target_mid + 100)

    mid_row = None
    range_end_row = None
    last_row = None
    max_key = None

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for i, row in enumerate(reader):
            if i >= n:
                break

            key = int(float(row["arrest_key"]))
            if max_key is None or key > max_key:
                max_key = key

            if i == target_mid:
                mid_row = row

            if i == target_range_end:
                range_end_row = row

            last_row = row

    if mid_row is None or range_end_row is None or last_row is None or max_key is None:
        raise ValueError(f"No se pudo leer suficientes filas desde {csv_path}")

    begin_key = int(float(mid_row["arrest_key"]))
    end_key = int(float(range_end_row["arrest_key"]))

    if begin_key > end_key:
        begin_key, end_key = end_key, begin_key

    return {
        "search_key": begin_key,
        "range_begin": begin_key,
        "range_end": end_key,
        "insert_base_row": last_row,
        "max_key": max_key,
        "longitude": float(mid_row["longitude"]),
        "latitude": float(mid_row["latitude"]),
    }


def build_create_sql(table_name: str, csv_path: Path, index_type: str) -> str:
    column_defs = []

    for col_name, col_type in COLUMNS:
        if col_name == "arrest_key":
            column_defs.append(f"    {col_name} {col_type} INDEX {index_type}")
        else:
            column_defs.append(f"    {col_name} {col_type}")

    columns_sql = ",\n".join(column_defs)

    return f"""
CREATE TABLE {table_name} (
{columns_sql}
) FROM FILE '{csv_path.resolve().as_posix()}';
"""


def build_create_spatial_sql(table_name: str, csv_path: Path) -> str:
    column_defs = []

    for col_name, col_type in COLUMNS:
        if col_name == "arrest_key":
            column_defs.append(f"    {col_name} {col_type} INDEX bplus")
        elif col_name == "longitude":
            column_defs.append(f"    {col_name} {col_type} INDEX rtree")
        else:
            column_defs.append(f"    {col_name} {col_type}")

    columns_sql = ",\n".join(column_defs)

    return f"""
CREATE TABLE {table_name} (
{columns_sql}
) FROM FILE '{csv_path.resolve().as_posix()}';
"""


def build_insert_sql(table_name: str, base_row: dict[str, Any], new_key: int) -> str:
    values = []

    for col_name, col_type in COLUMNS:
        if col_name == "arrest_key":
            values.append(str(new_key))
        else:
            values.append(sql_literal(base_row.get(col_name, ""), col_type))

    return f"INSERT INTO {table_name} VALUES ({', '.join(values)});"


def append_result(
    rows: list[dict[str, Any]],
    *,
    index_type: str,
    operation: str,
    n: int,
    trial: int,
    result: Any,
    notes: str = "",
) -> None:
    data = result_to_dict(result)

    rows.append(
        {
            "index_type": index_type,
            "operation": operation,
            "n": n,
            "trial": trial,
            "disk_reads": data["disk_reads"],
            "disk_writes": data["disk_writes"],
            "total_io": data["total_io"],
            "time_ms": data["time_ms"],
            "row_count": data["row_count"],
            "used_index": data["used_index"],
            "success": data["success"],
            "error": data["error"],
            "notes": notes,
        }
    )


def run_normal_index_benchmark(
    *,
    csv_path: Path,
    n: int,
    index_type: str,
    out_rows: list[dict[str, Any]],
    workspace_dir: Path,
    insert_trials: int,
) -> None:
    db_dir = workspace_dir / f"db_{index_type}_{n}"

    if db_dir.exists():
        shutil.rmtree(db_dir)

    db_dir.mkdir(parents=True, exist_ok=True)

    engine = DBEngine(str(db_dir))

    table_name = f"nypd_{index_type}_{n}"
    refs = read_nypd_reference_rows(csv_path, n)

    print(f"[{index_type}][n={n}] creando tabla desde CSV...")

    create_sql = build_create_sql(table_name, csv_path, index_type)
    create_result = engine.execute(create_sql)

    append_result(
        out_rows,
        index_type=index_type,
        operation="bulk_load_from_csv",
        n=n,
        trial=1,
        result=create_result,
        notes="CREATE TABLE ... FROM FILE",
    )

    print(f"[{index_type}][n={n}] search puntual...")

    search_sql = f"SELECT * FROM {table_name} WHERE arrest_key = {refs['search_key']};"
    search_result = engine.execute(search_sql)

    append_result(
        out_rows,
        index_type=index_type,
        operation="search",
        n=n,
        trial=1,
        result=search_result,
    )

    if index_type != "extendible_hash":
        print(f"[{index_type}][n={n}] range search...")

        range_sql = (
            f"SELECT * FROM {table_name} "
            f"WHERE arrest_key BETWEEN {refs['range_begin']} AND {refs['range_end']};"
        )

        range_result = engine.execute(range_sql)

        append_result(
            out_rows,
            index_type=index_type,
            operation="range_search",
            n=n,
            trial=1,
            result=range_result,
        )
    else:
        out_rows.append(
            {
                "index_type": index_type,
                "operation": "range_search",
                "n": n,
                "trial": 1,
                "disk_reads": "",
                "disk_writes": "",
                "total_io": "",
                "time_ms": "",
                "row_count": "",
                "used_index": "",
                "success": "not_applicable",
                "error": "",
                "notes": "Extendible Hashing no soporta rangeSearch",
            }
        )

    print(f"[{index_type}][n={n}] inserciones...")

    for trial in range(1, insert_trials + 1):
        new_key = refs["max_key"] + trial
        insert_sql = build_insert_sql(table_name, refs["insert_base_row"], new_key)
        insert_result = engine.execute(insert_sql)

        append_result(
            out_rows,
            index_type=index_type,
            operation="insert",
            n=n,
            trial=trial,
            result=insert_result,
            notes="single INSERT INTO",
        )


def run_rtree_benchmark(
    *,
    csv_path: Path,
    n: int,
    out_rows: list[dict[str, Any]],
    workspace_dir: Path,
) -> None:
    db_dir = workspace_dir / f"db_rtree_{n}"

    if db_dir.exists():
        shutil.rmtree(db_dir)

    db_dir.mkdir(parents=True, exist_ok=True)

    engine = DBEngine(str(db_dir))

    table_name = f"nypd_rtree_{n}"
    refs = read_nypd_reference_rows(csv_path, n)

    print(f"[rtree][n={n}] creando tabla espacial desde CSV...")

    create_sql = build_create_spatial_sql(table_name, csv_path)
    create_result = engine.execute(create_sql)

    append_result(
        out_rows,
        index_type="rtree",
        operation="bulk_load_from_csv",
        n=n,
        trial=1,
        result=create_result,
        notes="CREATE TABLE ... longitude INDEX rtree",
    )

    lon = refs["longitude"]
    lat = refs["latitude"]

    print(f"[rtree][n={n}] range espacial...")

    spatial_range_sql = (
        f"SELECT * FROM {table_name} "
        f"WHERE longitude IN (POINT({lon}, {lat}), RADIUS 0.01);"
    )

    spatial_range_result = engine.execute(spatial_range_sql)

    append_result(
        out_rows,
        index_type="rtree",
        operation="spatial_range_search",
        n=n,
        trial=1,
        result=spatial_range_result,
        notes="POINT + RADIUS",
    )

    print(f"[rtree][n={n}] kNN espacial...")

    spatial_knn_sql = (
        f"SELECT * FROM {table_name} "
        f"WHERE longitude IN (POINT({lon}, {lat}), K 10);"
    )

    spatial_knn_result = engine.execute(spatial_knn_sql)

    append_result(
        out_rows,
        index_type="rtree",
        operation="spatial_knn",
        n=n,
        trial=1,
        result=spatial_knn_result,
        notes="POINT + K",
    )


def write_results_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "index_type",
        "operation",
        "n",
        "trial",
        "disk_reads",
        "disk_writes",
        "total_io",
        "time_ms",
        "row_count",
        "used_index",
        "success",
        "error",
        "notes",
    ]

    with open(output_path, "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--csv-dir",
        required=True,
        help="Carpeta donde están nypd_arrests_1000.csv, nypd_arrests_10000.csv y nypd_arrests_100000.csv",
    )

    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=[1000, 10000, 100000],
        help="Tamaños a evaluar",
    )

    parser.add_argument(
        "--out-dir",
        default="experimental_results",
        help="Carpeta donde se guardarán resultados",
    )

    parser.add_argument(
        "--insert-trials",
        type=int,
        default=20,
        help="Número de INSERT individuales para promediar",
    )

    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    out_dir = Path(args.out_dir)
    workspace_dir = out_dir / "workspaces"
    results_path = out_dir / "nypd_benchmark_results.csv"

    rows: list[dict[str, Any]] = []

    for n in args.sizes:
        csv_path = csv_dir / f"nypd_arrests_{n}.csv"

        if not csv_path.exists():
            print(f"[WARN] No existe {csv_path}. Se omite n={n}.")
            continue

        for index_type in NORMAL_INDEXES:
            run_normal_index_benchmark(
                csv_path=csv_path,
                n=n,
                index_type=index_type,
                out_rows=rows,
                workspace_dir=workspace_dir,
                insert_trials=args.insert_trials,
            )

        run_rtree_benchmark(
            csv_path=csv_path,
            n=n,
            out_rows=rows,
            workspace_dir=workspace_dir,
        )

    write_results_csv(rows, results_path)

    print("\nBenchmark terminado.")
    print(f"Resultados guardados en: {results_path}")


if __name__ == "__main__":
    main()