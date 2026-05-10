import argparse
import csv
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.db_engine import DBEngine
from backend.concurrency.operation_log import OperationLog


COLUMNS_SQL = """
    arrest_key int INDEX bplus,
    arrest_date str,
    pd_cd int,
    ky_cd int,
    ofns_desc str,
    law_cat_cd str,
    arrest_boro str,
    arrest_precinct int,
    age_group str,
    perp_sex str,
    perp_race str,
    longitude float,
    latitude float
"""


def read_middle_key(csv_path: Path) -> int:
    keys = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            keys.append(int(float(row["arrest_key"])))

    if not keys:
        raise ValueError("El CSV no tiene registros válidos.")

    return keys[len(keys) // 2]


def result_to_dict(result):
    if hasattr(result, "to_dict"):
        return result.to_dict()
    if isinstance(result, dict):
        return result
    return result.__dict__


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--csv",
        required=True,
        help="Ruta al CSV nypd_arrests_1000.csv",
    )

    parser.add_argument(
        "--workspace",
        default="experimental_results/concurrency_demo_db",
        help="Carpeta temporal para la BD de la demo",
    )

    args = parser.parse_args()

    csv_path = Path(args.csv).resolve()
    workspace = Path(args.workspace)

    if not csv_path.exists():
        raise FileNotFoundError(f"No existe el CSV: {csv_path}")

    if workspace.exists():
        shutil.rmtree(workspace)

    workspace.mkdir(parents=True, exist_ok=True)

    engine = DBEngine(str(workspace))

    table_name = "nypd_concurrency_demo"

    print("\n===== CARGA DE TABLA PARA DEMO DE CONCURRENCIA =====\n")

    create_sql = f"""
CREATE TABLE {table_name} (
{COLUMNS_SQL}
) FROM FILE '{csv_path.as_posix()}';
"""

    create_result = engine.execute(create_sql)
    create_payload = result_to_dict(create_result)

    print("Tabla creada:", table_name)
    print("Filas cargadas:", create_payload.get("row_count", create_payload.get("rows_inserted", "")))
    print("Lecturas:", create_payload.get("disk_reads", 0))
    print("Escrituras:", create_payload.get("disk_writes", 0))
    print("Tiempo ms:", create_payload.get("time_ms", 0))

    target_key = read_middle_key(csv_path)

    print("\nClave usada para simular conflicto:", target_key)

    # Buscar RID real usando el índice B+ Tree.
    table_schema = engine.catalog.get_table(table_name)
    index = engine._open_index(table_schema, "arrest_key")

    rids = index.search(target_key)

    if not rids:
        raise ValueError(f"No se encontró RID para arrest_key={target_key}")

    target_rid = rids[0]

    data_page = target_rid.page_id
    index_page = 0  # página raíz o página inicial del índice para la simulación

    print("RID encontrado:", target_rid)
    print("Página de datos involucrada:", data_page)
    print("Página de índice simulada:", index_page)

    log = OperationLog()

    resource_index = f"{table_name}_arrest_key_bplus.idx"
    resource_table = f"{table_name}.tbl"

    print("\n===== SCHEDULE INTERCALADO =====\n")

    schedule = [
        ("T1", "READ", resource_index, index_page, f"search arrest_key={target_key} en B+ Tree"),
        ("T2", "READ", resource_index, index_page, f"search arrest_key={target_key} en B+ Tree"),
        ("T1", "READ", resource_table, data_page, f"lee registro RID={target_rid}"),
        ("T2", "READ", resource_table, data_page, f"lee registro RID={target_rid}"),
        ("T2", "DELETE", resource_table, data_page, f"elimina registro con arrest_key={target_key}"),
        ("T2", "WRITE", resource_index, index_page, f"actualiza B+ Tree por DELETE arrest_key={target_key}"),
        ("T1", "READ", resource_table, data_page, "T1 intenta leer una página modificada por T2"),
        ("T1", "COMMIT", "transaction", None, "commit T1"),
        ("T2", "COMMIT", "transaction", None, "commit T2"),
    ]

    for i, (tx_id, action, resource, page_id, detail) in enumerate(schedule, start=1):
        conflicts = log.add(
            tx_id=tx_id,
            action=action,
            resource=resource,
            page_id=page_id,
            detail=detail,
        )

        page_label = "-" if page_id is None else page_id
        print(f"{i:02d}. {tx_id} {action:<7} resource={resource:<35} page={page_label} | {detail}")

        for conflict in conflicts:
            print(f"    CONFLICTO: {conflict.conflict_type} -> {conflict.reason}")

    payload = log.to_dict()

    print("\n===== RESUMEN DEL LOG =====\n")
    print("Operaciones registradas:", payload["operation_count"])
    print("Conflictos detectados:", payload["conflict_count"])

    print("\n===== CONFLICTOS DETECTADOS =====\n")

    if not payload["conflicts"]:
        print("No se detectaron conflictos.")
    else:
        for i, conflict in enumerate(payload["conflicts"], start=1):
            print(f"{i}. Tipo:", conflict["type"])
            print("   Recurso:", conflict["resource_key"])
            print("   Motivo:", conflict["reason"])
            print("")

    print("Demo terminada.")


if __name__ == "__main__":
    main()