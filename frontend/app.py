from __future__ import annotations

import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request
from werkzeug.utils import secure_filename

# Permite ejecutar: python frontend/app.py desde la raíz del proyecto.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.db_engine import DBEngine  # noqa: E402


DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "db"
DEFAULT_CSV_DIR = PROJECT_ROOT / "data" / "csv"
EXECUTION_HISTORY: list[dict[str, Any]] = []
MAX_HISTORY = 80


def create_app(data_dir: str | Path | None = None, csv_dir: str | Path | None = None) -> Flask:
    """
    Crea la app web tipo pgAdmin para el mini DBMS.

    La interfaz cubre lo pedido en la Fase 9:
    - editor SQL con resaltado básico usando CodeMirror,
    - tabla de resultados,
    - estadísticas de lecturas/escrituras/tiempo,
    - log de ejecución,
    - visualización espacial para consultas R-Tree.
    """
    app = Flask(
        __name__,
        template_folder=str(PROJECT_ROOT / "frontend" / "templates"),
        static_folder=str(PROJECT_ROOT / "frontend" / "static"),
    )
    engine = DBEngine(data_dir or DEFAULT_DATA_DIR)
    app.config["CSV_UPLOAD_DIR"] = Path(csv_dir or DEFAULT_CSV_DIR)
    app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/health")
    def health():
        return jsonify({"ok": True, "message": "Mini DBMS frontend activo"})

    @app.get("/api/tables")
    def tables():
        return jsonify(_safe_tables_payload(engine))

    @app.post("/api/upload_csv")
    def upload_csv():
        """
        Sube un CSV al directorio data/csv y genera una sentencia
        CREATE TABLE ... FROM FILE editable en el editor SQL.

        Importante: este endpoint NO carga el CSV a la base por sí solo.
        Para cumplir el enunciado, la carga real sigue haciéndose mediante
        el comando CREATE TABLE ... FROM FILE ejecutado por el usuario.
        """
        uploaded = request.files.get("csv_file") or request.files.get("file")
        if uploaded is None or not uploaded.filename:
            return jsonify({"success": False, "error": "Selecciona un archivo CSV."}), 400

        original_name = uploaded.filename
        safe_name = secure_filename(original_name)
        if not safe_name.lower().endswith(".csv"):
            return jsonify({"success": False, "error": "Solo se permiten archivos .csv."}), 400

        upload_dir = Path(app.config["CSV_UPLOAD_DIR"])
        upload_dir.mkdir(parents=True, exist_ok=True)
        target_path = _unique_csv_path(upload_dir / safe_name)
        uploaded.save(target_path)

        table_name_raw = request.form.get("table_name") or target_path.stem
        table_name = _sanitize_identifier(table_name_raw, fallback="table")

        try:
            inferred = _infer_csv_schema(target_path)
            suggested_sql = _build_create_table_sql(table_name, inferred["columns"], target_path)
            preview_rows = inferred["preview_rows"]
            row_count_estimate = inferred["row_count_estimate"]
        except Exception as exc:
            target_path.unlink(missing_ok=True)
            return jsonify({"success": False, "error": f"No se pudo leer el CSV: {exc}"}), 200

        log_item = {
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "status": "OK",
            "command_type": "upload_csv",
            "message": f"CSV subido: {target_path.name}. Ejecuta el CREATE TABLE generado para cargarlo.",
            "rows": row_count_estimate,
            "disk_reads": 0,
            "disk_writes": 0,
            "disk_accesses": 0,
            "time_ms": 0.0,
            "used_index": "-",
            "sql": suggested_sql,
        }
        _append_history(log_item)

        return jsonify({
            "success": True,
            "message": log_item["message"],
            "file_name": target_path.name,
            "file_path": _path_for_sql(target_path),
            "table_name": table_name,
            "columns": inferred["columns"],
            "preview_rows": preview_rows,
            "row_count_estimate": row_count_estimate,
            "suggested_sql": suggested_sql,
            "history": EXECUTION_HISTORY[-MAX_HISTORY:],
        })


    @app.post("/api/execute")
    def execute_sql():
        payload = request.get_json(silent=True) or {}
        sql = str(payload.get("sql", "")).strip()
        if not sql:
            return jsonify({"success": False, "error": "Ingresa una consulta SQL."}), 400

        results_payload: list[dict[str, Any]] = []
        try:
            # Si vienen varias sentencias, ejecutamos script; si es una sola,
            # usamos execute para preservar el comportamiento normal.
            if _contains_multiple_statements(sql):
                results = engine.execute_script(sql, raise_errors=False)
            else:
                results = [engine.execute(sql, raise_errors=False)]

            for result in results:
                item = result.to_dict()
                if isinstance(result.raw, dict) and "spatial" in result.raw:
                    item["spatial"] = result.raw["spatial"]
                item["log"] = _build_log_entry(sql, item)
                results_payload.append(item)
                _append_history(item["log"])

            last_result = results_payload[-1] if results_payload else {}
            return jsonify(
                {
                    "success": bool(last_result.get("success", False)),
                    "results": results_payload,
                    "last_result": last_result,
                    "history": EXECUTION_HISTORY[-MAX_HISTORY:],
                    "tables": _safe_tables_payload(engine),
                }
            )
        except Exception as exc:
            error_item = {
                "success": False,
                "command_type": "unknown",
                "message": str(exc),
                "error": str(exc),
                "columns": [],
                "rows": [],
                "row_count": 0,
                "disk_reads": 0,
                "disk_writes": 0,
                "disk_accesses": 0,
                "time_ms": 0.0,
                "used_index": None,
            }
            error_item["log"] = _build_log_entry(sql, error_item)
            _append_history(error_item["log"])
            return jsonify(
                {
                    "success": False,
                    "results": [error_item],
                    "last_result": error_item,
                    "history": EXECUTION_HISTORY[-MAX_HISTORY:],
                    "tables": _safe_tables_payload(engine),
                    "error": str(exc),
                }
            ), 200

    return app


def _contains_multiple_statements(sql: str) -> bool:
    stripped = sql.strip()
    if not stripped:
        return False
    parts = [part.strip() for part in stripped.split(";") if part.strip()]
    return len(parts) > 1


def _build_log_entry(sql: str, result: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "status": "OK" if result.get("success") else "ERROR",
        "command_type": result.get("command_type", "unknown"),
        "message": result.get("message", ""),
        "rows": int(result.get("row_count", 0) or 0),
        "disk_reads": int(result.get("disk_reads", 0) or 0),
        "disk_writes": int(result.get("disk_writes", 0) or 0),
        "disk_accesses": int(result.get("disk_accesses", 0) or 0),
        "time_ms": round(float(result.get("time_ms", 0.0) or 0.0), 3),
        "used_index": result.get("used_index") or "-",
        "sql": sql,
    }


def _append_history(item: dict[str, Any]) -> None:
    EXECUTION_HISTORY.append(item)
    del EXECUTION_HISTORY[:-MAX_HISTORY]



def _unique_csv_path(path: Path) -> Path:
    """Evita sobrescribir CSV existentes agregando sufijo incremental."""
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def _sanitize_identifier(value: str, fallback: str = "col") -> str:
    """Convierte nombres de tabla/columna del CSV a identificadores simples."""
    text = re.sub(r"[^0-9A-Za-z_]+", "_", str(value).strip().lower())
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        text = fallback
    if text[0].isdigit():
        text = f"{fallback}_{text}"
    return text


def _infer_csv_schema(csv_path: Path, sample_size: int = 100) -> dict[str, Any]:
    """Infere tipos básicos int/float/str desde el encabezado y filas de muestra."""
    with csv_path.open("r", newline="", encoding="utf-8-sig") as fh:
        sample_text = fh.read(8192)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample_text, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel

        reader = csv.reader(fh, dialect)
        try:
            raw_headers = next(reader)
        except StopIteration as exc:
            raise ValueError("El archivo está vacío") from exc

        headers = _deduplicate_identifiers([
            _sanitize_identifier(header or f"col_{idx + 1}", fallback="col")
            for idx, header in enumerate(raw_headers)
        ])

        samples: list[list[str]] = []
        total_rows = 0
        for row in reader:
            if not row or not any(str(cell).strip() for cell in row):
                continue
            normalized = [str(cell).strip() for cell in row[:len(headers)]]
            while len(normalized) < len(headers):
                normalized.append("")
            total_rows += 1
            if len(samples) < sample_size:
                samples.append(normalized)

    columns = []
    for col_idx, name in enumerate(headers):
        values = [row[col_idx] for row in samples if col_idx < len(row)]
        columns.append({"name": name, "type": _infer_type(values)})

    columns = _suggest_indexes(columns)
    preview_rows = [dict(zip(headers, row)) for row in samples[:8]]
    return {
        "columns": columns,
        "preview_rows": preview_rows,
        "row_count_estimate": total_rows,
    }


def _deduplicate_identifiers(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result: list[str] = []
    for name in names:
        base = name or "col"
        count = seen.get(base, 0)
        seen[base] = count + 1
        result.append(base if count == 0 else f"{base}_{count + 1}")
    return result


def _infer_type(values: list[str]) -> str:
    non_empty = [value for value in values if value not in {"", "NULL", "null", "None"}]
    if not non_empty:
        return "str"
    if all(_is_int(value) for value in non_empty):
        return "int"
    if all(_is_float(value) for value in non_empty):
        return "float"
    return "str"


def _is_int(value: str) -> bool:
    try:
        int(str(value).strip())
        return "." not in str(value).strip()
    except ValueError:
        return False


def _is_float(value: str) -> bool:
    try:
        float(str(value).strip())
        return True
    except ValueError:
        return False


def _suggest_indexes(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sugiere índices editables, sin cargar el CSV automáticamente."""
    by_name = {col["name"]: col for col in columns}

    # Caso espacial típico: longitude/latitude o lon/lat.
    longitude_candidates = ["longitude", "longitud", "lon", "lng", "x"]
    latitude_candidates = ["latitude", "latitud", "lat", "y"]
    lon_col = next((by_name[name] for name in longitude_candidates if name in by_name), None)
    lat_col = next((by_name[name] for name in latitude_candidates if name in by_name), None)
    if lon_col and lat_col and lon_col.get("type") in {"float", "int"} and lat_col.get("type") in {"float", "int"}:
        lon_col["index"] = "rtree"

    # Caso clásico: id/código con B+ Tree.
    for col in columns:
        if col["name"] in {"id", "codigo", "code", "key"} and col.get("type") in {"int", "float", "str"}:
            col.setdefault("index", "bplus")
            return columns

    # Si no hay id, sugerimos B+ Tree en la primera columna numérica.
    first_numeric = next((col for col in columns if col.get("type") in {"int", "float"}), None)
    if first_numeric:
        first_numeric.setdefault("index", "bplus")
    return columns


def _build_create_table_sql(table_name: str, columns: list[dict[str, Any]], csv_path: Path) -> str:
    lines = []
    for column in columns:
        index_part = f" INDEX {column['index']}" if column.get("index") else ""
        lines.append(f"    {column['name']} {column['type']}{index_part}")
    columns_sql = ",\n".join(lines)
    path_sql = _path_for_sql(csv_path)
    return f"CREATE TABLE {table_name} (\n{columns_sql}\n) FROM FILE '{path_sql}';"


def _path_for_sql(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _safe_tables_payload(engine: DBEngine) -> dict[str, Any]:
    tables: list[dict[str, Any]] = []
    try:
        for table_name in engine.list_tables():
            try:
                tables.append(engine.describe_table(table_name))
            except Exception as exc:
                tables.append({"name": table_name, "error": str(exc)})
    except Exception as exc:
        return {"tables": [], "error": str(exc)}
    return {"tables": tables}


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
