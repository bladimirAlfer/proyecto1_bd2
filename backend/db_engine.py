from __future__ import annotations

import csv
import re
import shutil
from pathlib import Path
from typing import Any, Iterable

from .catalog import Catalog, TableSchema
from .catalog.index_schema import IndexSchema
from .indexes.base_index import BaseIndex
from .indexes.bplus_tree import BPlusTree
from .indexes.extendible_hash import ExtendibleHash
from .indexes.rtree_index import RTree
from .indexes.sequential_file import SequentialFile
from .storage.record_manager import RecordManager
from .storage.rid import RID
from .storage.serializer import Serializer


class DBEngine:
    DEFAULT_SELECT_LIMIT = 100

    """
    Fachada principal del mini DBMS hasta Fase 8.

    Responsabilidades actuales:
    - Crear tablas y registrar metadata en el catálogo.
    - Cargar CSV hacia archivos binarios paginados.
    - Mantener índices Sequential File, B+ Tree, Extendible Hashing y R-Tree declarados en columnas.
    - Ejecutar búsquedas exactas/rango usando el índice disponible.
    - Exponer execute(sql) para conectar Parser SQL + QueryExecutor.
    """

    def __init__(self, base_dir: str | Path = "data/db") -> None:
        self.base_dir = Path(base_dir)
        self.tables_dir = self.base_dir / "tables"
        self.indexes_dir = self.base_dir / "indexes"
        self.catalog_path = self.base_dir / "catalog.json"

        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.indexes_dir.mkdir(parents=True, exist_ok=True)

        self.catalog = Catalog(self.catalog_path)

    def execute(self, sql: str, *, raise_errors: bool = True):
        """Ejecuta una sentencia SQL usando el parser y QueryExecutor."""
        from .query import QueryExecutor

        return QueryExecutor(self, raise_errors=raise_errors).execute(sql)

    def execute_script(self, sql_script: str, *, raise_errors: bool = True):
        """Ejecuta varias sentencias SQL separadas por ';'."""
        from .query import QueryExecutor

        return QueryExecutor(self, raise_errors=raise_errors).execute_script(sql_script)

    # ------------------------------------------------------------------
    # DDL y carga de datos
    # ------------------------------------------------------------------

    def create_table(
        self,
        table_name: str,
        columns: list[dict[str, Any]],
        overwrite: bool = False,
    ) -> TableSchema:
        """
        Crea una tabla vacía.

        Ejemplo de columns:
        [
            {"name": "id", "type": "int", "index": "bplus"},
            {"name": "name", "type": "str"},
            {"name": "salary", "type": "float"},
        ]
        """
        self._validate_identifier(table_name, "tabla")
        data_file = self.tables_dir / f"{table_name}.tbl"

        if data_file.exists():
            if overwrite:
                data_file.unlink()
            elif not self.catalog.table_exists(table_name):
                raise ValueError(f"Ya existe el archivo físico de datos: {data_file}")

        if self.catalog.table_exists(table_name) and overwrite:
            old_schema = self.catalog.get_table(table_name)
            self._delete_index_files(old_schema)
            self.catalog.drop_table(table_name, delete_files=False)

        table_schema = Catalog.build_table_schema(table_name, columns, data_file)
        self._rewrite_index_paths(table_schema)
        self.catalog.create_table(table_schema, overwrite=overwrite)

        data_file.parent.mkdir(parents=True, exist_ok=True)
        data_file.touch(exist_ok=True)

        # Crear archivos físicos vacíos para índices soportados en esta fase.
        for index_schema in table_schema.indexes.values():
            if self._is_supported_index_type(index_schema.index_type):
                self._open_index(table_schema, index_schema.column_name).clear()

        return table_schema

    def create_table_from_csv(
        self,
        table_name: str,
        columns: list[dict[str, Any]],
        csv_path: str | Path,
        delimiter: str = ",",
        has_header: bool = True,
        overwrite: bool = False,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Crea una tabla, carga registros desde CSV y alimenta los índices
        soportados declarados en el esquema.
        """
        csv_path = Path(csv_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"No existe el CSV: {csv_path}")

        table_schema = self.create_table(table_name, columns, overwrite=overwrite)
        rm = self._record_manager(table_schema)
        rm.reset_counters()

        indexes = self._open_supported_indexes(table_schema)
        for index in indexes.values():
            index.reset_counters()

        inserted = 0
        for record in self._iter_csv_records(csv_path, table_schema, delimiter, has_header):
            rid = rm.insert(record)
            for column_name, index in indexes.items():
                index_key = self._index_key_for_row(table_schema, column_name, record)
                index.add(index_key, rid)
            inserted += 1

            if limit is not None and inserted >= limit:
                break

        # Sequential File necesita reconstrucción física final para dejar main
        # ordenado y overflow vacío. B+ Tree no necesita rebuild final, pero su
        # método existe como no-op para mantener API común.
        for index in indexes.values():
            if hasattr(index, "rebuild"):
                index.rebuild()

        table_schema.row_count = inserted
        self.catalog.create_table(table_schema, overwrite=True)

        stats = self._combine_stats(rm.get_stats(), *[idx.get_stats() for idx in indexes.values()])
        return {
            "table": table_name,
            "rows_inserted": inserted,
            "data_file": table_schema.data_file,
            "catalog_file": str(self.catalog_path),
            "index_files": {name: table_schema.indexes[name].file_path for name in indexes},
            **stats,
        }

    def drop_table(self, table_name: str) -> dict[str, Any]:
        """
        Elimina una tabla completa del mini DBMS.

        Borra:
        - el archivo binario paginado de registros,
        - los archivos físicos de índices asociados,
        - la entrada del catálogo.

        Es la operación usada por DROP TABLE desde el parser SQL.
        """
        table_schema = self.catalog.get_table(table_name)
        data_file = Path(table_schema.data_file)
        rows_before = int(table_schema.row_count)

        self._delete_index_files(table_schema)

        if data_file.exists():
            data_file.unlink()

        self.catalog.drop_table(table_name, delete_files=False)

        return {
            "table": table_name,
            "operation": "drop_table",
            "rows_deleted": rows_before,
            "data_file_deleted": str(data_file),
            "disk_reads": 0,
            "disk_writes": 0,
            "disk_accesses": 0,
        }

    def rebuild_index(self, table_name: str, column_name: str) -> dict[str, Any]:
        """Reconstruye desde cero un índice soportado para una columna."""
        table_schema = self.catalog.get_table(table_name)
        if not table_schema.has_index(column_name):
            raise ValueError(f"La columna {column_name} no tiene índice")

        index_schema = table_schema.indexes[column_name]
        if not self._is_supported_index_type(index_schema.index_type):
            raise NotImplementedError(f"Índice no implementado todavía: {index_schema.index_type}")

        index = self._open_index(table_schema, column_name)
        index.clear()
        rm = self._record_manager(table_schema)
        rm.reset_counters()
        index.reset_counters()

        count = 0
        for rid, row in rm.scan():
            index_key = self._index_key_for_row(table_schema, column_name, row)
            index.add(index_key, rid)
            count += 1

        if hasattr(index, "rebuild"):
            index.rebuild()

        stats = self._combine_stats(rm.get_stats(), index.get_stats())
        return {
            "table": table_name,
            "column": column_name,
            "index_type": index_schema.index_type,
            "entries": count,
            **stats,
        }

    # ------------------------------------------------------------------
    # DML básico
    # ------------------------------------------------------------------

    def insert_record(self, table_name: str, record: dict[str, Any] | list[Any] | tuple[Any, ...]) -> RID:
        table_schema = self.catalog.get_table(table_name)
        normalized = Serializer.normalize_record(record, table_schema.record_schema)
        rm = self._record_manager(table_schema)
        rid = rm.insert(normalized)

        indexes = self._open_supported_indexes(table_schema)
        for column_name, index in indexes.items():
            index_key = self._index_key_for_row(table_schema, column_name, normalized)
            index.add(index_key, rid)

        table_schema.row_count += 1
        self.catalog.create_table(table_schema, overwrite=True)
        return rid

    def read_record(self, table_name: str, rid: RID | tuple[int, int] | list[int]) -> dict[str, Any] | None:
        table_schema = self.catalog.get_table(table_name)
        rm = self._record_manager(table_schema)

        if not isinstance(rid, RID):
            rid = RID(int(rid[0]), int(rid[1]))

        return rm.read(rid)

    def delete_by_key(self, table_name: str, column_name: str, key: Any) -> dict[str, Any]:
        """Elimina registros usando índice si existe; si no, escanea paginado."""
        table_schema = self.catalog.get_table(table_name)
        column = table_schema.get_column(column_name)
        key = Serializer.cast_value(key, column.type)

        rm = self._record_manager(table_schema)
        rm.reset_counters()

        deleted = 0
        if self._has_supported_index(table_schema, column_name):
            index = self._open_index(table_schema, column_name)
            index.reset_counters()
            rids = index.search(key)
            for rid in rids:
                if rm.delete(rid):
                    deleted += 1
                    index.remove(key, rid)
            stats = self._combine_stats(rm.get_stats(), index.get_stats())
        else:
            # Fallback paginado: escanea registros activos y elimina los que matchean.
            for rid, row in rm.scan():
                if row[column_name] == key and rm.delete(rid):
                    deleted += 1
            stats = rm.get_stats()

        if deleted > 0:
            table_schema.row_count = max(0, table_schema.row_count - deleted)
            self.catalog.create_table(table_schema, overwrite=True)

        return {"table": table_name, "column": column_name, "key": key, "rows_deleted": deleted, **stats}

    # ------------------------------------------------------------------
    # Consultas usando índice
    # ------------------------------------------------------------------

    def search_by_index(self, table_name: str, column_name: str, key: Any) -> dict[str, Any]:
        table_schema = self.catalog.get_table(table_name)
        column = table_schema.get_column(column_name)
        key = Serializer.cast_value(key, column.type)

        rm = self._record_manager(table_schema)
        rm.reset_counters()

        if self._has_supported_index(table_schema, column_name):
            index = self._open_index(table_schema, column_name)
            index.reset_counters()
            rids = index.search(key)
            rows = [row for row in (rm.read(rid) for rid in rids) if row is not None]
            stats = self._combine_stats(rm.get_stats(), index.get_stats())
            used_index = table_schema.indexes[column_name].index_type
        else:
            rows = []
            for _, row in rm.scan():
                if row[column_name] == key:
                    rows.append(row)
            stats = rm.get_stats()
            used_index = "none"

        return {
            "table": table_name,
            "column": column_name,
            "operation": "search",
            "key": key,
            "used_index": used_index,
            "rows": rows,
            "row_count": len(rows),
            **stats,
        }

    def range_search_by_index(self, table_name: str, column_name: str, begin_key: Any, end_key: Any) -> dict[str, Any]:
        table_schema = self.catalog.get_table(table_name)
        column = table_schema.get_column(column_name)
        begin_key = Serializer.cast_value(begin_key, column.type)
        end_key = Serializer.cast_value(end_key, column.type)

        if begin_key > end_key:
            begin_key, end_key = end_key, begin_key

        rm = self._record_manager(table_schema)
        rm.reset_counters()

        index_type = table_schema.indexes[column_name].index_type if table_schema.has_index(column_name) else "none"

        # Extendible Hashing no soporta búsquedas por rango; en ese caso el
        # ejecutor hace fallback a escaneo paginado de la tabla.
        if self._has_supported_index(table_schema, column_name) and index_type != "extendible_hash":
            index = self._open_index(table_schema, column_name)
            index.reset_counters()
            rids = index.range_search(begin_key, end_key)
            rows = [row for row in (rm.read(rid) for rid in rids) if row is not None]
            stats = self._combine_stats(rm.get_stats(), index.get_stats())
            used_index = table_schema.indexes[column_name].index_type
        else:
            rows = []
            for _, row in rm.scan():
                if begin_key <= row[column_name] <= end_key:
                    rows.append(row)
            stats = rm.get_stats()
            used_index = "none" if index_type != "extendible_hash" else "none_hash_range_unsupported"

        return {
            "table": table_name,
            "column": column_name,
            "operation": "range_search",
            "begin_key": begin_key,
            "end_key": end_key,
            "used_index": used_index,
            "rows": rows,
            "row_count": len(rows),
            **stats,
        }

    def rtree_range_search(
        self,
        table_name: str,
        column_name: str,
        point: tuple[float, float] | list[float],
        radius: float,
    ) -> dict[str, Any]:
        """Ejecuta consulta espacial R-Tree: POINT(x,y), RADIUS r."""
        table_schema = self.catalog.get_table(table_name)
        if not table_schema.has_index(column_name):
            raise ValueError(f"La columna {column_name} no tiene índice R-Tree")

        index_schema = table_schema.indexes[column_name]
        if index_schema.index_type != "rtree":
            raise ValueError(f"La columna {column_name} no usa R-Tree, usa {index_schema.index_type}")

        rm = self._record_manager(table_schema)
        rm.reset_counters()
        index = self._rtree_index(table_schema, column_name)
        index.reset_counters()

        center = (float(point[0]), float(point[1]))
        radius = float(radius)
        rids = index.range_search(center, radius)
        rows = [row for row in (rm.read(rid) for rid in rids) if row is not None]
        stats = self._combine_stats(rm.get_stats(), index.get_stats())

        spatial_points = []
        for row in rows:
            try:
                x, y = self._extract_spatial_point(table_schema, column_name, row)
                spatial_points.append({"x": x, "y": y, "match": True})
            except Exception:
                pass

        return {
            "table": table_name,
            "column": column_name,
            "operation": "rtree_range_search",
            "query_point": {"x": center[0], "y": center[1]},
            "radius": radius,
            "used_index": "rtree",
            "rows": rows,
            "row_count": len(rows),
            "spatial": {
                "query_point": {"x": center[0], "y": center[1]},
                "radius": radius,
                "points": spatial_points,
            },
            **stats,
        }

    def rtree_knn(
        self,
        table_name: str,
        column_name: str,
        point: tuple[float, float] | list[float],
        k: int,
    ) -> dict[str, Any]:
        """Ejecuta consulta espacial R-Tree: POINT(x,y), K k."""
        table_schema = self.catalog.get_table(table_name)
        if not table_schema.has_index(column_name):
            raise ValueError(f"La columna {column_name} no tiene índice R-Tree")

        index_schema = table_schema.indexes[column_name]
        if index_schema.index_type != "rtree":
            raise ValueError(f"La columna {column_name} no usa R-Tree, usa {index_schema.index_type}")

        rm = self._record_manager(table_schema)
        rm.reset_counters()
        index = self._rtree_index(table_schema, column_name)
        index.reset_counters()

        center = (float(point[0]), float(point[1]))
        k = int(k)
        rids = index.knn_search(center, k)
        rows = [row for row in (rm.read(rid) for rid in rids) if row is not None]
        stats = self._combine_stats(rm.get_stats(), index.get_stats())

        spatial_points = []
        for row in rows:
            try:
                x, y = self._extract_spatial_point(table_schema, column_name, row)
                spatial_points.append({"x": x, "y": y, "match": True})
            except Exception:
                pass

        return {
            "table": table_name,
            "column": column_name,
            "operation": "rtree_knn",
            "query_point": {"x": center[0], "y": center[1]},
            "k": k,
            "used_index": "rtree",
            "rows": rows,
            "row_count": len(rows),
            "spatial": {
                "query_point": {"x": center[0], "y": center[1]},
                "k": k,
                "points": spatial_points,
            },
            **stats,
        }

    # ------------------------------------------------------------------
    # Utilidades de tabla
    # ------------------------------------------------------------------

    def select_all(
        self,
        table_name: str,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        """
        Ejecuta SELECT * FROM table mediante escaneo paginado.

        Para respetar la restricción del proyecto de no cargar la tabla completa
        en memoria, este método devuelve como máximo `limit` registros. Si la
        consulta SQL no especifica TOP, se aplica DEFAULT_SELECT_LIMIT.
        """
        table_schema = self.catalog.get_table(table_name)
        effective_limit = self.DEFAULT_SELECT_LIMIT if limit is None else int(limit)
        offset = int(offset)

        if effective_limit <= 0:
            raise ValueError("limit debe ser un entero positivo")
        if offset < 0:
            raise ValueError("offset no puede ser negativo")

        rm = self._record_manager(table_schema)
        rm.reset_counters()

        rows: list[dict[str, Any]] = []
        seen = 0
        for _, row in rm.scan():
            if seen < offset:
                seen += 1
                continue

            rows.append(row)
            seen += 1

            if len(rows) >= effective_limit:
                break

        stats = rm.get_stats()
        total_rows = int(table_schema.row_count)
        next_offset = offset + len(rows)
        has_more = next_offset < total_rows

        return {
            "table": table_name,
            "operation": "select_all",
            "used_index": "none",
            "rows": rows,
            "row_count": len(rows),
            "returned_rows": len(rows),
            "total_rows": total_rows,
            "limit": effective_limit,
            "offset": offset,
            "next_offset": next_offset if has_more else None,
            "has_more": has_more,
            **stats,
        }

    def iter_table_records(self, table_name: str):
        """Itera registros de una tabla sin materializarlos todos en memoria."""
        table_schema = self.catalog.get_table(table_name)
        rm = self._record_manager(table_schema)
        for _, row in rm.scan():
            yield row

    def scan_table(
        self,
        table_name: str,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Devuelve una página de registros para compatibilidad con pruebas.

        No carga toda la tabla por defecto: si no se especifica limit, usa
        DEFAULT_SELECT_LIMIT. Para procesamiento completo, usar iter_table_records.
        """
        payload = self.select_all(table_name, limit=limit, offset=offset)
        return payload["rows"]

    def scan_table_with_rids(
        self,
        table_name: str,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[tuple[RID, dict[str, Any]]]:
        """Devuelve una página de (RID, row), sin materializar toda la tabla."""
        table_schema = self.catalog.get_table(table_name)
        effective_limit = self.DEFAULT_SELECT_LIMIT if limit is None else int(limit)
        rm = self._record_manager(table_schema)

        rows: list[tuple[RID, dict[str, Any]]] = []
        seen = 0
        for rid, row in rm.scan():
            if seen < offset:
                seen += 1
                continue

            rows.append((rid, row))
            seen += 1

            if len(rows) >= effective_limit:
                break

        return rows

    def list_tables(self) -> list[str]:
        return self.catalog.list_tables()

    def describe_table(self, table_name: str) -> dict[str, Any]:
        return self.catalog.get_table(table_name).to_dict()

    def get_table_stats(self, table_name: str) -> dict[str, int]:
        table_schema = self.catalog.get_table(table_name)
        rm = self._record_manager(table_schema)
        return rm.get_stats()

    def reset_table_stats(self, table_name: str) -> None:
        table_schema = self.catalog.get_table(table_name)
        rm = self._record_manager(table_schema)
        rm.reset_counters()

    def clear_database(self) -> None:
        """Elimina data/catalog/indexes dentro del base_dir. Útil para pruebas."""
        if self.base_dir.exists():
            shutil.rmtree(self.base_dir)

        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.indexes_dir.mkdir(parents=True, exist_ok=True)
        self.catalog = Catalog(self.catalog_path)

    # ------------------------------------------------------------------
    # Métodos internos
    # ------------------------------------------------------------------

    def _record_manager(self, table_schema: TableSchema) -> RecordManager:
        return RecordManager(table_schema.data_file, table_schema.record_schema)

    def _iter_csv_records(
        self,
        csv_path: Path,
        table_schema: TableSchema,
        delimiter: str,
        has_header: bool,
    ) -> Iterable[dict[str, Any]]:
        column_names = [col.name for col in table_schema.columns]
        record_schema = table_schema.record_schema

        with open(csv_path, "r", encoding="utf-8-sig", newline="") as file:
            if has_header:
                reader = csv.DictReader(file, delimiter=delimiter)
                if reader.fieldnames is None:
                    return

                header_map = self._build_csv_header_map(reader.fieldnames)
                missing = [name for name in column_names if name not in header_map]
                if missing:
                    available = list(header_map.keys())
                    raise ValueError(
                        "Columnas faltantes en CSV: "
                        f"{missing}. Columnas disponibles normalizadas: {available}"
                    )

                for row in reader:
                    # Permite que el esquema use identificadores SQL limpios
                    # como product_id, aunque el header real sea "Product ID".
                    raw_record = {name: row.get(header_map[name], "") for name in column_names}
                    yield Serializer.normalize_record(raw_record, record_schema)
            else:
                reader = csv.reader(file, delimiter=delimiter)
                for values in reader:
                    yield Serializer.normalize_record(values, record_schema)

    @staticmethod
    def _sanitize_csv_identifier(value: str, fallback: str = "col") -> str:
        """Replica la normalización usada por el frontend para headers CSV."""
        text = re.sub(r"[^0-9A-Za-z_]+", "_", str(value).strip().lower())
        text = re.sub(r"_+", "_", text).strip("_")
        if not text:
            text = fallback
        if text[0].isdigit():
            text = f"{fallback}_{text}"
        return text

    @classmethod
    def _build_csv_header_map(cls, fieldnames: list[str]) -> dict[str, str]:
        """
        Construye un mapa entre los nombres limpios usados en SQL y los
        encabezados reales del CSV.

        Ejemplos:
            "Product ID"             -> product_id
            "Air temperature [K]"    -> air_temperature_k
            "Rotational speed [rpm]" -> rotational_speed_rpm
            "Torque [Nm]"            -> torque_nm
        """
        mapping: dict[str, str] = {}
        seen: dict[str, int] = {}

        for raw_name in fieldnames:
            original = raw_name or ""
            sanitized_base = cls._sanitize_csv_identifier(original)
            count = seen.get(sanitized_base, 0)
            seen[sanitized_base] = count + 1
            sanitized = sanitized_base if count == 0 else f"{sanitized_base}_{count + 1}"

            mapping[sanitized] = original
            mapping[original] = original

        return mapping

    def _index_key_for_row(self, table_schema: TableSchema, column_name: str, row: dict[str, Any]) -> Any:
        """Devuelve la clave física que debe insertarse en el índice."""
        index_schema = table_schema.indexes[column_name]
        if index_schema.index_type == "rtree":
            return self._extract_spatial_point(table_schema, column_name, row)
        return row[column_name]

    def _extract_spatial_point(
        self,
        table_schema: TableSchema,
        column_name: str,
        row: dict[str, Any],
    ) -> tuple[float, float]:
        """
        Obtiene la clave compuesta (longitud, latitud) para R-Tree.

        Formas soportadas:
        1. Columna indexada con valor tipo "x,y" o "POINT(x,y)".
        2. Índice declarado sobre longitude/lon/lng/x y columna hermana
           latitude/lat/y.
        3. Índice declarado sobre location/point/coordinates y columnas
           longitude+latitude o lon+lat o x+y.
        """
        def parse_point_value(value: Any) -> tuple[float, float] | None:
            if isinstance(value, dict):
                if "x" in value and "y" in value:
                    return float(value["x"]), float(value["y"])
                if "longitude" in value and "latitude" in value:
                    return float(value["longitude"]), float(value["latitude"])
                if "lon" in value and "lat" in value:
                    return float(value["lon"]), float(value["lat"])
            if isinstance(value, (list, tuple)) and len(value) == 2:
                return float(value[0]), float(value[1])
            if isinstance(value, str):
                cleaned = value.strip().replace("POINT", "").replace("point", "")
                cleaned = cleaned.strip("()[]{} ")
                parts = [part.strip() for part in cleaned.split(",")]
                if len(parts) == 2:
                    try:
                        return float(parts[0]), float(parts[1])
                    except ValueError:
                        return None
            return None

        if column_name in row:
            parsed = parse_point_value(row[column_name])
            if parsed is not None:
                return parsed

        lower_to_name = {col.name.lower(): col.name for col in table_schema.columns}
        col_lower = column_name.lower()

        y_candidates = ["latitude", "lat", "y"]
        if col_lower in {"longitude", "lon", "lng", "x"}:
            for candidate in y_candidates:
                if candidate in lower_to_name:
                    return float(row[column_name]), float(row[lower_to_name[candidate]])

        pair_candidates = [
            ("longitude", "latitude"),
            ("lon", "lat"),
            ("lng", "lat"),
            ("x", "y"),
        ]
        for x_name, y_name in pair_candidates:
            if x_name in lower_to_name and y_name in lower_to_name:
                return float(row[lower_to_name[x_name]]), float(row[lower_to_name[y_name]])

        raise ValueError(
            f"No se pudo construir punto R-Tree para columna {column_name}. "
            "Usa una columna 'x,y' o columnas longitude/latitude, lon/lat o x/y."
        )

    def _rewrite_index_paths(self, table_schema: TableSchema) -> None:
        """Ubica los archivos de índices dentro de data/db/indexes."""
        for column_name, index_schema in table_schema.indexes.items():
            index_schema.file_path = str(
                self.indexes_dir / f"{table_schema.name}_{column_name}_{index_schema.index_type}.idx"
            )

    def _open_supported_indexes(self, table_schema: TableSchema) -> dict[str, BaseIndex]:
        indexes: dict[str, BaseIndex] = {}
        for column_name, index_schema in table_schema.indexes.items():
            if self._is_supported_index_type(index_schema.index_type):
                indexes[column_name] = self._open_index(table_schema, column_name)
        return indexes

    def _open_index(self, table_schema: TableSchema, column_name: str) -> BaseIndex:
        if column_name not in table_schema.indexes:
            raise ValueError(f"La columna {column_name} no tiene índice")

        index_schema: IndexSchema = table_schema.indexes[column_name]
        column = table_schema.get_column(column_name)

        if index_schema.index_type == "sequential":
            threshold = int(index_schema.options.get("overflow_threshold", SequentialFile.DEFAULT_OVERFLOW_THRESHOLD))
            return SequentialFile(index_schema.file_path, key_type=column.type, overflow_threshold=threshold)

        if index_schema.index_type == "bplus":
            max_keys = int(index_schema.options.get("max_keys", BPlusTree.DEFAULT_MAX_KEYS))
            return BPlusTree(index_schema.file_path, key_type=column.type, max_keys=max_keys)

        if index_schema.index_type == "extendible_hash":
            bucket_size = int(index_schema.options.get("bucket_size", ExtendibleHash.DEFAULT_BUCKET_SIZE))
            max_depth = int(index_schema.options.get("max_global_depth", ExtendibleHash.DEFAULT_MAX_GLOBAL_DEPTH))
            return ExtendibleHash(
                index_schema.file_path,
                key_type=column.type,
                bucket_size=bucket_size,
                max_global_depth=max_depth,
            )

        if index_schema.index_type == "rtree":
            max_entries = int(index_schema.options.get("max_entries", RTree.DEFAULT_MAX_ENTRIES))
            min_entries = index_schema.options.get("min_entries")
            min_entries = int(min_entries) if min_entries is not None else None
            return RTree(
                index_schema.file_path,
                key_type="point",
                max_entries=max_entries,
                min_entries=min_entries,
            )

        raise NotImplementedError(f"Índice no implementado todavía: {index_schema.index_type}")

    def _sequential_index(self, table_schema: TableSchema, column_name: str) -> SequentialFile:
        index = self._open_index(table_schema, column_name)
        if not isinstance(index, SequentialFile):
            raise NotImplementedError("La columna no usa Sequential File")
        return index

    def _bplus_index(self, table_schema: TableSchema, column_name: str) -> BPlusTree:
        index = self._open_index(table_schema, column_name)
        if not isinstance(index, BPlusTree):
            raise NotImplementedError("La columna no usa B+ Tree")
        return index

    def _extendible_hash_index(self, table_schema: TableSchema, column_name: str) -> ExtendibleHash:
        index = self._open_index(table_schema, column_name)
        if not isinstance(index, ExtendibleHash):
            raise NotImplementedError("La columna no usa Extendible Hashing")
        return index

    def _rtree_index(self, table_schema: TableSchema, column_name: str) -> RTree:
        index = self._open_index(table_schema, column_name)
        if not isinstance(index, RTree):
            raise NotImplementedError("La columna no usa R-Tree")
        return index

    def _has_supported_index(self, table_schema: TableSchema, column_name: str) -> bool:
        return table_schema.has_index(column_name) and self._is_supported_index_type(
            table_schema.indexes[column_name].index_type
        )

    @staticmethod
    def _is_supported_index_type(index_type: str) -> bool:
        return index_type in {"sequential", "bplus", "extendible_hash", "rtree"}

    def _delete_index_files(self, table_schema: TableSchema) -> None:
        for index_schema in table_schema.indexes.values():
            base = Path(index_schema.file_path)
            candidates = [
                base,
                Path(f"{base}.main"),
                Path(f"{base}.overflow"),
                Path(f"{base}.meta.json"),
            ]
            for path in candidates:
                if path.exists():
                    path.unlink()

    @staticmethod
    def _combine_stats(*stats_list: dict[str, int]) -> dict[str, int]:
        reads = sum(stats.get("disk_reads", 0) for stats in stats_list)
        writes = sum(stats.get("disk_writes", 0) for stats in stats_list)
        return {
            "disk_reads": reads,
            "disk_writes": writes,
            "disk_accesses": reads + writes,
        }

    @staticmethod
    def _validate_identifier(value: str, label: str) -> None:
        if not value:
            raise ValueError(f"El nombre de {label} no puede estar vacío")

        if not value.replace("_", "").isalnum():
            raise ValueError(f"El nombre de {label} solo debe usar letras, números o _")
