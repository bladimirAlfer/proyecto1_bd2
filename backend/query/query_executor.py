from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Any

from backend.parser import parse_sql
from backend.parser.ast_nodes import (
    CreateTableCommand,
    DeleteCommand,
    DropTableCommand,
    EngineCall,
    InsertCommand,
    SelectAllCommand,
    SelectCommand,
)
from backend.storage.rid import RID
from backend.storage.serializer import Serializer

from .query_result import QueryResult, serializable_row, serializable_value


class QueryExecutionError(RuntimeError):
    pass


class QueryExecutor:
    """
    Ejecuta el AST generado por SQLParser contra DBEngine.

    Esta clase es la pieza que une:
    SQLParser -> EngineCall -> DBEngine -> índices/RecordManager/PageManager.
    """

    def __init__(self, engine: Any, *, raise_errors: bool = True) -> None:
        self.engine = engine
        self.raise_errors = raise_errors

    def execute(self, sql_or_command: str | Any) -> QueryResult:
        start = perf_counter()
        command_type = "unknown"
        try:
            command = parse_sql(sql_or_command) if isinstance(sql_or_command, str) else sql_or_command
            command_type = getattr(command, "command_type", command.__class__.__name__)
            result_payload = self._execute_command(command)
            time_ms = (perf_counter() - start) * 1000
            return self._build_query_result(command, result_payload, time_ms)
        except Exception as exc:
            time_ms = (perf_counter() - start) * 1000
            if self.raise_errors:
                raise
            return QueryResult.error_result(command_type, exc, time_ms)

    def execute_script(self, sql_script: str) -> list[QueryResult]:
        """
        Ejecuta varias sentencias separadas por ';'.

        Es intencionalmente simple porque el subconjunto SQL del proyecto no
        incluye procedimientos ni strings multilínea con ';' internos.
        """
        statements = [part.strip() for part in sql_script.split(";") if part.strip()]
        return [self.execute(statement + ";") for statement in statements]

    # ------------------------------------------------------------------
    # Ejecución por tipo de comando
    # ------------------------------------------------------------------

    def _execute_command(self, command: Any) -> Any:
        if isinstance(command, InsertCommand):
            return self._execute_insert(command)

        call: EngineCall = command.to_engine_call()

        if call.method in {"rtree_range_search", "rtree_knn"} and not hasattr(self.engine, call.method):
            raise NotImplementedError(
                f"{call.method} se implementará en la Fase 8 junto con el índice R-Tree"
            )

        if not hasattr(self.engine, call.method):
            raise QueryExecutionError(f"DBEngine no tiene el método requerido: {call.method}")

        method = getattr(self.engine, call.method)
        return method(*call.args, **call.kwargs)

    def _execute_insert(self, command: InsertCommand) -> dict[str, Any]:
        """
        Inserción con estadísticas de I/O.

        DBEngine.insert_record devuelve solo RID para mantener compatibilidad con
        fases anteriores. Para la UI necesitamos reads/writes/time, por eso aquí
        hacemos la inserción usando los mismos componentes internos del motor y
        devolvemos un payload completo.
        """
        table_schema = self.engine.catalog.get_table(command.table_name)
        normalized = Serializer.normalize_record(command.values, table_schema.record_schema)

        rm = self.engine._record_manager(table_schema)
        rm.reset_counters()
        rid = rm.insert(normalized)

        indexes = self.engine._open_supported_indexes(table_schema)
        for index in indexes.values():
            index.reset_counters()

        for column_name, index in indexes.items():
            index.add(normalized[column_name], rid)

        table_schema.row_count += 1
        self.engine.catalog.create_table(table_schema, overwrite=True)

        stats = self.engine._combine_stats(rm.get_stats(), *[idx.get_stats() for idx in indexes.values()])
        return {
            "table": command.table_name,
            "operation": "insert",
            "rid": rid,
            "record": normalized,
            "row_count": 1,
            **stats,
        }

    # ------------------------------------------------------------------
    # Normalización hacia QueryResult
    # ------------------------------------------------------------------

    def _build_query_result(self, command: Any, payload: Any, time_ms: float) -> QueryResult:
        if isinstance(command, (SelectCommand, SelectAllCommand)):
            return self._result_for_select(command, payload, time_ms)
        if isinstance(command, CreateTableCommand):
            return self._result_for_create(command, payload, time_ms)
        if isinstance(command, InsertCommand):
            return self._result_for_insert(command, payload, time_ms)
        if isinstance(command, DeleteCommand):
            return self._result_for_delete(command, payload, time_ms)
        if isinstance(command, DropTableCommand):
            return self._result_for_drop_table(command, payload, time_ms)

        return QueryResult(
            success=True,
            command_type=getattr(command, "command_type", "unknown"),
            message="Consulta ejecutada correctamente",
            raw=payload,
            time_ms=time_ms,
        )

    def _result_for_select(self, command: SelectCommand | SelectAllCommand, payload: dict[str, Any], time_ms: float) -> QueryResult:
        rows = [serializable_row(row) for row in payload.get("rows", [])]
        columns = self._table_columns(command.table_name, rows)
        used_index = payload.get("used_index")
        row_count = int(payload.get("row_count", len(rows)))

        total_rows = payload.get("total_rows")
        limit = payload.get("limit")
        has_more = bool(payload.get("has_more", False))

        if total_rows is not None and limit is not None:
            message = f"SELECT ejecutado: {row_count} fila(s) mostrada(s) de {total_rows}"
            if has_more:
                message += f". Resultado paginado con límite {limit}"
        else:
            message = f"SELECT ejecutado: {row_count} fila(s) encontrada(s)"

        if used_index and used_index != "none":
            message += f" usando índice {used_index}"
        elif used_index == "none":
            message += " usando escaneo paginado"

        return QueryResult(
            success=True,
            command_type="select",
            message=message,
            rows=rows,
            columns=columns,
            row_count=row_count,
            stats=payload,
            time_ms=time_ms,
            used_index=used_index,
            raw=payload,
        )

    def _result_for_create(self, command: CreateTableCommand, payload: Any, time_ms: float) -> QueryResult:
        stats = payload if isinstance(payload, dict) else {}
        if isinstance(payload, dict):
            rows_inserted = int(payload.get("rows_inserted", 0))
            message = f"Tabla '{command.table_name}' creada"
            if command.from_file is not None:
                message += f" y cargada desde CSV con {rows_inserted} fila(s)"
            rows = [serializable_row({k: serializable_value(v) for k, v in payload.items() if k not in {"disk_reads", "disk_writes", "disk_accesses"}})]
            row_count = rows_inserted
        else:
            message = f"Tabla '{command.table_name}' creada"
            table_dict = payload.to_dict() if hasattr(payload, "to_dict") else {"table": command.table_name}
            rows = [serializable_row(table_dict)]
            row_count = 0

        return QueryResult(
            success=True,
            command_type="create_table",
            message=message,
            rows=rows,
            columns=list(rows[0].keys()) if rows else [],
            row_count=row_count,
            stats=stats,
            time_ms=time_ms,
            raw=payload,
        )

    def _result_for_insert(self, command: InsertCommand, payload: dict[str, Any], time_ms: float) -> QueryResult:
        rid = payload.get("rid")
        row = {
            "table": command.table_name,
            "operation": "insert",
            "page_id": rid.page_id if isinstance(rid, RID) else None,
            "slot_id": rid.slot_id if isinstance(rid, RID) else None,
        }
        return QueryResult(
            success=True,
            command_type="insert",
            message=f"INSERT ejecutado: registro insertado en {rid}",
            rows=[row],
            columns=list(row.keys()),
            row_count=1,
            stats=payload,
            time_ms=time_ms,
            raw=payload,
        )

    def _result_for_delete(self, command: DeleteCommand, payload: dict[str, Any], time_ms: float) -> QueryResult:
        deleted = int(payload.get("rows_deleted", 0))
        row = {
            "table": command.table_name,
            "column": command.column,
            "key": command.value,
            "rows_deleted": deleted,
        }
        return QueryResult(
            success=True,
            command_type="delete",
            message=f"DELETE ejecutado: {deleted} fila(s) eliminada(s)",
            rows=[row],
            columns=list(row.keys()),
            row_count=deleted,
            stats=payload,
            time_ms=time_ms,
            raw=payload,
        )

    def _result_for_drop_table(self, command: DropTableCommand, payload: dict[str, Any], time_ms: float) -> QueryResult:
        row = {
            "table": command.table_name,
            "operation": "drop_table",
            "rows_deleted": int(payload.get("rows_deleted", 0)),
            "data_file_deleted": payload.get("data_file_deleted"),
        }
        return QueryResult(
            success=True,
            command_type="drop_table",
            message=f"DROP TABLE ejecutado: tabla '{command.table_name}' eliminada",
            rows=[row],
            columns=list(row.keys()),
            row_count=1,
            stats=payload,
            time_ms=time_ms,
            raw=payload,
        )

    def _table_columns(self, table_name: str, rows: list[dict[str, Any]]) -> list[str]:
        try:
            table_schema = self.engine.catalog.get_table(table_name)
            return [column.name for column in table_schema.columns]
        except Exception:
            return list(rows[0].keys()) if rows else []
