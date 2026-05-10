from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from backend.storage.rid import RID


@dataclass
class QueryResult:
    """
    Resultado uniforme de una sentencia SQL ejecutada por QueryExecutor.

    La UI y los benchmarks pueden consumir este objeto sin conocer si la
    sentencia fue DDL, DML o SELECT. Siempre expone:
    - rows: filas para mostrar en tabla.
    - stats: lecturas/escrituras/accesos a disco.
    - time_ms: tiempo de pared de la consulta.
    """

    success: bool
    command_type: str
    message: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    row_count: int = 0
    stats: dict[str, int] = field(default_factory=dict)
    time_ms: float = 0.0
    used_index: str | None = None
    raw: Any | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        self.stats = _normalize_stats(self.stats)
        if self.row_count == 0 and self.rows:
            self.row_count = len(self.rows)
        if not self.columns and self.rows:
            self.columns = list(self.rows[0].keys())

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "command_type": self.command_type,
            "message": self.message,
            "columns": self.columns,
            "rows": self.rows,
            "row_count": self.row_count,
            "disk_reads": self.stats["disk_reads"],
            "disk_writes": self.stats["disk_writes"],
            "disk_accesses": self.stats["disk_accesses"],
            "time_ms": self.time_ms,
            "used_index": self.used_index,
            "error": self.error,
        }

    @classmethod
    def error_result(cls, command_type: str, error: Exception, time_ms: float = 0.0) -> "QueryResult":
        return cls(
            success=False,
            command_type=command_type,
            message=str(error),
            error=str(error),
            time_ms=time_ms,
        )


def serializable_value(value: Any) -> Any:
    if isinstance(value, RID):
        return {"page_id": value.page_id, "slot_id": value.slot_id}
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()
    return value


def serializable_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: serializable_value(value) for key, value in row.items()}


def _normalize_stats(stats: dict[str, Any] | None) -> dict[str, Any]:
    """
    Normaliza las métricas principales de I/O sin perder metadatos extra.

    Algunas consultas, como SELECT TOP, agregan información útil para la UI
    (`limit`, `total_rows`, `has_more`). Antes se descartaba todo salvo
    disk_reads/disk_writes/disk_accesses; ahora se preserva.
    """
    normalized = dict(stats or {})
    reads = int(normalized.get("disk_reads", 0))
    writes = int(normalized.get("disk_writes", 0))
    accesses = int(normalized.get("disk_accesses", reads + writes))

    normalized["disk_reads"] = reads
    normalized["disk_writes"] = writes
    normalized["disk_accesses"] = accesses
    return normalized
