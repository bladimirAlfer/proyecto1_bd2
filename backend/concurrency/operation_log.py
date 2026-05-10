from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Operation:
    """
    Operación lógica de una transacción sobre una página o recurso.

    El simulador básico del proyecto no implementa locks ni deadlock detection.
    Solo registra el orden de ejecución y detecta conflictos simples cuando dos
    transacciones diferentes acceden al mismo recurso y al menos una escribe.
    """

    tx_id: str
    action: str
    resource: str
    page_id: int | None = None
    detail: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().strftime("%H:%M:%S.%f")[:-3])

    def normalized_action(self) -> str:
        return self.action.strip().upper()

    def resource_key(self) -> str:
        if self.page_id is None:
            return self.resource
        return f"{self.resource}:page:{self.page_id}"

    def is_write(self) -> bool:
        return self.normalized_action() in {"WRITE", "INSERT", "DELETE", "UPDATE", "REMOVE"}

    def is_read(self) -> bool:
        return self.normalized_action() in {"READ", "SEARCH", "SCAN"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_id": self.tx_id,
            "action": self.normalized_action(),
            "resource": self.resource,
            "page_id": self.page_id,
            "resource_key": self.resource_key(),
            "detail": self.detail,
            "timestamp": self.timestamp,
        }


@dataclass(frozen=True)
class Conflict:
    """Conflicto READ/WRITE o WRITE/WRITE detectado por el log."""

    first: Operation
    second: Operation
    conflict_type: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.conflict_type,
            "reason": self.reason,
            "resource_key": self.first.resource_key(),
            "first": self.first.to_dict(),
            "second": self.second.to_dict(),
        }


class OperationLog:
    """
    Log simplificado para el simulador de concurrencia.

    Registra operaciones en orden y detecta conflictos con reglas básicas:
    - Mismo recurso/página.
    - Transacciones distintas.
    - Al menos una operación es escritura.
    """

    def __init__(self) -> None:
        self._operations: list[Operation] = []
        self._conflicts: list[Conflict] = []

    def append(self, operation: Operation) -> list[Conflict]:
        new_conflicts = self._detect_conflicts_for(operation)
        self._operations.append(operation)
        self._conflicts.extend(new_conflicts)
        return new_conflicts

    def add(
        self,
        tx_id: str,
        action: str,
        resource: str,
        page_id: int | None = None,
        detail: str = "",
    ) -> list[Conflict]:
        return self.append(Operation(tx_id=tx_id, action=action, resource=resource, page_id=page_id, detail=detail))

    def clear(self) -> None:
        self._operations.clear()
        self._conflicts.clear()

    @property
    def operations(self) -> list[Operation]:
        return list(self._operations)

    @property
    def conflicts(self) -> list[Conflict]:
        return list(self._conflicts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operations": [op.to_dict() for op in self._operations],
            "conflicts": [conflict.to_dict() for conflict in self._conflicts],
            "operation_count": len(self._operations),
            "conflict_count": len(self._conflicts),
        }

    def _detect_conflicts_for(self, operation: Operation) -> list[Conflict]:
        conflicts: list[Conflict] = []
        if not (operation.is_read() or operation.is_write()):
            return conflicts

        for previous in self._operations:
            if previous.tx_id == operation.tx_id:
                continue
            if previous.resource_key() != operation.resource_key():
                continue
            if not (previous.is_write() or operation.is_write()):
                continue

            conflict_type = self._classify(previous, operation)
            reason = (
                f"{previous.tx_id} {previous.normalized_action()} y "
                f"{operation.tx_id} {operation.normalized_action()} acceden al mismo recurso "
                f"{operation.resource_key()}"
            )
            conflicts.append(Conflict(previous, operation, conflict_type, reason))
        return conflicts

    @staticmethod
    def _classify(first: Operation, second: Operation) -> str:
        if first.is_write() and second.is_write():
            return "WRITE_WRITE"
        if first.is_write() and second.is_read():
            return "WRITE_READ"
        if first.is_read() and second.is_write():
            return "READ_WRITE"
        return "READ_READ"
