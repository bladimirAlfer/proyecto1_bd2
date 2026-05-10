from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .operation_log import Operation


@dataclass
class Transaction:
    """Transacción simple para el simulador de concurrencia."""

    tx_id: str
    operations: list[Operation] = field(default_factory=list)
    status: str = "ACTIVE"

    def read(self, resource: str, page_id: int | None = None, detail: str = "") -> "Transaction":
        self.operations.append(Operation(self.tx_id, "READ", resource, page_id, detail))
        return self

    def write(self, resource: str, page_id: int | None = None, detail: str = "") -> "Transaction":
        self.operations.append(Operation(self.tx_id, "WRITE", resource, page_id, detail))
        return self

    def insert(self, resource: str, page_id: int | None = None, detail: str = "") -> "Transaction":
        self.operations.append(Operation(self.tx_id, "INSERT", resource, page_id, detail))
        return self

    def delete(self, resource: str, page_id: int | None = None, detail: str = "") -> "Transaction":
        self.operations.append(Operation(self.tx_id, "DELETE", resource, page_id, detail))
        return self

    def commit(self) -> "Transaction":
        self.operations.append(Operation(self.tx_id, "COMMIT", "transaction", None, "commit"))
        self.status = "COMMITTED"
        return self

    def abort(self, reason: str = "abort") -> "Transaction":
        self.operations.append(Operation(self.tx_id, "ABORT", "transaction", None, reason))
        self.status = "ABORTED"
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_id": self.tx_id,
            "status": self.status,
            "operations": [operation.to_dict() for operation in self.operations],
        }
