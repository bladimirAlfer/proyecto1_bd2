from __future__ import annotations

from itertools import zip_longest
from typing import Any, Iterable

from .operation_log import Operation, OperationLog
from .transaction import Transaction


class ConcurrencySimulator:
    """
    Simulador básico de acceso concurrente.

    Cumple el componente obligatorio del proyecto:
    - Ejecuta al menos dos transacciones simultáneas/intercaladas.
    - Registra un log simplificado del orden de operaciones.
    - Identifica conflictos cuando hay accesos a la misma página/recurso.

    No implementa locks, timestamps ni detección de deadlocks; eso corresponde
    al adicional opcional del enunciado.
    """

    def __init__(self) -> None:
        self.log = OperationLog()

    def run_interleaved(self, transactions: Iterable[Transaction]) -> dict[str, Any]:
        """Ejecuta operaciones de varias transacciones en round-robin."""
        self.log.clear()
        txs = list(transactions)
        for group in zip_longest(*(tx.operations for tx in txs)):
            for operation in group:
                if operation is not None:
                    self.log.append(operation)
        return self.summary(txs)

    def run_schedule(self, operations: Iterable[Operation]) -> dict[str, Any]:
        """Ejecuta un schedule explícito de operaciones."""
        self.log.clear()
        for operation in operations:
            self.log.append(operation)
        return self.summary([])

    def demo_same_page_conflict(self) -> dict[str, Any]:
        """Caso demostrativo mínimo con conflicto READ/WRITE sobre la misma página."""
        t1 = Transaction("T1")
        t1.read("employee.tbl", page_id=0, detail="search id=10")
        t1.write("employee.tbl", page_id=0, detail="update salary")
        t1.commit()

        t2 = Transaction("T2")
        t2.read("employee.tbl", page_id=0, detail="search id=10")
        t2.delete("employee.tbl", page_id=0, detail="delete id=10")
        t2.commit()

        return self.run_interleaved([t1, t2])

    def demo_no_conflict(self) -> dict[str, Any]:
        """Caso demostrativo sin conflicto porque cada transacción toca páginas distintas."""
        t1 = Transaction("T1").read("employee.tbl", 0, "search id=1").commit()
        t2 = Transaction("T2").write("employee.tbl", 2, "insert id=99").commit()
        return self.run_interleaved([t1, t2])

    def summary(self, transactions: list[Transaction]) -> dict[str, Any]:
        payload = self.log.to_dict()
        payload["transactions"] = [tx.to_dict() for tx in transactions]
        payload["has_conflicts"] = payload["conflict_count"] > 0
        return payload
