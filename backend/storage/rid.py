from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class RID:
    """
    Record ID físico.

    Identifica un registro dentro del archivo paginado:
    - page_id: número de página.
    - slot_id: posición del registro dentro del directorio de slots de esa página.

    Los índices deben guardar key -> RID, no la fila completa.
    """

    page_id: int
    slot_id: int

    def __post_init__(self) -> None:
        if self.page_id < 0:
            raise ValueError("page_id debe ser >= 0")
        if self.slot_id < 0:
            raise ValueError("slot_id debe ser >= 0")

    def to_tuple(self) -> tuple[int, int]:
        return self.page_id, self.slot_id

    @classmethod
    def from_tuple(cls, value: tuple[int, int]) -> "RID":
        return cls(page_id=int(value[0]), slot_id=int(value[1]))

    def __str__(self) -> str:
        return f"RID(page={self.page_id}, slot={self.slot_id})"
