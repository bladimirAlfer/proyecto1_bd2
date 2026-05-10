from __future__ import annotations

from dataclasses import dataclass


PAGE_SIZE = 4096


@dataclass
class Page:
    """
    Representa una página física de tamaño fijo.

    Regla del proyecto:
    - Toda lectura/escritura hacia disco debe hacerse como una página completa.
    - Esta clase solo encapsula los bytes de una página; no abre archivos.
    """

    page_id: int
    data: bytearray

    def __post_init__(self) -> None:
        if len(self.data) > PAGE_SIZE:
            raise ValueError(f"La página no puede exceder {PAGE_SIZE} bytes")

        if len(self.data) < PAGE_SIZE:
            self.data.extend(b"\x00" * (PAGE_SIZE - len(self.data)))

    @classmethod
    def empty(cls, page_id: int) -> "Page":
        return cls(page_id=page_id, data=bytearray(PAGE_SIZE))

    @classmethod
    def from_bytes(cls, page_id: int, raw: bytes) -> "Page":
        return cls(page_id=page_id, data=bytearray(raw))

    def to_bytes(self) -> bytes:
        if len(self.data) != PAGE_SIZE:
            raise ValueError("La página debe tener tamaño fijo antes de escribirse")
        return bytes(self.data)

    def read_slice(self, offset: int, size: int) -> bytes:
        self._validate_range(offset, size)
        return bytes(self.data[offset:offset + size])

    def write_slice(self, offset: int, payload: bytes) -> None:
        self._validate_range(offset, len(payload))
        self.data[offset:offset + len(payload)] = payload

    def _validate_range(self, offset: int, size: int) -> None:
        if offset < 0 or size < 0:
            raise ValueError("offset y size deben ser no negativos")

        if offset + size > PAGE_SIZE:
            raise ValueError("La operación excede el tamaño de la página")
