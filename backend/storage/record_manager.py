from __future__ import annotations

import struct
from pathlib import Path
from typing import Any, Iterator

from .page import PAGE_SIZE, Page
from .page_manager import PageManager
from .rid import RID
from .serializer import Serializer


class RecordManager:
    """
    Maneja registros de longitud variable usando slotted pages.

    Layout de cada página:

    [ Header ][ datos de registros ... espacio libre ... ][ slot directory ]

    Header:
    - magic: identifica que la página pertenece al RecordManager.
    - slot_count: cantidad total de slots creados.
    - free_start: primer byte libre después de los datos.
    - free_end: primer byte ocupado por el directorio de slots desde el final.

    Slot:
    - offset: posición donde inicia el registro.
    - size: tamaño del registro en bytes.
    - flags: 1 activo, 0 eliminado.

    Esta estructura permite insertar/eliminar sin cargar el archivo completo.
    """

    MAGIC = b"RMPG"
    HEADER_FORMAT = "<4sHHH"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    SLOT_FORMAT = "<HHB"
    SLOT_SIZE = struct.calcsize(SLOT_FORMAT)

    ACTIVE = 1
    DELETED = 0

    def __init__(self, file_path: str | Path, schema: list[dict[str, str]] | None = None) -> None:
        self.page_manager = PageManager(file_path)
        self.schema = schema

    def insert(self, record: dict[str, Any] | list[Any] | tuple[Any, ...]) -> RID:
        payload = Serializer.serialize(record, self.schema)

        if len(payload) + self.SLOT_SIZE + self.HEADER_SIZE > PAGE_SIZE:
            raise ValueError("El registro es demasiado grande para una página")

        # 1) Intentar reutilizar un slot eliminado con espacio suficiente.
        for page_id in range(self.page_manager.num_pages):
            page = self._read_or_format_page(page_id)
            reused_slot = self._try_reuse_deleted_slot(page, payload)
            if reused_slot is not None:
                self.page_manager.write_page(page)
                return RID(page_id, reused_slot)

        # 2) Intentar insertar en alguna página con espacio libre.
        for page_id in range(self.page_manager.num_pages):
            page = self._read_or_format_page(page_id)
            if self._has_space_for_new_slot(page, len(payload)):
                slot_id = self._insert_new_slot(page, payload)
                self.page_manager.write_page(page)
                return RID(page_id, slot_id)

        # 3) Si no hay espacio, crear una nueva página.
        page = self.page_manager.allocate_page()
        self._format_page(page)
        slot_id = self._insert_new_slot(page, payload)
        self.page_manager.write_page(page)
        return RID(page.page_id, slot_id)

    def read(self, rid: RID) -> dict[str, Any] | None:
        page = self.page_manager.read_page(rid.page_id)

        if not self._is_formatted(page):
            return None

        slot_count, _, _ = self._read_header_values(page)
        if rid.slot_id >= slot_count:
            return None

        offset, size, flags = self._read_slot(page, rid.slot_id)
        if flags != self.ACTIVE:
            return None

        payload = page.read_slice(offset, size)
        return Serializer.deserialize(payload, self.schema)

    def delete(self, rid: RID) -> bool:
        page = self.page_manager.read_page(rid.page_id)

        if not self._is_formatted(page):
            return False

        slot_count, _, _ = self._read_header_values(page)
        if rid.slot_id >= slot_count:
            return False

        offset, size, flags = self._read_slot(page, rid.slot_id)
        if flags != self.ACTIVE:
            return False

        self._write_slot(page, rid.slot_id, offset, size, self.DELETED)
        self.page_manager.write_page(page)
        return True

    def update(self, rid: RID, record: dict[str, Any] | list[Any] | tuple[Any, ...]) -> RID:
        """
        Actualiza un registro.

        Si el nuevo registro entra en el mismo slot, se mantiene el RID.
        Si no entra, se elimina el registro anterior y se inserta uno nuevo.
        """
        payload = Serializer.serialize(record, self.schema)
        page = self.page_manager.read_page(rid.page_id)

        if not self._is_formatted(page):
            raise ValueError("RID inválido: página sin formato")

        slot_count, _, _ = self._read_header_values(page)
        if rid.slot_id >= slot_count:
            raise ValueError("RID inválido: slot fuera de rango")

        offset, old_size, flags = self._read_slot(page, rid.slot_id)
        if flags != self.ACTIVE:
            raise ValueError("RID inválido: registro eliminado")

        if len(payload) <= old_size:
            page.write_slice(offset, payload)
            self._write_slot(page, rid.slot_id, offset, len(payload), self.ACTIVE)
            self.page_manager.write_page(page)
            return rid

        self._write_slot(page, rid.slot_id, offset, old_size, self.DELETED)
        self.page_manager.write_page(page)
        return self.insert(record)

    def scan(self) -> Iterator[tuple[RID, dict[str, Any]]]:
        """
        Recorre todos los registros activos página por página.

        No carga el archivo completo; cada iteración lee páginas usando PageManager.
        """
        for page_id in range(self.page_manager.num_pages):
            page = self.page_manager.read_page(page_id)
            if not self._is_formatted(page):
                continue

            slot_count, _, _ = self._read_header_values(page)
            for slot_id in range(slot_count):
                offset, size, flags = self._read_slot(page, slot_id)
                if flags == self.ACTIVE:
                    payload = page.read_slice(offset, size)
                    yield RID(page_id, slot_id), Serializer.deserialize(payload, self.schema)

    def reset_counters(self) -> None:
        self.page_manager.reset_counters()

    def get_stats(self) -> dict[str, int]:
        return self.page_manager.get_stats()

    # -----------------------------
    # Métodos internos de página
    # -----------------------------

    def _read_or_format_page(self, page_id: int) -> Page:
        page = self.page_manager.read_page(page_id)
        if not self._is_formatted(page):
            self._format_page(page)
            self.page_manager.write_page(page)
        return page

    def _format_page(self, page: Page) -> None:
        self._write_header(
            page=page,
            slot_count=0,
            free_start=self.HEADER_SIZE,
            free_end=PAGE_SIZE,
        )

    def _is_formatted(self, page: Page) -> bool:
        return page.read_slice(0, 4) == self.MAGIC

    def _read_header_values(self, page: Page) -> tuple[int, int, int]:
        magic, slot_count, free_start, free_end = struct.unpack(
            self.HEADER_FORMAT,
            page.read_slice(0, self.HEADER_SIZE),
        )

        if magic != self.MAGIC:
            raise ValueError("Página no inicializada para RecordManager")

        return slot_count, free_start, free_end

    def _write_header(self, page: Page, slot_count: int, free_start: int, free_end: int) -> None:
        raw = struct.pack(self.HEADER_FORMAT, self.MAGIC, slot_count, free_start, free_end)
        page.write_slice(0, raw)

    def _slot_position(self, slot_id: int) -> int:
        return PAGE_SIZE - self.SLOT_SIZE * (slot_id + 1)

    def _read_slot(self, page: Page, slot_id: int) -> tuple[int, int, int]:
        pos = self._slot_position(slot_id)
        return struct.unpack(self.SLOT_FORMAT, page.read_slice(pos, self.SLOT_SIZE))

    def _write_slot(self, page: Page, slot_id: int, offset: int, size: int, flags: int) -> None:
        pos = self._slot_position(slot_id)
        raw = struct.pack(self.SLOT_FORMAT, offset, size, flags)
        page.write_slice(pos, raw)

    def _has_space_for_new_slot(self, page: Page, payload_size: int) -> bool:
        _, free_start, free_end = self._read_header_values(page)
        return free_end - free_start >= payload_size + self.SLOT_SIZE

    def _insert_new_slot(self, page: Page, payload: bytes) -> int:
        slot_count, free_start, free_end = self._read_header_values(page)

        if free_end - free_start < len(payload) + self.SLOT_SIZE:
            raise ValueError("No hay espacio suficiente en la página")

        page.write_slice(free_start, payload)

        slot_id = slot_count
        new_free_start = free_start + len(payload)
        new_free_end = free_end - self.SLOT_SIZE

        self._write_slot(page, slot_id, free_start, len(payload), self.ACTIVE)
        self._write_header(page, slot_count + 1, new_free_start, new_free_end)

        return slot_id

    def _try_reuse_deleted_slot(self, page: Page, payload: bytes) -> int | None:
        slot_count, _, _ = self._read_header_values(page)

        for slot_id in range(slot_count):
            offset, size, flags = self._read_slot(page, slot_id)
            if flags == self.DELETED and len(payload) <= size:
                page.write_slice(offset, payload)
                self._write_slot(page, slot_id, offset, len(payload), self.ACTIVE)
                return slot_id

        return None
