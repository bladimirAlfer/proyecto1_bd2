from __future__ import annotations

import os
from pathlib import Path

from .page import PAGE_SIZE, Page


class PageManager:
    """
    Administra un archivo binario como un conjunto de páginas de tamaño fijo.

    Responsabilidades:
    - Leer páginas completas.
    - Escribir páginas completas.
    - Crear nuevas páginas.
    - Contabilizar accesos a disco por operación.

    Importante:
    No expone métodos para leer el archivo completo. Esto fuerza a que las capas
    superiores respeten el acceso paginado pedido por el proyecto.
    """

    def __init__(self, file_path: str | Path, page_size: int = PAGE_SIZE) -> None:
        if page_size != PAGE_SIZE:
            raise ValueError("Esta implementación usa PAGE_SIZE fijo de 4096 bytes")

        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.page_size = page_size
        self.read_count = 0
        self.write_count = 0

        if not self.file_path.exists():
            self.file_path.touch()

    def read_page(self, page_id: int) -> Page:
        """Lee una página completa desde disco y suma 1 lectura."""
        self._validate_page_id(page_id)

        if page_id >= self.num_pages:
            raise IndexError(f"La página {page_id} no existe")

        with open(self.file_path, "rb") as file:
            file.seek(page_id * self.page_size)
            raw = file.read(self.page_size)

        self.read_count += 1
        return Page.from_bytes(page_id, raw)

    def write_page(self, page: Page) -> None:
        """Escribe una página completa en disco y suma 1 escritura."""
        self._validate_page_id(page.page_id)

        raw = page.to_bytes()

        with open(self.file_path, "r+b") as file:
            file.seek(page.page_id * self.page_size)
            file.write(raw)

        self.write_count += 1

    def allocate_page(self) -> Page:
        """
        Crea una página vacía al final del archivo.

        La inicialización de la página cuenta como una escritura a disco.
        """
        page_id = self.num_pages
        page = Page.empty(page_id)

        with open(self.file_path, "ab") as file:
            file.write(page.to_bytes())

        self.write_count += 1
        return page

    def reset_counters(self) -> None:
        self.read_count = 0
        self.write_count = 0

    def get_stats(self) -> dict[str, int]:
        return {
            "disk_reads": self.read_count,
            "disk_writes": self.write_count,
            "disk_accesses": self.read_count + self.write_count,
        }

    @property
    def num_pages(self) -> int:
        size = os.path.getsize(self.file_path)
        return size // self.page_size

    def close(self) -> None:
        """
        Método placeholder para mantener una API clara.
        No hay handler abierto de forma permanente.
        """
        return None

    def _validate_page_id(self, page_id: int) -> None:
        if page_id < 0:
            raise ValueError("page_id debe ser >= 0")
