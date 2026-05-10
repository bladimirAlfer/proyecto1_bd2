from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


SUPPORTED_INDEX_TYPES = {
    "sequential",
    "sequential_file",
    "extendible_hash",
    "hash",
    "bplus",
    "bplus_tree",
    "rtree",
    "r_tree",
}

_INDEX_ALIASES = {
    "sequential_file": "sequential",
    "hash": "extendible_hash",
    "bplus_tree": "bplus",
    "r_tree": "rtree",
}


def normalize_index_type(index_type: str) -> str:
    value = index_type.strip().lower()
    if value not in SUPPORTED_INDEX_TYPES:
        allowed = ", ".join(sorted(SUPPORTED_INDEX_TYPES))
        raise ValueError(f"Tipo de índice no soportado: {index_type}. Permitidos: {allowed}")
    return _INDEX_ALIASES.get(value, value)


@dataclass
class IndexSchema:
    """
    Metadata de un índice asociado a una columna.

    En la Fase 2 solo se registra la metadata del índice. Las estructuras físicas
    reales se implementan en fases posteriores: SequentialFile, ExtendibleHash,
    BPlusTree y RTree.
    """

    table_name: str
    column_name: str
    index_type: str
    file_path: str
    options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.table_name = self.table_name.strip()
        self.column_name = self.column_name.strip()
        self.index_type = normalize_index_type(self.index_type)
        self.file_path = str(Path(self.file_path))

        if not self.table_name:
            raise ValueError("table_name no puede estar vacío")
        if not self.column_name:
            raise ValueError("column_name no puede estar vacío")

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "column_name": self.column_name,
            "index_type": self.index_type,
            "file_path": self.file_path,
            "options": self.options,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IndexSchema":
        return cls(
            table_name=data["table_name"],
            column_name=data["column_name"],
            index_type=data["index_type"],
            file_path=data["file_path"],
            options=dict(data.get("options", {})),
        )
