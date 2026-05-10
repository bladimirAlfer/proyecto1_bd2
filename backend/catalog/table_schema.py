from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .index_schema import IndexSchema, normalize_index_type


SUPPORTED_COLUMN_TYPES = {
    "int",
    "integer",
    "float",
    "double",
    "real",
    "str",
    "string",
    "text",
    "varchar",
    "bool",
    "boolean",
}

_TYPE_ALIASES = {
    "integer": "int",
    "double": "float",
    "real": "float",
    "string": "str",
    "text": "str",
    "varchar": "str",
    "boolean": "bool",
}


def normalize_column_type(type_name: str) -> str:
    value = type_name.strip().lower()
    if value not in SUPPORTED_COLUMN_TYPES:
        allowed = ", ".join(sorted(SUPPORTED_COLUMN_TYPES))
        raise ValueError(f"Tipo de columna no soportado: {type_name}. Permitidos: {allowed}")
    return _TYPE_ALIASES.get(value, value)


@dataclass
class ColumnSchema:
    name: str
    type: str
    index: str | None = None

    def __post_init__(self) -> None:
        self.name = self.name.strip()
        self.type = normalize_column_type(self.type)

        if not self.name:
            raise ValueError("El nombre de columna no puede estar vacío")

        if self.index is not None:
            self.index = normalize_index_type(self.index)

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "name": self.name,
            "type": self.type,
        }
        if self.index is not None:
            data["index"] = self.index
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnSchema":
        return cls(
            name=data["name"],
            type=data["type"],
            index=data.get("index"),
        )


@dataclass
class TableSchema:
    """
    Metadata de una tabla del mini DBMS.

    Guarda el esquema lógico, el archivo físico de datos y los índices declarados
    por columna. El archivo físico será manipulado por RecordManager usando
    páginas de 4096 bytes.
    """

    name: str
    columns: list[ColumnSchema]
    data_file: str
    indexes: dict[str, IndexSchema] = field(default_factory=dict)
    row_count: int = 0

    def __post_init__(self) -> None:
        self.name = self.name.strip()
        self.data_file = str(Path(self.data_file))

        if not self.name:
            raise ValueError("El nombre de tabla no puede estar vacío")

        if not self.columns:
            raise ValueError("La tabla debe tener al menos una columna")

        names = [col.name for col in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("No se permiten columnas duplicadas")

        column_set = set(names)
        for column_name in self.indexes:
            if column_name not in column_set:
                raise ValueError(f"El índice usa una columna inexistente: {column_name}")

        for col in self.columns:
            if col.index is not None and col.name not in self.indexes:
                index_file = str(Path(self.data_file).with_suffix(f".{col.name}.{col.index}.idx"))
                self.indexes[col.name] = IndexSchema(
                    table_name=self.name,
                    column_name=col.name,
                    index_type=col.index,
                    file_path=index_file,
                )

    @property
    def record_schema(self) -> list[dict[str, str]]:
        return [{"name": col.name, "type": col.type} for col in self.columns]

    def get_column(self, column_name: str) -> ColumnSchema:
        for col in self.columns:
            if col.name == column_name:
                return col
        raise KeyError(f"Columna no encontrada: {column_name}")

    def has_column(self, column_name: str) -> bool:
        return any(col.name == column_name for col in self.columns)

    def has_index(self, column_name: str) -> bool:
        return column_name in self.indexes

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "columns": [col.to_dict() for col in self.columns],
            "data_file": self.data_file,
            "indexes": {name: idx.to_dict() for name, idx in self.indexes.items()},
            "row_count": self.row_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TableSchema":
        columns = [ColumnSchema.from_dict(col) for col in data["columns"]]
        indexes = {
            name: IndexSchema.from_dict(idx)
            for name, idx in data.get("indexes", {}).items()
        }
        return cls(
            name=data["name"],
            columns=columns,
            data_file=data["data_file"],
            indexes=indexes,
            row_count=int(data.get("row_count", 0)),
        )
