from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .table_schema import ColumnSchema, TableSchema


class Catalog:
    """
    Catálogo persistente del mini DBMS.

    Guarda la metadata de tablas e índices en un archivo JSON. No guarda datos de
    registros: esos datos viven en archivos binarios paginados administrados por
    RecordManager.
    """

    def __init__(self, catalog_path: str | Path) -> None:
        self.catalog_path = Path(catalog_path)
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        self.tables: dict[str, TableSchema] = {}
        self.load()

    def load(self) -> None:
        if not self.catalog_path.exists():
            self.tables = {}
            return

        with open(self.catalog_path, "r", encoding="utf-8") as file:
            raw = json.load(file)

        self.tables = {
            name: TableSchema.from_dict(table_data)
            for name, table_data in raw.get("tables", {}).items()
        }

    def save(self) -> None:
        payload = {
            "tables": {
                name: table.to_dict()
                for name, table in sorted(self.tables.items())
            }
        }

        with open(self.catalog_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

    def create_table(self, table_schema: TableSchema, overwrite: bool = False) -> None:
        if table_schema.name in self.tables and not overwrite:
            raise ValueError(f"La tabla ya existe: {table_schema.name}")

        self.tables[table_schema.name] = table_schema
        self.save()

    def drop_table(self, table_name: str, delete_files: bool = False) -> None:
        table = self.get_table(table_name)

        if delete_files:
            data_file = Path(table.data_file)
            if data_file.exists():
                data_file.unlink()

            for index_schema in table.indexes.values():
                index_file = Path(index_schema.file_path)
                if index_file.exists():
                    index_file.unlink()

        del self.tables[table_name]
        self.save()

    def get_table(self, table_name: str) -> TableSchema:
        if table_name not in self.tables:
            raise KeyError(f"Tabla no encontrada: {table_name}")
        return self.tables[table_name]

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.tables

    def list_tables(self) -> list[str]:
        return sorted(self.tables.keys())

    def update_row_count(self, table_name: str, row_count: int) -> None:
        table = self.get_table(table_name)
        table.row_count = int(row_count)
        self.save()

    @staticmethod
    def build_table_schema(
        table_name: str,
        columns: list[dict[str, Any] | ColumnSchema],
        data_file: str | Path,
    ) -> TableSchema:
        normalized_columns: list[ColumnSchema] = []

        for column in columns:
            if isinstance(column, ColumnSchema):
                normalized_columns.append(column)
            else:
                normalized_columns.append(ColumnSchema.from_dict(column))

        return TableSchema(
            name=table_name,
            columns=normalized_columns,
            data_file=str(data_file),
        )
