from __future__ import annotations

import json
from typing import Any


class Serializer:
    """
    Serializador simple para registros.

    Formato físico del registro:
    - JSON UTF-8 compacto.

    Ventajas para este proyecto:
    - Soporta registros de longitud variable.
    - Es fácil de depurar.
    - Funciona con esquemas provenientes de CREATE TABLE.

    El RecordManager almacena estos bytes dentro de páginas paginadas.
    """

    @staticmethod
    def serialize(record: dict[str, Any] | list[Any] | tuple[Any, ...], schema: list[dict[str, str]] | None = None) -> bytes:
        normalized = Serializer.normalize_record(record, schema)
        return json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def deserialize(payload: bytes, schema: list[dict[str, str]] | None = None) -> dict[str, Any]:
        if not payload:
            return {}

        record = json.loads(payload.decode("utf-8"))

        if schema is None:
            return record

        return Serializer.cast_record(record, schema)

    @staticmethod
    def normalize_record(record: dict[str, Any] | list[Any] | tuple[Any, ...], schema: list[dict[str, str]] | None = None) -> dict[str, Any]:
        if schema is None:
            if not isinstance(record, dict):
                raise ValueError("Si no hay schema, el registro debe ser dict")
            return dict(record)

        if isinstance(record, dict):
            normalized = {}
            for col in schema:
                name = col["name"]
                normalized[name] = Serializer.cast_value(record.get(name), col["type"])
            return normalized

        if isinstance(record, (list, tuple)):
            if len(record) != len(schema):
                raise ValueError("La cantidad de valores no coincide con el schema")

            normalized = {}
            for value, col in zip(record, schema):
                normalized[col["name"]] = Serializer.cast_value(value, col["type"])
            return normalized

        raise TypeError("record debe ser dict, list o tuple")

    @staticmethod
    def cast_record(record: dict[str, Any], schema: list[dict[str, str]]) -> dict[str, Any]:
        result = {}
        for col in schema:
            name = col["name"]
            result[name] = Serializer.cast_value(record.get(name), col["type"])
        return result

    @staticmethod
    def cast_value(value: Any, type_name: str) -> Any:
        if value is None:
            return None

        t = type_name.lower()

        if t in ("int", "integer"):
            if value == "":
                return None
            return int(value)

        if t in ("float", "double", "real"):
            if value == "":
                return None
            return float(value)

        if t in ("bool", "boolean"):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in ("1", "true", "t", "yes", "y")
            return bool(value)

        if t in ("str", "string", "text", "varchar"):
            return str(value)

        raise ValueError(f"Tipo no soportado: {type_name}")
