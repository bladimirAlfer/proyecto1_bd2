from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class EngineCall:
    """
    Representa la traducción del parser hacia una llamada lógica del motor.

    La Fase 6 implementará QueryExecutor y podrá ejecutar directamente estas
    llamadas. En esta fase se deja la traducción explícita para cumplir la
    especificación del parser: SQL -> operación del índice/motor.
    """

    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class ColumnDefinition:
    name: str
    type: str
    index_type: str | None = None

    def to_engine_column(self) -> dict[str, Any]:
        column = {"name": self.name, "type": self.type}
        if self.index_type is not None:
            column["index"] = self.index_type
        return column


@dataclass(frozen=True)
class Point:
    x: float
    y: float

    def as_tuple(self) -> tuple[float, float]:
        return (self.x, self.y)


@dataclass(frozen=True)
class EqualsCondition:
    column: str
    value: Any
    kind: Literal["equals"] = "equals"


@dataclass(frozen=True)
class BetweenCondition:
    column: str
    begin: Any
    end: Any
    kind: Literal["between"] = "between"


@dataclass(frozen=True)
class SpatialRadiusCondition:
    column: str
    point: Point
    radius: float
    kind: Literal["spatial_radius"] = "spatial_radius"


@dataclass(frozen=True)
class SpatialKNNCondition:
    column: str
    point: Point
    k: int
    kind: Literal["spatial_knn"] = "spatial_knn"


Condition = EqualsCondition | BetweenCondition | SpatialRadiusCondition | SpatialKNNCondition


@dataclass(frozen=True)
class CreateTableCommand:
    table_name: str
    columns: list[ColumnDefinition]
    from_file: str | None = None
    command_type: Literal["create_table"] = "create_table"

    def to_engine_call(self) -> EngineCall:
        columns = [column.to_engine_column() for column in self.columns]
        if self.from_file is not None:
            return EngineCall(
                method="create_table_from_csv",
                args=(self.table_name, columns, self.from_file),
                kwargs={},
            )
        return EngineCall(
            method="create_table",
            args=(self.table_name, columns),
            kwargs={},
        )


@dataclass(frozen=True)
class SelectAllCommand:
    table_name: str
    top: int | None = None
    command_type: Literal["select"] = "select"

    def to_engine_call(self) -> EngineCall:
        return EngineCall(
            method="select_all",
            args=(self.table_name,),
            kwargs={"limit": self.top} if self.top is not None else {},
        )


@dataclass(frozen=True)
class SelectCommand:
    table_name: str
    condition: Condition
    command_type: Literal["select"] = "select"

    def to_engine_call(self) -> EngineCall:
        condition = self.condition
        if isinstance(condition, EqualsCondition):
            return EngineCall(
                method="search_by_index",
                args=(self.table_name, condition.column, condition.value),
                kwargs={},
            )

        if isinstance(condition, BetweenCondition):
            return EngineCall(
                method="range_search_by_index",
                args=(self.table_name, condition.column, condition.begin, condition.end),
                kwargs={},
            )

        if isinstance(condition, SpatialRadiusCondition):
            return EngineCall(
                method="rtree_range_search",
                args=(self.table_name, condition.column, condition.point.as_tuple(), condition.radius),
                kwargs={},
            )

        if isinstance(condition, SpatialKNNCondition):
            return EngineCall(
                method="rtree_knn",
                args=(self.table_name, condition.column, condition.point.as_tuple(), condition.k),
                kwargs={},
            )

        raise TypeError(f"Condición no soportada: {condition!r}")


@dataclass(frozen=True)
class InsertCommand:
    table_name: str
    values: list[Any]
    command_type: Literal["insert"] = "insert"

    def to_engine_call(self) -> EngineCall:
        return EngineCall(
            method="insert_record",
            args=(self.table_name, self.values),
            kwargs={},
        )


@dataclass(frozen=True)
class DeleteCommand:
    table_name: str
    column: str
    value: Any
    command_type: Literal["delete"] = "delete"

    def to_engine_call(self) -> EngineCall:
        return EngineCall(
            method="delete_by_key",
            args=(self.table_name, self.column, self.value),
            kwargs={},
        )


@dataclass(frozen=True)
class DropTableCommand:
    table_name: str
    command_type: Literal["drop_table"] = "drop_table"

    def to_engine_call(self) -> EngineCall:
        return EngineCall(
            method="drop_table",
            args=(self.table_name,),
            kwargs={},
        )
