from __future__ import annotations

from typing import Any

from .ast_nodes import (
    BetweenCondition,
    ColumnDefinition,
    CreateTableCommand,
    DeleteCommand,
    DropTableCommand,
    EqualsCondition,
    InsertCommand,
    Point,
    SelectAllCommand,
    SelectCommand,
    SpatialKNNCondition,
    SpatialRadiusCondition,
)
from .tokenizer import Token, Tokenizer, TokenizerError


class SQLParserError(ValueError):
    pass


class SQLParser:
    """
    Parser descendente recursivo para el subconjunto SQL del proyecto.

    Sentencias soportadas:
    - CREATE TABLE name (col type [INDEX tecnica], ...) [FROM FILE path];
    - SELECT * FROM table;
    - SELECT TOP n * FROM table;
    - SELECT * FROM table WHERE col = value;
    - SELECT * FROM table WHERE col BETWEEN v1 AND v2;
    - SELECT * FROM table WHERE col IN (POINT(x, y), RADIUS r);
    - SELECT * FROM table WHERE col IN (POINT(x, y), K k);
    - INSERT INTO table VALUES (...);
    - DELETE FROM table WHERE col = value;
    - DROP TABLE table;
    """

    def __init__(self, sql: str) -> None:
        try:
            self.tokens = Tokenizer(sql).tokenize()
        except TokenizerError as exc:
            raise SQLParserError(str(exc)) from exc
        self.pos = 0

    def parse(self):
        command = self._parse_statement()
        self._optional_symbol(";")
        self._expect_type("EOF")
        return command

    def _parse_statement(self):
        current = self._current()
        if current.is_keyword("CREATE"):
            return self._parse_create_table()
        if current.is_keyword("SELECT"):
            return self._parse_select()
        if current.is_keyword("INSERT"):
            return self._parse_insert()
        if current.is_keyword("DELETE"):
            return self._parse_delete()
        if current.is_keyword("DROP"):
            return self._parse_drop_table()
        self._error("Se esperaba CREATE, SELECT, INSERT, DELETE o DROP")

    # ------------------------------------------------------------------
    # CREATE TABLE
    # ------------------------------------------------------------------

    def _parse_create_table(self) -> CreateTableCommand:
        self._expect_keyword("CREATE")
        self._expect_keyword("TABLE")
        table_name = self._expect_identifier()
        self._expect_symbol("(")

        columns: list[ColumnDefinition] = []
        while True:
            columns.append(self._parse_column_definition())
            if not self._optional_symbol(","):
                break

        self._expect_symbol(")")

        from_file: str | None = None
        if self._optional_keyword("FROM"):
            self._expect_keyword("FILE")
            from_file = self._parse_file_path()

        return CreateTableCommand(table_name=table_name, columns=columns, from_file=from_file)

    def _parse_column_definition(self) -> ColumnDefinition:
        name = self._expect_identifier()
        type_name = self._expect_type_name()

        index_type: str | None = None
        if self._optional_keyword("INDEX"):
            index_type = self._expect_identifier_like()

        return ColumnDefinition(name=name, type=type_name, index_type=index_type)

    def _expect_type_name(self) -> str:
        # El tipo puede venir como int, float, varchar, etc. Puede ser keyword o identifier.
        token = self._current()
        if token.type not in {"IDENTIFIER", "KEYWORD"}:
            self._error("Se esperaba tipo de columna")
        self.pos += 1
        type_name = token.value.lower()

        # Permite varchar(100), ignorando el tamaño para el almacenamiento JSON.
        if self._optional_symbol("("):
            self._expect_type("NUMBER")
            self._expect_symbol(")")

        return type_name

    def _parse_file_path(self) -> str:
        token = self._current()
        if token.type == "STRING":
            self.pos += 1
            return token.value

        # Path no entrecomillado: reconstruye lexemas hasta ; o EOF.
        parts: list[str] = []
        while not self._current().is_symbol(";") and self._current().type != "EOF":
            parts.append(self._current().value)
            self.pos += 1

        path = "".join(parts).strip()
        if not path:
            self._error("Se esperaba ruta después de FROM FILE")
        return path

    # ------------------------------------------------------------------
    # SELECT
    # ------------------------------------------------------------------

    def _parse_select(self) -> SelectCommand | SelectAllCommand:
        self._expect_keyword("SELECT")

        top: int | None = None
        if self._optional_keyword("TOP"):
            top_token = self._expect_type("NUMBER")
            top_value = self._number_value(top_token.value)
            if not isinstance(top_value, int) or top_value <= 0:
                self._error("TOP debe ser un número entero positivo")
            top = top_value

        self._expect_symbol("*")
        self._expect_keyword("FROM")
        table_name = self._expect_identifier()

        if self._optional_keyword("WHERE"):
            if top is not None:
                self._error("TOP solo está soportado para SELECT * FROM tabla sin WHERE")
            condition = self._parse_condition()
            return SelectCommand(table_name=table_name, condition=condition)

        return SelectAllCommand(table_name=table_name, top=top)

    def _parse_condition(self):
        column = self._expect_identifier()

        if self._optional_symbol("="):
            value = self._parse_literal()
            return EqualsCondition(column=column, value=value)

        if self._optional_keyword("BETWEEN"):
            begin = self._parse_literal()
            self._expect_keyword("AND")
            end = self._parse_literal()
            return BetweenCondition(column=column, begin=begin, end=end)

        if self._optional_keyword("IN"):
            return self._parse_spatial_condition(column)

        self._error("Se esperaba =, BETWEEN o IN en la condición WHERE")

    def _parse_spatial_condition(self, column: str):
        self._expect_symbol("(")
        self._expect_keyword("POINT")
        self._expect_symbol("(")
        x = self._expect_number_as_float()
        self._expect_symbol(",")
        y = self._expect_number_as_float()
        self._expect_symbol(")")
        point = Point(x=x, y=y)
        self._expect_symbol(",")

        if self._optional_keyword("RADIUS"):
            radius = self._expect_number_as_float()
            self._expect_symbol(")")
            return SpatialRadiusCondition(column=column, point=point, radius=radius)

        if self._optional_keyword("K"):
            k_token = self._expect_type("NUMBER")
            k_value = self._number_value(k_token.value)
            if not isinstance(k_value, int):
                self._error("K debe ser un número entero")
            self._expect_symbol(")")
            return SpatialKNNCondition(column=column, point=point, k=k_value)

        self._error("Se esperaba RADIUS o K en consulta espacial")

    # ------------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------------

    def _parse_insert(self) -> InsertCommand:
        self._expect_keyword("INSERT")
        self._expect_keyword("INTO")
        table_name = self._expect_identifier()
        self._expect_keyword("VALUES")
        self._expect_symbol("(")

        values: list[Any] = []
        if not self._current().is_symbol(")"):
            while True:
                values.append(self._parse_literal())
                if not self._optional_symbol(","):
                    break

        self._expect_symbol(")")
        return InsertCommand(table_name=table_name, values=values)

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------

    def _parse_delete(self) -> DeleteCommand | DropTableCommand:
        self._expect_keyword("DELETE")

        # Alias no estándar, agregado para la UI: DELETE TABLE employee;
        # La sentencia SQL recomendada sigue siendo DROP TABLE employee;
        if self._optional_keyword("TABLE"):
            table_name = self._expect_identifier()
            return DropTableCommand(table_name=table_name)

        self._expect_keyword("FROM")
        table_name = self._expect_identifier()
        self._expect_keyword("WHERE")
        column = self._expect_identifier()
        self._expect_symbol("=")
        value = self._parse_literal()
        return DeleteCommand(table_name=table_name, column=column, value=value)

    def _parse_drop_table(self) -> DropTableCommand:
        self._expect_keyword("DROP")
        self._expect_keyword("TABLE")
        table_name = self._expect_identifier()
        return DropTableCommand(table_name=table_name)

    # ------------------------------------------------------------------
    # Literales y utilidades
    # ------------------------------------------------------------------

    def _parse_literal(self) -> Any:
        token = self._current()

        if token.type == "STRING":
            self.pos += 1
            return token.value

        if token.type == "NUMBER":
            self.pos += 1
            return self._number_value(token.value)

        # Permite true/false/null como identificadores simples, y también
        # strings sin comillas para casos básicos.
        if token.type in {"IDENTIFIER", "KEYWORD"}:
            self.pos += 1
            value = token.value
            upper = value.upper()
            if upper == "NULL":
                return None
            if upper == "TRUE":
                return True
            if upper == "FALSE":
                return False
            return value

        self._error("Se esperaba literal")

    def _expect_number_as_float(self) -> float:
        token = self._expect_type("NUMBER")
        return float(token.value)

    @staticmethod
    def _number_value(raw: str) -> int | float:
        if "." in raw:
            return float(raw)
        return int(raw)

    def _current(self) -> Token:
        return self.tokens[self.pos]

    def _expect_keyword(self, value: str) -> Token:
        token = self._current()
        if not token.is_keyword(value):
            self._error(f"Se esperaba keyword {value}")
        self.pos += 1
        return token

    def _optional_keyword(self, value: str) -> bool:
        if self._current().is_keyword(value):
            self.pos += 1
            return True
        return False

    def _expect_symbol(self, value: str) -> Token:
        token = self._current()
        if not token.is_symbol(value):
            self._error(f"Se esperaba símbolo {value!r}")
        self.pos += 1
        return token

    def _optional_symbol(self, value: str) -> bool:
        if self._current().is_symbol(value):
            self.pos += 1
            return True
        return False

    def _expect_type(self, token_type: str) -> Token:
        token = self._current()
        if token.type != token_type:
            self._error(f"Se esperaba token de tipo {token_type}")
        self.pos += 1
        return token

    def _expect_identifier(self) -> str:
        token = self._current()
        if token.type != "IDENTIFIER":
            self._error("Se esperaba identificador")
        self.pos += 1
        return token.value

    def _expect_identifier_like(self) -> str:
        token = self._current()
        if token.type not in {"IDENTIFIER", "KEYWORD"}:
            self._error("Se esperaba identificador")
        self.pos += 1
        return token.value.lower()

    def _error(self, message: str):
        token = self._current()
        raise SQLParserError(f"{message}. Token actual: {token.value!r} en posición {token.position}")


def parse_sql(sql: str):
    return SQLParser(sql).parse()
