from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


KEYWORDS = {
    "AND",
    "BETWEEN",
    "BY",
    "CREATE",
    "DELETE",
    "DROP",
    "FILE",
    "FROM",
    "IN",
    "INDEX",
    "INSERT",
    "INTO",
    "K",
    "POINT",
    "RADIUS",
    "SELECT",
    "TABLE",
    "TOP",
    "VALUES",
    "WHERE",
}

SINGLE_CHAR_SYMBOLS = {
    "(",
    ")",
    ",",
    ";",
    "*",
    "=",
    "/",
    "\\",
    ".",
    ":",
    "-",
}


@dataclass(frozen=True)
class Token:
    type: str
    value: str
    position: int

    def is_keyword(self, value: str) -> bool:
        return self.type == "KEYWORD" and self.value.upper() == value.upper()

    def is_symbol(self, value: str) -> bool:
        return self.type == "SYMBOL" and self.value == value


class TokenizerError(ValueError):
    pass


class Tokenizer:
    """
    Tokenizer simple para el subconjunto SQL del proyecto.

    No depende de librerías externas. Reconoce:
    - keywords case-insensitive
    - identificadores
    - números enteros/flotantes, incluyendo negativos
    - strings con comillas simples o dobles
    - símbolos de SQL y símbolos útiles para rutas de archivo
    """

    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.length = len(sql)
        self.pos = 0

    def tokenize(self) -> list[Token]:
        tokens: list[Token] = []

        while self.pos < self.length:
            char = self.sql[self.pos]

            if char.isspace():
                self.pos += 1
                continue

            if char == "-" and self._peek(1) == "-":
                self._skip_line_comment()
                continue

            if char == "/" and self._peek(1) == "*":
                self._skip_block_comment()
                continue

            if char in ("'", '"'):
                tokens.append(self._read_string())
                continue

            if char.isdigit() or (char == "-" and self._peek(1).isdigit()):
                tokens.append(self._read_number())
                continue

            if self._is_identifier_start(char):
                tokens.append(self._read_identifier_or_keyword())
                continue

            if char in SINGLE_CHAR_SYMBOLS:
                tokens.append(Token("SYMBOL", char, self.pos))
                self.pos += 1
                continue

            raise TokenizerError(f"Carácter no reconocido en posición {self.pos}: {char!r}")

        tokens.append(Token("EOF", "", self.pos))
        return tokens

    def _peek(self, offset: int) -> str:
        index = self.pos + offset
        if index >= self.length:
            return ""
        return self.sql[index]

    def _read_string(self) -> Token:
        quote = self.sql[self.pos]
        start = self.pos
        self.pos += 1
        chars: list[str] = []

        while self.pos < self.length:
            char = self.sql[self.pos]

            if char == quote:
                # SQL permite duplicar comillas: 'O''Brien'.
                if self._peek(1) == quote:
                    chars.append(quote)
                    self.pos += 2
                    continue
                self.pos += 1
                return Token("STRING", "".join(chars), start)

            if char == "\\" and self.pos + 1 < self.length:
                self.pos += 1
                chars.append(self.sql[self.pos])
                self.pos += 1
                continue

            chars.append(char)
            self.pos += 1

        raise TokenizerError(f"String sin cerrar desde posición {start}")

    def _read_number(self) -> Token:
        start = self.pos

        if self.sql[self.pos] == "-":
            self.pos += 1

        has_dot = False
        while self.pos < self.length:
            char = self.sql[self.pos]
            if char.isdigit():
                self.pos += 1
            elif char == "." and not has_dot and self._peek(1).isdigit():
                has_dot = True
                self.pos += 1
            else:
                break

        return Token("NUMBER", self.sql[start:self.pos], start)

    def _read_identifier_or_keyword(self) -> Token:
        start = self.pos
        self.pos += 1

        while self.pos < self.length and self._is_identifier_part(self.sql[self.pos]):
            self.pos += 1

        raw = self.sql[start:self.pos]
        upper = raw.upper()
        if upper in KEYWORDS:
            return Token("KEYWORD", upper, start)
        return Token("IDENTIFIER", raw, start)

    def _skip_line_comment(self) -> None:
        while self.pos < self.length and self.sql[self.pos] != "\n":
            self.pos += 1

    def _skip_block_comment(self) -> None:
        start = self.pos
        self.pos += 2
        while self.pos < self.length - 1:
            if self.sql[self.pos] == "*" and self.sql[self.pos + 1] == "/":
                self.pos += 2
                return
            self.pos += 1
        raise TokenizerError(f"Comentario de bloque sin cerrar desde posición {start}")

    @staticmethod
    def _is_identifier_start(char: str) -> bool:
        return char.isalpha() or char == "_"

    @staticmethod
    def _is_identifier_part(char: str) -> bool:
        return char.isalnum() or char == "_"


def tokenize(sql: str) -> list[Token]:
    return Tokenizer(sql).tokenize()


def token_values(tokens: Iterable[Token]) -> list[str]:
    return [token.value for token in tokens]
