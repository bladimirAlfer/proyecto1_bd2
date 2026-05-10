from __future__ import annotations

from pathlib import Path

from .db_engine import DBEngine


BASE_DIR = Path("data/db")


def main() -> None:
    db = DBEngine(BASE_DIR)
    print("Mini DBMS - Fase 2")
    print("Tablas registradas:", db.list_tables())


if __name__ == "__main__":
    main()
