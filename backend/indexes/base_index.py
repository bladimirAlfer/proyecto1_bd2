from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from backend.storage.rid import RID


class BaseIndex(ABC):
    """
    Interfaz común para los índices del mini DBMS.

    Los índices almacenan pares key -> RID. El RID apunta al registro real
    dentro del archivo de datos manejado por RecordManager.
    """

    def __init__(self, file_path: str | Path, key_type: str = "int") -> None:
        self.file_path = Path(file_path)
        self.key_type = key_type
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def add(self, key: Any, rid: RID) -> None:
        raise NotImplementedError

    @abstractmethod
    def search(self, key: Any) -> list[RID]:
        raise NotImplementedError

    @abstractmethod
    def range_search(self, begin_key: Any, end_key: Any) -> list[RID]:
        raise NotImplementedError

    @abstractmethod
    def remove(self, key: Any, rid: RID | None = None) -> int:
        raise NotImplementedError

    @abstractmethod
    def reset_counters(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_stats(self) -> dict[str, int]:
        raise NotImplementedError
