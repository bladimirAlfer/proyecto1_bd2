from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from backend.indexes.base_index import BaseIndex
from backend.storage.record_manager import RecordManager
from backend.storage.rid import RID
from backend.storage.serializer import Serializer


@dataclass(frozen=True)
class EntryLocation:
    """Ubicación física de una entrada del índice sequential."""

    area: str  # "main" u "overflow"
    rid: RID   # RID dentro del archivo del índice, no de la tabla


class SequentialFile(BaseIndex):
    """
    Sequential File paginado con archivo principal ordenado y archivo auxiliar.

    Diseño físico:
    - file_path + ".main"      : archivo principal ordenado por key.
    - file_path + ".overflow"  : archivo auxiliar de desbordamiento.
    - file_path + ".meta.json" : metadata pequeña del índice.

    Cada entrada guarda:
    - key: clave indexada.
    - rid_page, rid_slot: RID del registro real dentro de la tabla.
    - next_area, next_page, next_slot: puntero lógico a la siguiente entrada.

    Las lecturas/escrituras de main y overflow se hacen mediante RecordManager,
    que a su vez usa PageManager con páginas fijas de 4096 bytes.
    """

    DEFAULT_OVERFLOW_THRESHOLD = 64

    def __init__(
        self,
        file_path: str | Path,
        key_type: str = "int",
        overflow_threshold: int = DEFAULT_OVERFLOW_THRESHOLD,
    ) -> None:
        super().__init__(file_path=file_path, key_type=key_type)
        if overflow_threshold <= 0:
            raise ValueError("overflow_threshold debe ser mayor que 0")

        self.overflow_threshold = overflow_threshold
        self.main_path = Path(f"{self.file_path}.main")
        self.overflow_path = Path(f"{self.file_path}.overflow")
        self.meta_path = Path(f"{self.file_path}.meta.json")

        self.main_path.parent.mkdir(parents=True, exist_ok=True)
        self.overflow_path.parent.mkdir(parents=True, exist_ok=True)
        self.main_path.touch(exist_ok=True)
        self.overflow_path.touch(exist_ok=True)

        self.entry_schema = self._build_entry_schema(key_type)
        self.main_rm = RecordManager(self.main_path, self.entry_schema)
        self.overflow_rm = RecordManager(self.overflow_path, self.entry_schema)

        if not self.meta_path.exists():
            self._write_meta(
                {
                    "key_type": self.key_type,
                    "overflow_threshold": self.overflow_threshold,
                    "main_count": 0,
                    "overflow_count": 0,
                    "head_area": "",
                    "head_page": -1,
                    "head_slot": -1,
                }
            )
        else:
            meta = self._read_meta()
            self.key_type = meta.get("key_type", self.key_type)
            self.overflow_threshold = int(meta.get("overflow_threshold", self.overflow_threshold))

    # ------------------------------------------------------------------
    # API pública del índice
    # ------------------------------------------------------------------

    def add(self, key: Any, rid: RID) -> None:
        """
        Inserta key -> RID.

        Las nuevas entradas se colocan en overflow. Cuando overflow alcanza K
        registros, se reconstruye físicamente el archivo principal en orden.
        """
        key = self._cast_key(key)
        if not isinstance(rid, RID):
            rid = RID(int(rid[0]), int(rid[1]))

        entry = self._make_entry(key, rid)
        index_rid = self.overflow_rm.insert(entry)
        new_location = EntryLocation("overflow", index_rid)

        meta = self._read_meta()
        meta["overflow_count"] = int(meta.get("overflow_count", 0)) + 1
        self._write_meta(meta)

        # Mantiene los punteros lógicos actualizados en el estado actual.
        self._link_new_entry(new_location, entry)

        if self.overflow_count >= self.overflow_threshold:
            self.rebuild()

    def search(self, key: Any) -> list[RID]:
        """
        Busca una clave exacta.

        En main se aprovecha que está ordenado: se corta cuando key_actual > key.
        En overflow se escanea completo porque contiene registros recientes no
        ordenados físicamente.
        """
        key = self._cast_key(key)
        result: list[RID] = []

        for _, entry in self._scan_area("main"):
            current = entry["key"]
            if current == key:
                result.append(self._entry_data_rid(entry))
            elif current > key:
                break

        for _, entry in self._scan_area("overflow"):
            if entry["key"] == key:
                result.append(self._entry_data_rid(entry))

        return result

    def range_search(self, begin_key: Any, end_key: Any) -> list[RID]:
        """
        Busca claves en el intervalo [begin_key, end_key].
        """
        begin_key = self._cast_key(begin_key)
        end_key = self._cast_key(end_key)

        if begin_key > end_key:
            begin_key, end_key = end_key, begin_key

        matches: list[dict[str, Any]] = []

        for _, entry in self._scan_area("main"):
            current = entry["key"]
            if current > end_key:
                break
            if begin_key <= current <= end_key:
                matches.append(entry)

        for _, entry in self._scan_area("overflow"):
            current = entry["key"]
            if begin_key <= current <= end_key:
                matches.append(entry)

        matches.sort(key=self._entry_sort_key)
        return [self._entry_data_rid(entry) for entry in matches]

    def remove(self, key: Any, rid: RID | None = None) -> int:
        """
        Elimina entradas del índice por clave.

        Si rid es None, elimina todas las entradas con esa clave.
        Si rid se envía, elimina solo la entrada key -> rid.
        Retorna la cantidad de entradas eliminadas del índice.
        """
        key = self._cast_key(key)
        target = rid if isinstance(rid, RID) or rid is None else RID(int(rid[0]), int(rid[1]))
        deleted = 0

        for location, entry in list(self._all_active_locations()):
            if entry["key"] != key:
                continue
            if target is not None and self._entry_data_rid(entry) != target:
                continue

            rm = self._rm_for_area(location.area)
            if rm.delete(location.rid):
                deleted += 1
                self._decrement_count(location.area)

        if deleted > 0:
            # La forma más segura de mantener el archivo principal ordenado y los
            # punteros correctos después de una eliminación es reconstruir.
            self.rebuild()

        return deleted

    def rebuild(self) -> None:
        """
        Reconstrucción física del sequential file.

        No carga el archivo principal completo en memoria. Aprovecha que main ya
        está ordenado y solo ordena el overflow, que está acotado por K.
        Luego hace merge secuencial hacia un archivo temporal.
        """
        overflow_entries = [entry for _, entry in self._scan_area("overflow")]
        overflow_entries.sort(key=self._entry_sort_key)

        main_iter = (entry for _, entry in self._scan_area("main"))
        merged_iter = self._merge_sorted(main_iter, iter(overflow_entries))

        tmp_dir = self.main_path.parent
        fd, tmp_name = tempfile.mkstemp(prefix=f"{self.main_path.name}.", suffix=".tmp", dir=tmp_dir)
        os.close(fd)
        tmp_path = Path(tmp_name)

        # Asegurar que el archivo temporal empiece vacío.
        tmp_path.write_bytes(b"")
        tmp_rm = RecordManager(tmp_path, self.entry_schema)

        new_count = 0
        previous_rid: RID | None = None
        previous_entry: dict[str, Any] | None = None
        head_location: EntryLocation | None = None

        for entry in merged_iter:
            clean_entry = self._make_entry(entry["key"], self._entry_data_rid(entry))
            current_rid = tmp_rm.insert(clean_entry)
            current_location = EntryLocation("main", current_rid)

            if head_location is None:
                head_location = current_location

            if previous_rid is not None and previous_entry is not None:
                self._set_next(previous_entry, current_location)
                tmp_rm.update(previous_rid, previous_entry)

            previous_rid = current_rid
            previous_entry = clean_entry
            new_count += 1

        if previous_rid is not None and previous_entry is not None:
            self._set_next(previous_entry, None)
            tmp_rm.update(previous_rid, previous_entry)

        shutil.move(str(tmp_path), str(self.main_path))
        self.overflow_path.write_bytes(b"")

        self.main_rm = RecordManager(self.main_path, self.entry_schema)
        self.overflow_rm = RecordManager(self.overflow_path, self.entry_schema)

        meta = self._read_meta()
        meta["main_count"] = new_count
        meta["overflow_count"] = 0
        if head_location is None:
            meta["head_area"] = ""
            meta["head_page"] = -1
            meta["head_slot"] = -1
        else:
            meta["head_area"] = head_location.area
            meta["head_page"] = head_location.rid.page_id
            meta["head_slot"] = head_location.rid.slot_id
        self._write_meta(meta)

    def reset_counters(self) -> None:
        self.main_rm.reset_counters()
        self.overflow_rm.reset_counters()

    def get_stats(self) -> dict[str, int]:
        main_stats = self.main_rm.get_stats()
        overflow_stats = self.overflow_rm.get_stats()
        reads = main_stats["disk_reads"] + overflow_stats["disk_reads"]
        writes = main_stats["disk_writes"] + overflow_stats["disk_writes"]
        return {
            "disk_reads": reads,
            "disk_writes": writes,
            "disk_accesses": reads + writes,
        }

    def clear(self) -> None:
        """Vacía físicamente main, overflow y metadata."""
        self.main_path.write_bytes(b"")
        self.overflow_path.write_bytes(b"")
        self.main_rm = RecordManager(self.main_path, self.entry_schema)
        self.overflow_rm = RecordManager(self.overflow_path, self.entry_schema)
        self._write_meta(
            {
                "key_type": self.key_type,
                "overflow_threshold": self.overflow_threshold,
                "main_count": 0,
                "overflow_count": 0,
                "head_area": "",
                "head_page": -1,
                "head_slot": -1,
            }
        )

    # ------------------------------------------------------------------
    # Métodos útiles para pruebas/debug
    # ------------------------------------------------------------------

    @property
    def main_count(self) -> int:
        return int(self._read_meta().get("main_count", 0))

    @property
    def overflow_count(self) -> int:
        return int(self._read_meta().get("overflow_count", 0))

    def dump_main_entries(self) -> list[dict[str, Any]]:
        return [entry for _, entry in self._scan_area("main")]

    def dump_overflow_entries(self) -> list[dict[str, Any]]:
        return [entry for _, entry in self._scan_area("overflow")]

    def dump_all_entries_sorted(self) -> list[dict[str, Any]]:
        entries = [entry for _, entry in self._all_active_locations()]
        entries.sort(key=self._entry_sort_key)
        return entries

    # ------------------------------------------------------------------
    # Métodos internos
    # ------------------------------------------------------------------

    @staticmethod
    def _build_entry_schema(key_type: str) -> list[dict[str, str]]:
        return [
            {"name": "key", "type": key_type},
            {"name": "rid_page", "type": "int"},
            {"name": "rid_slot", "type": "int"},
            {"name": "next_area", "type": "str"},
            {"name": "next_page", "type": "int"},
            {"name": "next_slot", "type": "int"},
        ]

    def _make_entry(self, key: Any, data_rid: RID) -> dict[str, Any]:
        return {
            "key": self._cast_key(key),
            "rid_page": data_rid.page_id,
            "rid_slot": data_rid.slot_id,
            # Se inicializa con la cadena más larga usada por area para que las
            # actualizaciones de puntero normalmente no aumenten el tamaño JSON.
            "next_area": "overflow",
            "next_page": -1,
            "next_slot": -1,
        }

    def _cast_key(self, key: Any) -> Any:
        return Serializer.cast_value(key, self.key_type)

    def _entry_data_rid(self, entry: dict[str, Any]) -> RID:
        return RID(int(entry["rid_page"]), int(entry["rid_slot"]))

    def _entry_sort_key(self, entry: dict[str, Any]) -> tuple[Any, int, int]:
        return (entry["key"], int(entry["rid_page"]), int(entry["rid_slot"]))

    def _scan_area(self, area: str) -> Iterator[tuple[EntryLocation, dict[str, Any]]]:
        rm = self._rm_for_area(area)
        for index_rid, entry in rm.scan():
            yield EntryLocation(area, index_rid), entry

    def _all_active_locations(self) -> Iterator[tuple[EntryLocation, dict[str, Any]]]:
        yield from self._scan_area("main")
        yield from self._scan_area("overflow")

    def _rm_for_area(self, area: str) -> RecordManager:
        if area == "main":
            return self.main_rm
        if area == "overflow":
            return self.overflow_rm
        raise ValueError(f"Área inválida: {area}")

    def _read_meta(self) -> dict[str, Any]:
        with open(self.meta_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def _write_meta(self, meta: dict[str, Any]) -> None:
        meta["key_type"] = self.key_type
        meta["overflow_threshold"] = self.overflow_threshold
        with open(self.meta_path, "w", encoding="utf-8") as file:
            json.dump(meta, file, indent=2)

    def _decrement_count(self, area: str) -> None:
        meta = self._read_meta()
        key = "main_count" if area == "main" else "overflow_count"
        meta[key] = max(0, int(meta.get(key, 0)) - 1)
        self._write_meta(meta)

    def _set_next(self, entry: dict[str, Any], next_location: EntryLocation | None) -> None:
        if next_location is None:
            # Mantener next_area como "overflow" evita crecimiento de payload en updates.
            entry["next_area"] = "overflow"
            entry["next_page"] = -1
            entry["next_slot"] = -1
            return

        entry["next_area"] = next_location.area
        entry["next_page"] = next_location.rid.page_id
        entry["next_slot"] = next_location.rid.slot_id

    def _link_new_entry(self, new_location: EntryLocation, new_entry: dict[str, Any]) -> None:
        """
        Inserta el nuevo nodo dentro de la cadena lógica ordenada.

        Este método mantiene punteros prev -> nuevo -> next sin reconstruir todo.
        El orden físico sigue siendo main ordenado + overflow no ordenado.
        """
        new_sort_key = self._entry_sort_key(new_entry)
        predecessor: tuple[EntryLocation, dict[str, Any]] | None = None
        successor: tuple[EntryLocation, dict[str, Any]] | None = None

        for location, entry in self._all_active_locations():
            if location == new_location:
                continue

            current_sort_key = self._entry_sort_key(entry)
            if current_sort_key <= new_sort_key:
                if predecessor is None or current_sort_key > self._entry_sort_key(predecessor[1]):
                    predecessor = (location, entry)
            else:
                if successor is None or current_sort_key < self._entry_sort_key(successor[1]):
                    successor = (location, entry)

        if successor is None:
            self._set_next(new_entry, None)
        else:
            self._set_next(new_entry, successor[0])
        self._rm_for_area(new_location.area).update(new_location.rid, new_entry)

        meta = self._read_meta()
        if predecessor is None:
            meta["head_area"] = new_location.area
            meta["head_page"] = new_location.rid.page_id
            meta["head_slot"] = new_location.rid.slot_id
        else:
            pred_location, pred_entry = predecessor
            self._set_next(pred_entry, new_location)
            self._rm_for_area(pred_location.area).update(pred_location.rid, pred_entry)

            # Si había head, se mantiene. Si no, este nuevo enlace define head.
            if int(meta.get("head_page", -1)) < 0:
                meta["head_area"] = pred_location.area
                meta["head_page"] = pred_location.rid.page_id
                meta["head_slot"] = pred_location.rid.slot_id

        self._write_meta(meta)

    def _merge_sorted(
        self,
        main_iter: Iterator[dict[str, Any]],
        overflow_iter: Iterator[dict[str, Any]],
    ) -> Iterator[dict[str, Any]]:
        main_current = next(main_iter, None)
        overflow_current = next(overflow_iter, None)

        while main_current is not None or overflow_current is not None:
            if main_current is None:
                yield overflow_current
                overflow_current = next(overflow_iter, None)
            elif overflow_current is None:
                yield main_current
                main_current = next(main_iter, None)
            elif self._entry_sort_key(main_current) <= self._entry_sort_key(overflow_current):
                yield main_current
                main_current = next(main_iter, None)
            else:
                yield overflow_current
                overflow_current = next(overflow_iter, None)
