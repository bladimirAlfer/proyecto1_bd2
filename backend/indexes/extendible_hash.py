from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path
from typing import Any

from backend.indexes.base_index import BaseIndex
from backend.storage.page import PAGE_SIZE, Page
from backend.storage.page_manager import PageManager
from backend.storage.rid import RID
from backend.storage.serializer import Serializer


class ExtendibleHash(BaseIndex):
    """
    Extendible Hashing paginado para memoria secundaria.

    Diseño:
    - El índice guarda pares key -> RID.
    - Cada bucket ocupa exactamente una página de 4096 bytes.
    - El directorio dinámico vive en un archivo pequeño de metadata JSON.
    - Los buckets se leen/escriben únicamente mediante PageManager.

    Estructura de bucket:
    {
        "local_depth": 2,
        "entries": [
            {"key": 10, "rids": [[0, 1], [0, 2]]},
            {"key": 25, "rids": [[4, 0]]}
        ]
    }

    Importante:
    - Extendible Hashing no soporta range_search. Por eso ese método lanza
      NotImplementedError, alineado con el enunciado del proyecto.
    - El límite bucket_size se aplica a claves distintas. Claves duplicadas se
      almacenan como una sola entrada con varios RIDs.
    """

    MAGIC = b"EXHB"
    HEADER_FORMAT = "<4sI"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DEFAULT_BUCKET_SIZE = 4
    DEFAULT_GLOBAL_DEPTH = 1
    DEFAULT_MAX_GLOBAL_DEPTH = 20

    def __init__(
        self,
        file_path: str | Path,
        key_type: str = "int",
        bucket_size: int = DEFAULT_BUCKET_SIZE,
        max_global_depth: int = DEFAULT_MAX_GLOBAL_DEPTH,
    ) -> None:
        super().__init__(file_path=file_path, key_type=key_type)

        if bucket_size < 1:
            raise ValueError("bucket_size debe ser >= 1")
        if max_global_depth < 1:
            raise ValueError("max_global_depth debe ser >= 1")

        self.bucket_size = int(bucket_size)
        self.max_global_depth = int(max_global_depth)
        self.meta_path = Path(f"{self.file_path}.meta.json")
        self.page_manager = PageManager(self.file_path)

        if not self.meta_path.exists():
            self._initialize_empty()
        else:
            meta = self._read_meta()
            self.key_type = meta.get("key_type", self.key_type)
            self.bucket_size = int(meta.get("bucket_size", self.bucket_size))
            self.max_global_depth = int(meta.get("max_global_depth", self.max_global_depth))

    # ------------------------------------------------------------------
    # API pública del índice
    # ------------------------------------------------------------------

    def add(self, key: Any, rid: RID) -> None:
        key = self._cast_key(key)
        rid = self._ensure_rid(rid)
        rid_pair = self._rid_to_pair(rid)

        while True:
            bucket_page = self._directory_lookup_page(key)
            bucket = self._read_bucket(bucket_page)

            entry = self._find_entry(bucket, key)
            if entry is not None:
                if rid_pair not in entry["rids"]:
                    entry["rids"].append(rid_pair)
                    entry["rids"].sort()
                    self._write_bucket(bucket_page, bucket)
                return

            if len(bucket["entries"]) < self.bucket_size:
                bucket["entries"].append({"key": key, "rids": [rid_pair]})
                bucket["entries"].sort(key=lambda item: item["key"])
                self._write_bucket(bucket_page, bucket)
                return

            # Bucket lleno: split local. Luego se reintenta la inserción.
            if int(bucket["local_depth"]) >= self.max_global_depth:
                # Caso patológico: demasiadas claves comparten los mismos bits.
                # Para no romper la operación, permitimos una página con más
                # entradas y lo dejamos documentado como overflow lógico.
                bucket["entries"].append({"key": key, "rids": [rid_pair]})
                bucket["entries"].sort(key=lambda item: item["key"])
                self._write_bucket(bucket_page, bucket)
                return

            self._split_bucket(bucket_page)

    def search(self, key: Any) -> list[RID]:
        key = self._cast_key(key)
        bucket_page = self._directory_lookup_page(key)
        bucket = self._read_bucket(bucket_page)
        entry = self._find_entry(bucket, key)
        if entry is None:
            return []
        return [self._pair_to_rid(pair) for pair in entry["rids"]]

    def range_search(self, begin_key: Any, end_key: Any) -> list[RID]:
        raise NotImplementedError("Extendible Hashing no soporta rangeSearch")

    def remove(self, key: Any, rid: RID | None = None) -> int:
        key = self._cast_key(key)
        bucket_page = self._directory_lookup_page(key)
        bucket = self._read_bucket(bucket_page)
        entry_index = self._find_entry_index(bucket, key)
        if entry_index == -1:
            return 0

        entry = bucket["entries"][entry_index]
        if rid is None:
            deleted = len(entry["rids"])
            bucket["entries"].pop(entry_index)
            self._write_bucket(bucket_page, bucket)
            return deleted

        target_pair = self._rid_to_pair(self._ensure_rid(rid))
        before = len(entry["rids"])
        entry["rids"] = [pair for pair in entry["rids"] if pair != target_pair]
        deleted = before - len(entry["rids"])

        if deleted == 0:
            return 0

        if not entry["rids"]:
            bucket["entries"].pop(entry_index)
        else:
            entry["rids"].sort()

        self._write_bucket(bucket_page, bucket)
        return deleted

    def rebuild(self) -> None:
        """No-op para mantener API común con DBEngine."""
        return None

    def clear(self) -> None:
        self.file_path.write_bytes(b"")
        self.page_manager = PageManager(self.file_path)
        self._initialize_empty()

    def reset_counters(self) -> None:
        self.page_manager.reset_counters()

    def get_stats(self) -> dict[str, int]:
        return self.page_manager.get_stats()

    # ------------------------------------------------------------------
    # Propiedades útiles
    # ------------------------------------------------------------------

    @property
    def global_depth(self) -> int:
        return int(self._read_meta()["global_depth"])

    @property
    def directory(self) -> list[int]:
        return [int(value) for value in self._read_meta()["directory"]]

    # ------------------------------------------------------------------
    # Inicialización y metadata
    # ------------------------------------------------------------------

    def _initialize_empty(self) -> None:
        bucket0 = self._allocate_bucket(local_depth=self.DEFAULT_GLOBAL_DEPTH)
        bucket1 = self._allocate_bucket(local_depth=self.DEFAULT_GLOBAL_DEPTH)
        self._write_meta(
            {
                "key_type": self.key_type,
                "bucket_size": self.bucket_size,
                "max_global_depth": self.max_global_depth,
                "global_depth": self.DEFAULT_GLOBAL_DEPTH,
                "directory": [bucket0, bucket1],
            }
        )

    def _read_meta(self) -> dict[str, Any]:
        with open(self.meta_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def _write_meta(self, meta: dict[str, Any]) -> None:
        self.meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.meta_path, "w", encoding="utf-8") as file:
            json.dump(meta, file, indent=2)

    # ------------------------------------------------------------------
    # Directorio y hashing
    # ------------------------------------------------------------------

    def _directory_lookup_page(self, key: Any) -> int:
        meta = self._read_meta()
        index = self._directory_index(key, int(meta["global_depth"]))
        return int(meta["directory"][index])

    def _directory_index(self, key: Any, depth: int) -> int:
        if depth <= 0:
            return 0
        mask = (1 << depth) - 1
        return self._stable_hash(key) & mask

    def _stable_hash(self, key: Any) -> int:
        payload = json.dumps(
            {"type": self.key_type, "key": key},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hashlib.sha256(payload).digest()
        return int.from_bytes(digest[:8], byteorder="little", signed=False)

    def _split_bucket(self, bucket_page: int) -> None:
        meta = self._read_meta()
        directory = [int(value) for value in meta["directory"]]
        global_depth = int(meta["global_depth"])

        old_bucket = self._read_bucket(bucket_page)
        old_depth = int(old_bucket["local_depth"])

        if old_depth == global_depth:
            if global_depth >= self.max_global_depth:
                return
            directory = directory + directory
            global_depth += 1

        new_depth = old_depth + 1
        new_bucket_page = self._allocate_bucket(local_depth=new_depth)

        old_bucket["local_depth"] = new_depth
        old_entries = old_bucket["entries"]
        old_bucket["entries"] = []
        new_bucket = {"local_depth": new_depth, "entries": []}

        split_bit = 1 << old_depth
        for i, page in enumerate(directory):
            if page == bucket_page and (i & split_bit):
                directory[i] = new_bucket_page

        # Redistribuir claves distintas según el directorio actualizado.
        for entry in old_entries:
            target_page = directory[self._directory_index(entry["key"], global_depth)]
            if target_page == bucket_page:
                old_bucket["entries"].append(entry)
            else:
                new_bucket["entries"].append(entry)

        old_bucket["entries"].sort(key=lambda item: item["key"])
        new_bucket["entries"].sort(key=lambda item: item["key"])

        self._write_bucket(bucket_page, old_bucket)
        self._write_bucket(new_bucket_page, new_bucket)

        meta["global_depth"] = global_depth
        meta["directory"] = directory
        self._write_meta(meta)

    # ------------------------------------------------------------------
    # Bucket pages
    # ------------------------------------------------------------------

    def _allocate_bucket(self, local_depth: int) -> int:
        page = self.page_manager.allocate_page()
        bucket = {"local_depth": int(local_depth), "entries": []}
        self._write_bucket(page.page_id, bucket)
        return page.page_id

    def _read_bucket(self, page_id: int) -> dict[str, Any]:
        page = self.page_manager.read_page(page_id)
        magic, payload_size = struct.unpack(self.HEADER_FORMAT, page.read_slice(0, self.HEADER_SIZE))

        if magic != self.MAGIC:
            raise ValueError(f"La página {page_id} no es un bucket ExtendibleHash válido")

        if payload_size < 0 or payload_size > PAGE_SIZE - self.HEADER_SIZE:
            raise ValueError(f"Bucket corrupto en página {page_id}")

        payload = page.read_slice(self.HEADER_SIZE, payload_size)
        bucket = json.loads(payload.decode("utf-8"))
        bucket["local_depth"] = int(bucket.get("local_depth", 1))
        bucket["entries"] = list(bucket.get("entries", []))
        return bucket

    def _write_bucket(self, page_id: int, bucket: dict[str, Any]) -> None:
        payload = json.dumps(bucket, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        max_payload = PAGE_SIZE - self.HEADER_SIZE
        if len(payload) > max_payload:
            raise ValueError(
                "El bucket excede una página de 4096 bytes. "
                "Reduce bucket_size o usa claves/RIDs más pequeños."
            )

        page = Page.empty(page_id)
        header = struct.pack(self.HEADER_FORMAT, self.MAGIC, len(payload))
        page.write_slice(0, header)
        page.write_slice(self.HEADER_SIZE, payload)
        self.page_manager.write_page(page)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_entry(self, bucket: dict[str, Any], key: Any) -> dict[str, Any] | None:
        index = self._find_entry_index(bucket, key)
        if index == -1:
            return None
        return bucket["entries"][index]

    @staticmethod
    def _find_entry_index(bucket: dict[str, Any], key: Any) -> int:
        for i, entry in enumerate(bucket["entries"]):
            if entry["key"] == key:
                return i
        return -1

    def _cast_key(self, key: Any) -> Any:
        return Serializer.cast_value(key, self.key_type)

    @staticmethod
    def _ensure_rid(rid: RID | tuple[int, int] | list[int]) -> RID:
        if isinstance(rid, RID):
            return rid
        return RID(int(rid[0]), int(rid[1]))

    @staticmethod
    def _rid_to_pair(rid: RID) -> list[int]:
        return [int(rid.page_id), int(rid.slot_id)]

    @staticmethod
    def _pair_to_rid(pair: list[int] | tuple[int, int]) -> RID:
        return RID(int(pair[0]), int(pair[1]))
