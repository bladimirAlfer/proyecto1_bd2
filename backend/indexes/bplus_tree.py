from __future__ import annotations

import bisect
import json
import os
import struct
from pathlib import Path
from typing import Any, Iterator

from backend.indexes.base_index import BaseIndex
from backend.storage.page import PAGE_SIZE, Page
from backend.storage.page_manager import PageManager
from backend.storage.rid import RID
from backend.storage.serializer import Serializer


class BPlusTree(BaseIndex):
    """
    B+ Tree paginado para memoria secundaria.

    Diseño físico:
    - Cada nodo del árbol ocupa exactamente una página de 4096 bytes.
    - Los nodos se guardan en file_path usando PageManager.
    - La metadata pequeña del árbol se guarda en file_path + ".meta.json".

    Estructura lógica:
    - Nodo hoja:
        {
            "is_leaf": true,
            "keys": [...],
            "values": [[[rid_page, rid_slot], ...], ...],
            "next_leaf": page_id | -1,
            "parent": page_id | -1
        }
    - Nodo interno:
        {
            "is_leaf": false,
            "keys": [...],
            "children": [page_id, ...],
            "parent": page_id | -1
        }

    Notas:
    - El índice guarda pares key -> RID. El registro completo vive en el
      RecordManager de la tabla.
    - range_search ubica primero la hoja de begin_key y luego recorre hojas
      enlazadas mediante next_leaf, como pide el proyecto.
    - remove elimina claves/RID de hojas y colapsa la raíz si queda vacía. No
      fuerza redistribución/merge de nodos en cada underflow porque el objetivo
      principal del proyecto es medir acceso paginado y operaciones funcionales.
    """

    MAGIC = b"BPTN"
    HEADER_FORMAT = "<4sI"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DEFAULT_MAX_KEYS = 32

    def __init__(self, file_path: str | Path, key_type: str = "int", max_keys: int = DEFAULT_MAX_KEYS) -> None:
        super().__init__(file_path=file_path, key_type=key_type)

        if max_keys < 3:
            raise ValueError("max_keys debe ser al menos 3")

        self.max_keys = int(max_keys)
        self.meta_path = Path(f"{self.file_path}.meta.json")
        self.page_manager = PageManager(self.file_path)

        if not self.meta_path.exists():
            self._write_meta(
                {
                    "key_type": self.key_type,
                    "max_keys": self.max_keys,
                    "root_page": -1,
                    "height": 0,
                }
            )
        else:
            meta = self._read_meta()
            self.key_type = meta.get("key_type", self.key_type)
            self.max_keys = int(meta.get("max_keys", self.max_keys))

    # ------------------------------------------------------------------
    # API pública del índice
    # ------------------------------------------------------------------

    def add(self, key: Any, rid: RID) -> None:
        key = self._cast_key(key)
        if not isinstance(rid, RID):
            rid = RID(int(rid[0]), int(rid[1]))

        if self.root_page == -1:
            root = self._new_leaf_node(parent=-1)
            root["keys"].append(key)
            root["values"].append([self._rid_to_pair(rid)])
            root_page = self._allocate_node(root)
            self._set_root(root_page, height=1)
            return

        leaf_page = self._find_leaf_page(key)
        leaf = self._read_node(leaf_page)
        self._insert_in_leaf(leaf, key, rid)
        self._write_node(leaf_page, leaf)

        if len(leaf["keys"]) > self.max_keys:
            self._split_leaf(leaf_page)

    def search(self, key: Any) -> list[RID]:
        key = self._cast_key(key)
        if self.root_page == -1:
            return []

        leaf_page = self._find_leaf_page(key)
        result: list[RID] = []

        while leaf_page != -1:
            leaf = self._read_node(leaf_page)
            keys = leaf["keys"]
            pos = bisect.bisect_left(keys, key)

            while pos < len(keys) and keys[pos] == key:
                result.extend(self._pair_to_rid(pair) for pair in leaf["values"][pos])
                pos += 1

            # En un B+ Tree bien formado, las claves duplicadas se guardan en la
            # misma posición de la hoja. Si la siguiente clave ya es mayor, se corta.
            if pos < len(keys) and keys[pos] > key:
                break

            next_leaf = int(leaf.get("next_leaf", -1))
            if next_leaf == -1:
                break

            next_node = self._read_node(next_leaf)
            if not next_node["keys"] or next_node["keys"][0] > key:
                break
            leaf_page = next_leaf

        return result

    def range_search(self, begin_key: Any, end_key: Any) -> list[RID]:
        begin_key = self._cast_key(begin_key)
        end_key = self._cast_key(end_key)

        if begin_key > end_key:
            begin_key, end_key = end_key, begin_key

        if self.root_page == -1:
            return []

        leaf_page = self._find_leaf_page(begin_key)
        result: list[RID] = []

        while leaf_page != -1:
            leaf = self._read_node(leaf_page)
            for key, rid_pairs in zip(leaf["keys"], leaf["values"]):
                if key < begin_key:
                    continue
                if key > end_key:
                    return result
                result.extend(self._pair_to_rid(pair) for pair in rid_pairs)

            leaf_page = int(leaf.get("next_leaf", -1))

        return result

    def remove(self, key: Any, rid: RID | None = None) -> int:
        key = self._cast_key(key)
        if self.root_page == -1:
            return 0

        target = rid if isinstance(rid, RID) or rid is None else RID(int(rid[0]), int(rid[1]))
        leaf_page = self._find_leaf_page(key)
        leaf = self._read_node(leaf_page)
        deleted = 0

        pos = bisect.bisect_left(leaf["keys"], key)
        if pos >= len(leaf["keys"]) or leaf["keys"][pos] != key:
            return 0

        if target is None:
            deleted = len(leaf["values"][pos])
            leaf["keys"].pop(pos)
            leaf["values"].pop(pos)
        else:
            target_pair = self._rid_to_pair(target)
            before = len(leaf["values"][pos])
            leaf["values"][pos] = [pair for pair in leaf["values"][pos] if pair != target_pair]
            deleted = before - len(leaf["values"][pos])
            if not leaf["values"][pos]:
                leaf["keys"].pop(pos)
                leaf["values"].pop(pos)

        if deleted == 0:
            return 0

        self._write_node(leaf_page, leaf)
        self._refresh_parent_separator_after_delete(leaf_page)
        self._collapse_empty_root()
        return deleted

    def rebuild(self) -> None:
        """
        Mantiene compatibilidad con DBEngine.

        A diferencia del Sequential File, el B+ Tree ya queda ordenado y balanceado
        durante cada inserción. Por eso no necesita una reconstrucción final tras
        cargar CSV.
        """
        return None

    def clear(self) -> None:
        self.file_path.write_bytes(b"")
        self.page_manager = PageManager(self.file_path)
        self._write_meta(
            {
                "key_type": self.key_type,
                "max_keys": self.max_keys,
                "root_page": -1,
                "height": 0,
            }
        )

    def reset_counters(self) -> None:
        self.page_manager.reset_counters()

    def get_stats(self) -> dict[str, int]:
        return self.page_manager.get_stats()

    # ------------------------------------------------------------------
    # Propiedades útiles para pruebas/debug
    # ------------------------------------------------------------------

    @property
    def root_page(self) -> int:
        return int(self._read_meta().get("root_page", -1))

    @property
    def height(self) -> int:
        return int(self._read_meta().get("height", 0))

    @property
    def num_pages(self) -> int:
        return self.page_manager.num_pages

    def iter_leaf_items(self) -> Iterator[tuple[Any, list[RID]]]:
        """Iterador de depuración: recorre las hojas enlazadas en orden."""
        if self.root_page == -1:
            return

        page_id = self.root_page
        node = self._read_node(page_id)
        while not node["is_leaf"]:
            page_id = int(node["children"][0])
            node = self._read_node(page_id)

        while page_id != -1:
            node = self._read_node(page_id)
            for key, pairs in zip(node["keys"], node["values"]):
                yield key, [self._pair_to_rid(pair) for pair in pairs]
            page_id = int(node.get("next_leaf", -1))

    # ------------------------------------------------------------------
    # Inserción y split
    # ------------------------------------------------------------------

    def _insert_in_leaf(self, leaf: dict[str, Any], key: Any, rid: RID) -> None:
        keys = leaf["keys"]
        pos = bisect.bisect_left(keys, key)

        if pos < len(keys) and keys[pos] == key:
            pair = self._rid_to_pair(rid)
            if pair not in leaf["values"][pos]:
                leaf["values"][pos].append(pair)
            return

        keys.insert(pos, key)
        leaf["values"].insert(pos, [self._rid_to_pair(rid)])

    def _split_leaf(self, leaf_page: int) -> None:
        leaf = self._read_node(leaf_page)
        mid = (len(leaf["keys"]) + 1) // 2

        right = self._new_leaf_node(parent=int(leaf.get("parent", -1)))
        right["keys"] = leaf["keys"][mid:]
        right["values"] = leaf["values"][mid:]
        right["next_leaf"] = int(leaf.get("next_leaf", -1))

        leaf["keys"] = leaf["keys"][:mid]
        leaf["values"] = leaf["values"][:mid]

        right_page = self._allocate_node(right)
        leaf["next_leaf"] = right_page
        self._write_node(leaf_page, leaf)

        separator = right["keys"][0]
        self._insert_in_parent(leaf_page, separator, right_page)

    def _insert_in_parent(self, left_page: int, key: Any, right_page: int) -> None:
        left = self._read_node(left_page)
        parent_page = int(left.get("parent", -1))

        if parent_page == -1:
            root = self._new_internal_node(parent=-1)
            root["keys"] = [key]
            root["children"] = [left_page, right_page]
            root_page = self._allocate_node(root)

            left["parent"] = root_page
            right = self._read_node(right_page)
            right["parent"] = root_page
            self._write_node(left_page, left)
            self._write_node(right_page, right)
            self._set_root(root_page, height=self.height + 1 if self.height else 2)
            return

        parent = self._read_node(parent_page)
        insert_pos = parent["children"].index(left_page) + 1
        parent["keys"].insert(insert_pos - 1, key)
        parent["children"].insert(insert_pos, right_page)

        right = self._read_node(right_page)
        right["parent"] = parent_page
        self._write_node(right_page, right)
        self._write_node(parent_page, parent)

        if len(parent["keys"]) > self.max_keys:
            self._split_internal(parent_page)

    def _split_internal(self, node_page: int) -> None:
        node = self._read_node(node_page)
        mid = len(node["keys"]) // 2
        promote_key = node["keys"][mid]

        right = self._new_internal_node(parent=int(node.get("parent", -1)))
        right["keys"] = node["keys"][mid + 1:]
        right["children"] = node["children"][mid + 1:]

        node["keys"] = node["keys"][:mid]
        node["children"] = node["children"][:mid + 1]

        right_page = self._allocate_node(right)
        self._write_node(node_page, node)

        for child_page in right["children"]:
            child = self._read_node(int(child_page))
            child["parent"] = right_page
            self._write_node(int(child_page), child)

        self._insert_in_parent(node_page, promote_key, right_page)

    # ------------------------------------------------------------------
    # Búsqueda de hoja
    # ------------------------------------------------------------------

    def _find_leaf_page(self, key: Any) -> int:
        page_id = self.root_page
        if page_id == -1:
            raise ValueError("El árbol está vacío")

        node = self._read_node(page_id)
        while not node["is_leaf"]:
            pos = bisect.bisect_right(node["keys"], key)
            page_id = int(node["children"][pos])
            node = self._read_node(page_id)

        return page_id

    # ------------------------------------------------------------------
    # Eliminación básica
    # ------------------------------------------------------------------

    def _refresh_parent_separator_after_delete(self, leaf_page: int) -> None:
        """
        Si se elimina la primera clave de una hoja, actualiza el separador del
        padre. No compacta páginas obsoletas; mantiene la búsqueda correcta y
        evita cargar el índice completo en memoria.
        """
        leaf = self._read_node(leaf_page)
        parent_page = int(leaf.get("parent", -1))
        if parent_page == -1 or not leaf["keys"]:
            return

        parent = self._read_node(parent_page)
        try:
            child_pos = parent["children"].index(leaf_page)
        except ValueError:
            return

        if child_pos > 0:
            parent["keys"][child_pos - 1] = leaf["keys"][0]
            self._write_node(parent_page, parent)

    def _collapse_empty_root(self) -> None:
        root_page = self.root_page
        if root_page == -1:
            return

        root = self._read_node(root_page)
        if root["is_leaf"]:
            if not root["keys"]:
                self._set_root(-1, height=0)
            return

        if not root["keys"] and len(root["children"]) == 1:
            child_page = int(root["children"][0])
            child = self._read_node(child_page)
            child["parent"] = -1
            self._write_node(child_page, child)
            self._set_root(child_page, height=max(1, self.height - 1))

    # ------------------------------------------------------------------
    # Lectura/escritura paginada de nodos
    # ------------------------------------------------------------------

    def _allocate_node(self, node: dict[str, Any]) -> int:
        page = self.page_manager.allocate_page()
        self._write_node_to_page(page, node)
        self.page_manager.write_page(page)
        return page.page_id

    def _read_node(self, page_id: int) -> dict[str, Any]:
        page = self.page_manager.read_page(page_id)
        magic, length = struct.unpack(self.HEADER_FORMAT, page.read_slice(0, self.HEADER_SIZE))
        if magic != self.MAGIC:
            raise ValueError(f"Página {page_id} no contiene un nodo B+ válido")
        payload = page.read_slice(self.HEADER_SIZE, length)
        return json.loads(payload.decode("utf-8"))

    def _write_node(self, page_id: int, node: dict[str, Any]) -> None:
        page = self.page_manager.read_page(page_id)
        self._write_node_to_page(page, node)
        self.page_manager.write_page(page)

    def _write_node_to_page(self, page: Page, node: dict[str, Any]) -> None:
        payload = json.dumps(node, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(payload) + self.HEADER_SIZE > PAGE_SIZE:
            raise ValueError(
                "El nodo B+ excede una página. Reduce max_keys o usa claves más pequeñas."
            )

        page.data[:] = b"\x00" * PAGE_SIZE
        page.write_slice(0, struct.pack(self.HEADER_FORMAT, self.MAGIC, len(payload)))
        page.write_slice(self.HEADER_SIZE, payload)

    # ------------------------------------------------------------------
    # Factories y metadata
    # ------------------------------------------------------------------

    @staticmethod
    def _new_leaf_node(parent: int) -> dict[str, Any]:
        return {
            "is_leaf": True,
            "keys": [],
            "values": [],
            "next_leaf": -1,
            "parent": int(parent),
        }

    @staticmethod
    def _new_internal_node(parent: int) -> dict[str, Any]:
        return {
            "is_leaf": False,
            "keys": [],
            "children": [],
            "parent": int(parent),
        }

    def _read_meta(self) -> dict[str, Any]:
        if not self.meta_path.exists():
            return {
                "key_type": self.key_type,
                "max_keys": self.max_keys,
                "root_page": -1,
                "height": 0,
            }
        with open(self.meta_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def _write_meta(self, meta: dict[str, Any]) -> None:
        self.meta_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.meta_path.with_suffix(self.meta_path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as file:
            json.dump(meta, file, indent=2, ensure_ascii=False)
        os.replace(tmp, self.meta_path)

    def _set_root(self, root_page: int, height: int) -> None:
        meta = self._read_meta()
        meta["root_page"] = int(root_page)
        meta["height"] = int(height)
        self._write_meta(meta)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _cast_key(self, key: Any) -> Any:
        return Serializer.cast_value(key, self.key_type)

    @staticmethod
    def _rid_to_pair(rid: RID) -> list[int]:
        return [int(rid.page_id), int(rid.slot_id)]

    @staticmethod
    def _pair_to_rid(pair: list[int] | tuple[int, int]) -> RID:
        return RID(int(pair[0]), int(pair[1]))
