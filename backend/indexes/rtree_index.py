from __future__ import annotations

import json
import math
import struct
from pathlib import Path
from typing import Any, Iterable

from backend.indexes.base_index import BaseIndex
from backend.storage.page import PAGE_SIZE, Page
from backend.storage.page_manager import PageManager
from backend.storage.rid import RID


MBR = list[float]  # [min_x, min_y, max_x, max_y]


class RTree(BaseIndex):
    """
    R-Tree paginado para claves espaciales 2D.

    El índice almacena entradas punto -> RID. Cada nodo ocupa una página física
    de 4096 bytes y se serializa como JSON compacto dentro de la página.

    Nodo hoja:
        {
            "is_leaf": true,
            "parent": -1,
            "entries": [
                {"mbr": [x, y, x, y], "point": [x, y], "rid": [page, slot]}
            ]
        }

    Nodo interno:
        {
            "is_leaf": false,
            "parent": -1,
            "entries": [
                {"mbr": [minx, miny, maxx, maxy], "child": page_id}
            ]
        }

    Decisiones de diseño:
    - Split lineal simple por el eje con mayor dispersión.
    - No se implementa reinserción por underflow en delete, porque el enunciado
      exige rangeSearch y kNN; remove queda funcional para mantener consistencia
      con BaseIndex.
    - kNN recorre el árbol paginado y ordena candidatos por distancia euclídea.
    """

    MAGIC = b"RTRE"
    HEADER_FORMAT = "<4sI"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    DEFAULT_MAX_ENTRIES = 6
    DEFAULT_MIN_ENTRIES = 3

    def __init__(
        self,
        file_path: str | Path,
        key_type: str = "point",
        max_entries: int = DEFAULT_MAX_ENTRIES,
        min_entries: int | None = None,
    ) -> None:
        super().__init__(file_path=file_path, key_type=key_type)
        if max_entries < 4:
            raise ValueError("max_entries debe ser al menos 4")

        self.max_entries = int(max_entries)
        self.min_entries = int(min_entries if min_entries is not None else max(2, max_entries // 2))
        self.meta_path = Path(f"{self.file_path}.meta.json")
        self.page_manager = PageManager(self.file_path)

        if not self.meta_path.exists():
            self._write_meta(
                {
                    "key_type": self.key_type,
                    "max_entries": self.max_entries,
                    "min_entries": self.min_entries,
                    "root_page": -1,
                    "height": 0,
                }
            )
        else:
            meta = self._read_meta()
            self.key_type = meta.get("key_type", self.key_type)

            # Evita usar metadatos antiguos con nodos muy grandes.
            self.max_entries = min(
                int(meta.get("max_entries", self.max_entries)),
                self.DEFAULT_MAX_ENTRIES,
            )

            self.min_entries = min(
                int(meta.get("min_entries", self.min_entries)),
                max(2, self.max_entries // 2),
            )

            meta["max_entries"] = self.max_entries
            meta["min_entries"] = self.min_entries
            self._write_meta(meta)
    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def add(self, key: Any, rid: RID) -> None:
        point = self._cast_point(key)
        rid = rid if isinstance(rid, RID) else RID(int(rid[0]), int(rid[1]))
        entry = {"mbr": self._point_mbr(point), "point": [point[0], point[1]], "rid": self._rid_to_pair(rid)}

        if self.root_page == -1:
            root = self._new_node(is_leaf=True, parent=-1)
            root["entries"].append(entry)
            root_page = self._allocate_node(root)
            self._set_root(root_page, height=1)
            return

        leaf_page = self._choose_leaf(self.root_page, entry["mbr"])
        leaf = self._read_node(leaf_page)
        leaf["entries"].append(entry)
        self._write_node(leaf_page, leaf)

        if len(leaf["entries"]) > self.max_entries:
            self._split_node(leaf_page)
        else:
            self._adjust_ancestors(leaf_page)

    def search(self, key: Any) -> list[RID]:
        point = self._cast_point(key)
        return self.range_search(point, 0.0)

    def range_search(self, point: Any, radius: Any) -> list[RID]:
        center = self._cast_point(point)
        radius = float(radius)
        if radius < 0:
            raise ValueError("radius debe ser >= 0")
        if self.root_page == -1:
            return []

        query_mbr = [center[0] - radius, center[1] - radius, center[0] + radius, center[1] + radius]
        result: list[RID] = []
        stack = [self.root_page]

        while stack:
            page_id = stack.pop()
            node = self._read_node(page_id)
            if node["is_leaf"]:
                for entry in node["entries"]:
                    if self._mbr_intersects(entry["mbr"], query_mbr):
                        candidate = tuple(entry["point"])
                        if self._distance(center, candidate) <= radius:
                            result.append(self._pair_to_rid(entry["rid"]))
            else:
                for entry in node["entries"]:
                    if self._mbr_intersects(entry["mbr"], query_mbr):
                        stack.append(int(entry["child"]))

        return result

    def knn_search(self, point: Any, k: int) -> list[RID]:
        center = self._cast_point(point)
        k = int(k)
        if k <= 0 or self.root_page == -1:
            return []

        candidates: list[tuple[float, RID]] = []
        stack = [self.root_page]

        while stack:
            page_id = stack.pop()
            node = self._read_node(page_id)
            if node["is_leaf"]:
                for entry in node["entries"]:
                    dist = self._distance(center, tuple(entry["point"]))
                    candidates.append((dist, self._pair_to_rid(entry["rid"])))
            else:
                # Ordena aproximado por distancia mínima al MBR para leer primero
                # nodos prometedores, aunque igual se recorren todos para exactitud.
                children = sorted(
                    node["entries"],
                    key=lambda e: self._min_distance_to_mbr(center, e["mbr"]),
                    reverse=True,
                )
                for entry in children:
                    stack.append(int(entry["child"]))

        candidates.sort(key=lambda item: (item[0], item[1].page_id, item[1].slot_id))
        return [rid for _, rid in candidates[:k]]

    def remove(self, key: Any, rid: RID | None = None) -> int:
        if self.root_page == -1:
            return 0

        point = self._cast_point(key)
        target_pair = None
        if rid is not None:
            target = rid if isinstance(rid, RID) else RID(int(rid[0]), int(rid[1]))
            target_pair = self._rid_to_pair(target)

        deleted = 0
        leaf_pages = self._find_leaf_pages_containing_point(point)
        for leaf_page in leaf_pages:
            leaf = self._read_node(leaf_page)
            before = len(leaf["entries"])
            filtered = []
            for entry in leaf["entries"]:
                same_point = self._same_point(tuple(entry["point"]), point)
                same_rid = target_pair is None or entry["rid"] == target_pair
                if same_point and same_rid:
                    deleted += 1
                else:
                    filtered.append(entry)
            if len(filtered) != before:
                leaf["entries"] = filtered
                self._write_node(leaf_page, leaf)
                self._adjust_ancestors(leaf_page)

        self._collapse_empty_root()
        return deleted

    def clear(self) -> None:
        self.file_path.write_bytes(b"")
        self.page_manager = PageManager(self.file_path)
        self._write_meta(
            {
                "key_type": self.key_type,
                "max_entries": self.max_entries,
                "min_entries": self.min_entries,
                "root_page": -1,
                "height": 0,
            }
        )

    def rebuild(self) -> None:
        return None

    def reset_counters(self) -> None:
        self.page_manager.reset_counters()

    def get_stats(self) -> dict[str, int]:
        return self.page_manager.get_stats()

    # ------------------------------------------------------------------
    # Propiedades/debug
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

    def iter_points(self) -> Iterable[tuple[tuple[float, float], RID]]:
        if self.root_page == -1:
            return []
        items: list[tuple[tuple[float, float], RID]] = []
        stack = [self.root_page]
        while stack:
            node = self._read_node(stack.pop())
            if node["is_leaf"]:
                for entry in node["entries"]:
                    items.append((tuple(entry["point"]), self._pair_to_rid(entry["rid"])))
            else:
                for entry in node["entries"]:
                    stack.append(int(entry["child"]))
        return items

    # ------------------------------------------------------------------
    # Inserción y split
    # ------------------------------------------------------------------

    def _choose_leaf(self, page_id: int, entry_mbr: MBR) -> int:
        node = self._read_node(page_id)
        if node["is_leaf"]:
            return page_id

        best = None
        best_key = None
        for entry in node["entries"]:
            current_mbr = entry["mbr"]
            enlargement = self._area(self._mbr_union(current_mbr, entry_mbr)) - self._area(current_mbr)
            area = self._area(current_mbr)
            key = (enlargement, area)
            if best is None or key < best_key:
                best = int(entry["child"])
                best_key = key
        return self._choose_leaf(best, entry_mbr)

    def _split_node(self, page_id: int) -> None:
        node = self._read_node(page_id)
        group_a, group_b = self._linear_split(node["entries"])

        node["entries"] = group_a
        self._write_node(page_id, node)

        new_node = self._new_node(is_leaf=bool(node["is_leaf"]), parent=int(node.get("parent", -1)))
        new_node["entries"] = group_b
        new_page = self._allocate_node(new_node)

        if not node["is_leaf"]:
            self._set_children_parent(page_id, node["entries"])
            self._set_children_parent(new_page, new_node["entries"])

        parent_page = int(node.get("parent", -1))
        if parent_page == -1:
            root = self._new_node(is_leaf=False, parent=-1)
            root["entries"] = [
                {"mbr": self._node_mbr(node), "child": page_id},
                {"mbr": self._node_mbr(new_node), "child": new_page},
            ]
            root_page = self._allocate_node(root)
            node["parent"] = root_page
            new_node["parent"] = root_page
            self._write_node(page_id, node)
            self._write_node(new_page, new_node)
            self._set_root(root_page, height=self.height + 1)
            return

        parent = self._read_node(parent_page)
        # Actualiza MBR del hijo viejo y agrega el nuevo hijo.
        for entry in parent["entries"]:
            if int(entry["child"]) == page_id:
                entry["mbr"] = self._node_mbr(node)
                break
        parent["entries"].append({"mbr": self._node_mbr(new_node), "child": new_page})
        self._write_node(parent_page, parent)

        if len(parent["entries"]) > self.max_entries:
            self._split_node(parent_page)
        else:
            self._adjust_ancestors(parent_page)

    def _linear_split(self, entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if len(entries) <= 1:
            return entries, []

        # Elige el eje con mayor dispersión de centros.
        centers_x = [(entry["mbr"][0] + entry["mbr"][2]) / 2 for entry in entries]
        centers_y = [(entry["mbr"][1] + entry["mbr"][3]) / 2 for entry in entries]
        spread_x = max(centers_x) - min(centers_x)
        spread_y = max(centers_y) - min(centers_y)
        axis = 0 if spread_x >= spread_y else 1

        ordered = sorted(entries, key=lambda e: (e["mbr"][axis] + e["mbr"][axis + 2]) / 2)
        split_at = len(ordered) // 2
        split_at = max(self.min_entries, min(split_at, len(ordered) - self.min_entries))
        return ordered[:split_at], ordered[split_at:]

    def _adjust_ancestors(self, page_id: int) -> None:
        current_page = page_id
        while current_page != -1:
            node = self._read_node(current_page)
            parent_page = int(node.get("parent", -1))
            if parent_page == -1:
                break
            parent = self._read_node(parent_page)
            changed = False
            current_mbr = self._node_mbr(node)
            for entry in parent["entries"]:
                if int(entry["child"]) == current_page:
                    if entry["mbr"] != current_mbr:
                        entry["mbr"] = current_mbr
                        changed = True
                    break
            if changed:
                self._write_node(parent_page, parent)
            current_page = parent_page

    def _collapse_empty_root(self) -> None:
        if self.root_page == -1:
            return
        root = self._read_node(self.root_page)
        if root["entries"]:
            if not root["is_leaf"] and len(root["entries"]) == 1:
                child_page = int(root["entries"][0]["child"])
                child = self._read_node(child_page)
                child["parent"] = -1
                self._write_node(child_page, child)
                self._set_root(child_page, height=max(1, self.height - 1))
            return
        self._set_root(-1, height=0)

    def _find_leaf_pages_containing_point(self, point: tuple[float, float]) -> list[int]:
        point_mbr = self._point_mbr(point)
        stack = [self.root_page]
        leaves: list[int] = []
        while stack:
            page_id = stack.pop()
            node = self._read_node(page_id)
            if node["is_leaf"]:
                leaves.append(page_id)
            else:
                for entry in node["entries"]:
                    if self._mbr_contains(entry["mbr"], point_mbr):
                        stack.append(int(entry["child"]))
        return leaves

    # ------------------------------------------------------------------
    # Serialización de páginas
    # ------------------------------------------------------------------

    def _new_node(self, *, is_leaf: bool, parent: int) -> dict[str, Any]:
        return {"is_leaf": is_leaf, "parent": int(parent), "entries": []}

    def _allocate_node(self, node: dict[str, Any]) -> int:
        page = self.page_manager.allocate_page()
        self._write_node(page.page_id, node)
        return page.page_id

    def _read_node(self, page_id: int) -> dict[str, Any]:
        page = self.page_manager.read_page(page_id)
        raw = page.to_bytes()
        magic, payload_len = struct.unpack(self.HEADER_FORMAT, raw[: self.HEADER_SIZE])
        if magic != self.MAGIC:
            raise ValueError(f"Página {page_id} no contiene un nodo R-Tree válido")
        payload = raw[self.HEADER_SIZE : self.HEADER_SIZE + payload_len]
        return json.loads(payload.decode("utf-8"))

    def _write_node(self, page_id: int, node: dict[str, Any]) -> None:
        payload = json.dumps(node, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if self.HEADER_SIZE + len(payload) > PAGE_SIZE:
            raise ValueError(
                "Nodo R-Tree excede una página de 4096 bytes. "
                "Reduce max_entries o usa registros/keys más pequeños."
            )
        raw = bytearray(PAGE_SIZE)
        raw[: self.HEADER_SIZE] = struct.pack(self.HEADER_FORMAT, self.MAGIC, len(payload))
        raw[self.HEADER_SIZE : self.HEADER_SIZE + len(payload)] = payload
        self.page_manager.write_page(Page.from_bytes(page_id, bytes(raw)))

    def _set_children_parent(self, parent_page: int, entries: list[dict[str, Any]]) -> None:
        for entry in entries:
            child_page = int(entry["child"])
            child = self._read_node(child_page)
            child["parent"] = parent_page
            self._write_node(child_page, child)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def _read_meta(self) -> dict[str, Any]:
        if not self.meta_path.exists():
            return {}
        with open(self.meta_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def _write_meta(self, meta: dict[str, Any]) -> None:
        self.meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.meta_path, "w", encoding="utf-8") as file:
            json.dump(meta, file, indent=2)

    def _set_root(self, root_page: int, height: int) -> None:
        meta = self._read_meta()
        meta["root_page"] = int(root_page)
        meta["height"] = int(height)
        meta["key_type"] = self.key_type
        meta["max_entries"] = self.max_entries
        meta["min_entries"] = self.min_entries
        self._write_meta(meta)

    # ------------------------------------------------------------------
    # Geometría
    # ------------------------------------------------------------------

    @staticmethod
    def _point_mbr(point: tuple[float, float]) -> MBR:
        return [float(point[0]), float(point[1]), float(point[0]), float(point[1])]

    @staticmethod
    def _mbr_union(a: MBR, b: MBR) -> MBR:
        return [min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])]

    @staticmethod
    def _area(mbr: MBR) -> float:
        return max(0.0, mbr[2] - mbr[0]) * max(0.0, mbr[3] - mbr[1])

    @classmethod
    def _node_mbr(cls, node: dict[str, Any]) -> MBR:
        entries = node.get("entries", [])
        if not entries:
            return [0.0, 0.0, 0.0, 0.0]
        mbr = list(entries[0]["mbr"])
        for entry in entries[1:]:
            mbr = cls._mbr_union(mbr, entry["mbr"])
        return mbr

    @staticmethod
    def _mbr_intersects(a: MBR, b: MBR) -> bool:
        return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])

    @staticmethod
    def _mbr_contains(a: MBR, b: MBR) -> bool:
        return a[0] <= b[0] <= b[2] <= a[2] and a[1] <= b[1] <= b[3] <= a[3]

    @staticmethod
    def _distance(a: tuple[float, float], b: tuple[float, float]) -> float:
        return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

    @staticmethod
    def _min_distance_to_mbr(point: tuple[float, float], mbr: MBR) -> float:
        x, y = point
        dx = 0.0 if mbr[0] <= x <= mbr[2] else min(abs(x - mbr[0]), abs(x - mbr[2]))
        dy = 0.0 if mbr[1] <= y <= mbr[3] else min(abs(y - mbr[1]), abs(y - mbr[3]))
        return math.sqrt(dx * dx + dy * dy)

    @staticmethod
    def _same_point(a: tuple[float, float], b: tuple[float, float]) -> bool:
        return math.isclose(a[0], b[0]) and math.isclose(a[1], b[1])

    # ------------------------------------------------------------------
    # Conversión de claves/RID
    # ------------------------------------------------------------------

    @staticmethod
    def _cast_point(value: Any) -> tuple[float, float]:
        if isinstance(value, dict):
            if "x" in value and "y" in value:
                return (float(value["x"]), float(value["y"]))
            if "longitude" in value and "latitude" in value:
                return (float(value["longitude"]), float(value["latitude"]))
            if "lon" in value and "lat" in value:
                return (float(value["lon"]), float(value["lat"]))

        if isinstance(value, (list, tuple)) and len(value) == 2:
            return (float(value[0]), float(value[1]))

        if isinstance(value, str):
            cleaned = value.strip().replace("POINT", "").replace("point", "")
            cleaned = cleaned.strip("()[]{} ")
            parts = [part.strip() for part in cleaned.split(",")]
            if len(parts) == 2:
                return (float(parts[0]), float(parts[1]))

        raise ValueError(f"No se pudo convertir a punto 2D: {value!r}")

    @staticmethod
    def _rid_to_pair(rid: RID) -> list[int]:
        return [int(rid.page_id), int(rid.slot_id)]

    @staticmethod
    def _pair_to_rid(pair: list[int] | tuple[int, int]) -> RID:
        return RID(int(pair[0]), int(pair[1]))
