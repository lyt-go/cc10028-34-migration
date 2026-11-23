"""解析页版本服务：无版本 v1 解析页到带版本 v2 解析页记录的迁移、确认与消费输入解析。

背景：新版解析环境把「解析页」升级为带版本的记录（page_version v2）。升级前已经
落盘但尚未被分块切分消费的旧页（page_version v1，无版本字段）需要迁移到 v2，且：

- 同一文档升级后可能再次解析产生不同版本的补页，老任务恢复与新任务执行会同时面对
  多批输入，必须能查出每批页属于哪个版本、是否已确认；
- 迁移只复制、不改写旧页，迁移失败/中断后仍可从保留的 v1 旧页继续；
- 分块切分只允许消费「已确认」的版本，迁移切换不会把未确认补页掺入已确认输入，
  重跑也不会重复计页（页号+版本唯一、产物带清单幂等）。

产物布局（DATA_ROOT 下）：

- ``parsed/{doc}_parsed.jsonl``               旧格式 v1 解析页（迁移期间只读保留）
- ``parsed/pages/{doc}__v{version}.jsonl``    带版本 v2 解析页记录
- ``page_version/migration/{doc}.json``       迁移状态/台账（含锁与页清单）
- ``page_version/confirmed/{doc}.json``       每文档已确认版本集合
"""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from app.services.base import BaseModuleService
from app.store import store

V1 = "v1"
V2 = "v2"
LEGACY_PARSE_VERSION = V1

# 迁移状态机
STATE_PENDING = "待迁移"
STATE_RUNNING = "迁移中"
STATE_MIGRATED = "已迁移"
STATE_CONFIRMED = "已确认"
STATE_FAILED = "迁移失败"

# 进程内互斥：老任务恢复与新任务执行可能并发操作同一文档的迁移台账/确认集合。
_LOCK_TIMEOUT_SECONDS = 300.0


class PageVersionService(BaseModuleService):
    """解析页版本登记、v1→v2 迁移与消费输入确认。"""

    MODULE = "page_version"
    ENTITY = "解析页版本"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = [
        "文档编号", "格式版本", "页版本", "页号范围", "页数",
        "迁移状态", "是否确认", "来源路径", "更新时间",
    ]
    REQUIRED_FIELDS = ["文档编号", "页版本"]
    STATUS_ORDER = ["草稿", "待迁移", "迁移中", "已迁移", "已确认", "迁移失败"]
    ACTION_RULES = {
        "启动迁移": "迁移中",
        "确认版本": "已确认",
        "标记失败": "迁移失败",
    }
    NEGATIVE_ACTIONS = ["标记失败"]

    # ------------------------------------------------------------------ 路径

    def legacy_pages_path(self, doc_id: str) -> Path:
        """旧格式（v1、无版本字段）解析页的保留读取路径。"""
        return self._data_dir("parsed") / f"{doc_id}_parsed.jsonl"

    def versioned_pages_path(self, doc_id: str, version: str) -> Path:
        """带版本 v2 解析页记录路径；版本号写进文件名，避免不同代页互相覆盖。"""
        safe = self._safe_version(version)
        return self._data_dir("parsed") / "pages" / f"{doc_id}__{safe}.jsonl"

    def migration_state_path(self, doc_id: str) -> Path:
        return self._data_dir("page_version") / "migration" / f"{doc_id}.json"

    def confirmed_path(self, doc_id: str) -> Path:
        return self._data_dir("page_version") / "confirmed" / f"{doc_id}.json"

    @staticmethod
    def _safe_version(version: str) -> str:
        safe = str(version).strip().replace("/", "_").replace("\\", "_")
        return safe if safe.startswith("v") else f"v{safe}"

    # ------------------------------------------------------------------ 锁

    @contextmanager
    def _doc_lock(self, doc_id: str) -> Iterator[None]:
        """以迁移状态文件为载体的可重入进程锁，带过期时间防止中断残留死锁。"""
        lock_path = self._data_dir("page_version") / "locks" / f"{doc_id}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.time() + _LOCK_TIMEOUT_SECONDS
        while True:
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with open(fd, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"pid": os.getpid(), "acquired_at": time.time()}))
                break
            except FileExistsError:
                stale = self._read_json(lock_path)
                acquired = float((stale or {}).get("acquired_at", 0))
                if acquired and time.time() - acquired > _LOCK_TIMEOUT_SECONDS:
                    # 持有者崩溃残留的过期锁，强制接管。
                    lock_path.unlink(missing_ok=True)
                    continue
                if time.time() >= deadline:
                    raise TimeoutError(f"文档 {doc_id} 的迁移锁等待超时")
                time.sleep(0.02)
        try:
            yield
        finally:
            lock_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------ 状态

    def get_migration_state(self, doc_id: str) -> dict[str, Any] | None:
        return self._read_json(self.migration_state_path(doc_id))

    def _save_migration_state(self, doc_id: str, state: dict[str, Any]) -> None:
        state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        self._write_json_atomic(self.migration_state_path(doc_id), state)

    def list_versions(self, doc_id: str) -> list[dict[str, Any]]:
        """列出文档的全部带版本页代次及确认状态，供接手人判断拿到的是哪组页。"""
        confirmed = self._read_confirmed(doc_id)
        result = []
        pages_dir = self._data_dir("parsed") / "pages"
        for fp in sorted(pages_dir.glob(f"{doc_id}__v*.jsonl")):
            version = fp.stem.split("__", 1)[1]
            pages = self._read_jsonl(fp)
            result.append({
                "doc_id": doc_id,
                "version": version,
                "page_count": len(pages),
                "page_nums": [p.get("page_num") for p in pages],
                "confirmed": version in confirmed,
                "path": str(fp),
            })
        return result

    def _read_confirmed(self, doc_id: str) -> set[str]:
        payload = self._read_json(self.confirmed_path(doc_id)) or {}
        return set(payload.get("confirmed_versions", []))

    def _write_confirmed(self, doc_id: str, versions: set[str]) -> None:
        self._write_json_atomic(self.confirmed_path(doc_id), {
            "doc_id": doc_id,
            "confirmed_versions": sorted(versions),
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    # ------------------------------------------------------------------ 写入 v2

    def write_versioned_pages(
        self,
        doc_id: str,
        version: str,
        pages: list[dict[str, Any]],
        *,
        source: str = "",
        confirmed: bool = False,
    ) -> dict[str, Any]:
        """写入一整代带版本 v2 解析页。

        同代重写走原子替换并默认重置确认，避免「文件内容已换、确认标记仍旧」的错配。
        每页带页号与内容摘要（content_hash/page_summary），可与源记录逐页核对。
        """
        safe_version = self._safe_version(version)
        records = []
        for idx, page in enumerate(pages, start=1):
            page_num = int(page.get("page_num", idx))
            summary = str(
                page.get("page_summary")
                or page.get("summary")
                or page.get("text", "")
            )[:120]
            content_hash = page.get("content_hash") or self._compute_checksum(
                str(page.get("paragraphs", "")) + summary + str(page_num)
            )
            records.append({
                "doc_id": doc_id,
                "page_version": V2,
                "version": safe_version,
                "page_num": page_num,
                "page_summary": summary,
                "content_hash": content_hash,
                "text_length": page.get("text_length", len(summary)),
                "has_tables": page.get("has_tables", False),
                "has_images": page.get("has_images", False),
                "parser": page.get("parser", ""),
                "source": source,
                "migrated_from": page.get("migrated_from", ""),
                "written_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })

        # 同文档同版本页号必须唯一，防止重跑重复计页。
        nums = [r["page_num"] for r in records]
        if len(nums) != len(set(nums)):
            raise ValueError(f"文档 {doc_id} 版本 {safe_version} 存在重复页号: {nums}")

        path = self.versioned_pages_path(doc_id, safe_version)
        with self._doc_lock(doc_id):
            self._write_jsonl_atomic(path, records)
            confirmed_set = self._read_confirmed(doc_id)
            if confirmed:
                confirmed_set.add(safe_version)
            else:
                # 同代内容被替换，旧确认自动失效，必须重新确认才能被消费。
                confirmed_set.discard(safe_version)
            self._write_confirmed(doc_id, confirmed_set)
        return {
            "doc_id": doc_id,
            "version": safe_version,
            "pages": len(records),
            "path": str(path),
            "confirmed": confirmed,
        }

    # ------------------------------------------------------------------ 迁移

    def _v1_to_v2_record(self, doc_id: str, rec: dict[str, Any]) -> dict[str, Any]:
        """把一条无版本 v1 页记录提升为带版本 v2 记录，保留页号与内容摘要用于核对。"""
        page_num = int(rec.get("page_num", 0))
        summary = str(rec.get("page_summary", "")) or (
            f"legacy page {page_num}, paragraphs={rec.get('paragraphs', 0)}, "
            f"text_length={rec.get('text_length', 0)}"
        )
        return {
            "doc_id": doc_id,
            "page_version": V2,
            "version": LEGACY_PARSE_VERSION,
            "page_num": page_num,
            "page_summary": summary[:120],
            "content_hash": rec.get("content_hash")
            or self._compute_checksum(summary + str(page_num)),
            "text_length": rec.get("text_length", len(summary)),
            "has_tables": rec.get("has_tables", False),
            "has_images": rec.get("has_images", False),
            "parser": rec.get("parser", ""),
            "source": "migration",
            "migrated_from": "parsed_v1",
            "written_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def migrate_legacy_pages(self, doc_id: str, *, resume: bool = True) -> dict[str, Any]:
        """把文档的无版本 v1 解析页迁移为带版本 v2 记录（version=v1 的一代页）。

        - 幂等：已迁移/已确认直接回显既有结果，不重复写页；
        - 可中断恢复：中断后状态停在「迁移中」，再次调用从保留的旧页继续；
        - 失败安全：任何页核对失败都不改写旧页、不产出部分 v2 文件，状态置「迁移失败」。
        """
        legacy_path = self.legacy_pages_path(doc_id)
        with self._doc_lock(doc_id):
            state = self._read_json(self.migration_state_path(doc_id))
            if state and state.get("state") in (STATE_MIGRATED, STATE_CONFIRMED) and resume:
                return {**state, "idempotent": True}
            if state and state.get("state") == STATE_RUNNING and not resume:
                return {**state, "message": "迁移仍在进行中"}

            legacy_pages = self._read_jsonl(legacy_path)
            if not legacy_pages:
                fail_state = {
                    "doc_id": doc_id,
                    "state": STATE_FAILED,
                    "reason": f"旧格式解析页不存在或为空: {legacy_path}",
                    "legacy_path": str(legacy_path),
                    "migrated_pages": 0,
                }
                self._save_migration_state(doc_id, fail_state)
                return {**fail_state, "ok": False}

            # 迁移前先逐页核对源记录：页号不重复、内容摘要可计算。
            try:
                v2_records = self._build_migration_records(doc_id, legacy_pages)
            except ValueError as exc:
                fail_state = {
                    "doc_id": doc_id,
                    "state": STATE_FAILED,
                    "reason": str(exc),
                    "legacy_path": str(legacy_path),
                    "migrated_pages": 0,
                }
                self._save_migration_state(doc_id, fail_state)
                return {**fail_state, "ok": False}

            running = {
                "doc_id": doc_id,
                "state": STATE_RUNNING,
                "legacy_path": str(legacy_path),
                "total_pages": len(v2_records),
                "migrated_pages": 0,
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            self._save_migration_state(doc_id, running)

            # 原子整代写入：中途崩溃只会留下旧的 v2 文件或无文件，绝不会出现半代页。
            target = self.versioned_pages_path(doc_id, LEGACY_PARSE_VERSION)
            self._write_jsonl_atomic(target, v2_records)

            # 回读核对：页号、页数、内容摘要与 v1 源记录逐页一致才算迁移成功。
            verify = self._verify_migrated(doc_id, legacy_pages, v2_records)
            if not verify["ok"]:
                target.unlink(missing_ok=True)
                fail_state = {
                    "doc_id": doc_id,
                    "state": STATE_FAILED,
                    "reason": verify["reason"],
                    "legacy_path": str(legacy_path),
                    "migrated_pages": 0,
                }
                self._save_migration_state(doc_id, fail_state)
                return {**fail_state, "ok": False}

            done_state = {
                "doc_id": doc_id,
                "state": STATE_MIGRATED,
                "legacy_path": str(legacy_path),
                "v2_path": str(target),
                "total_pages": len(v2_records),
                "migrated_pages": len(v2_records),
                "page_nums": [r["page_num"] for r in v2_records],
                "page_checksums": {str(r["page_num"]): r["content_hash"] for r in v2_records},
            }
            self._save_migration_state(doc_id, done_state)
            return {**done_state, "ok": True}

    def _build_migration_records(
        self, doc_id: str, legacy_pages: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        nums = [int(p.get("page_num", 0)) for p in legacy_pages]
        if len(nums) != len(set(nums)):
            raise ValueError(f"v1 源页存在重复页号，拒绝迁移: {nums}")
        if any(n <= 0 for n in nums):
            raise ValueError(f"v1 源页存在非法页号: {nums}")
        return [self._v1_to_v2_record(doc_id, p) for p in legacy_pages]

    def _verify_migrated(
        self,
        doc_id: str,
        legacy_pages: list[dict[str, Any]],
        expected: list[dict[str, Any]],
    ) -> dict[str, Any]:
        actual = self._read_jsonl(self.versioned_pages_path(doc_id, LEGACY_PARSE_VERSION))
        if len(actual) != len(legacy_pages):
            return {"ok": False, "reason": "迁移后页数与 v1 源记录不一致"}
        actual_by_num = {int(p["page_num"]): p for p in actual}
        for src, exp in zip(legacy_pages, expected):
            num = int(src["page_num"])
            got = actual_by_num.get(num)
            if got is None:
                return {"ok": False, "reason": f"缺少页号 {num}"}
            if got.get("content_hash") != exp["content_hash"]:
                return {"ok": False, "reason": f"页 {num} 内容摘要与源记录不符"}
            if got.get("page_version") != V2 or got.get("version") != LEGACY_PARSE_VERSION:
                return {"ok": False, "reason": f"页 {num} 版本标记不正确"}
        return {"ok": True}

    # ------------------------------------------------------------------ 确认

    def confirm_version(self, doc_id: str, version: str) -> dict[str, Any]:
        """确认一代解析页可作为下游（分块切分）输入；未确认补页永远不会被自动消费。"""
        safe_version = self._safe_version(version)
        path = self.versioned_pages_path(doc_id, safe_version)
        with self._doc_lock(doc_id):
            pages = self._read_jsonl(path)
            if not pages:
                return {"ok": False, "error": f"版本 {safe_version} 的解析页不存在: {path}"}
            confirmed = self._read_confirmed(doc_id)
            confirmed.add(safe_version)
            self._write_confirmed(doc_id, confirmed)

            # v1 迁移代确认时联动迁移台账，保留完整审计轨迹。
            state = self._read_json(self.migration_state_path(doc_id))
            if safe_version == LEGACY_PARSE_VERSION and state and state.get("state") == STATE_MIGRATED:
                state["state"] = STATE_CONFIRMED
                state["confirmed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                self._save_migration_state(doc_id, state)
        return {
            "ok": True,
            "doc_id": doc_id,
            "version": safe_version,
            "confirmed_versions": sorted(confirmed),
            "pages": len(pages),
        }

    def confirmed_versions(self, doc_id: str) -> list[str]:
        return sorted(self._read_confirmed(doc_id))

    def read_versioned_pages(self, doc_id: str, version: str) -> list[dict[str, Any]]:
        """按版本读取 v2 解析页记录（带版本的标准读取路径）。"""
        return self._read_jsonl(self.versioned_pages_path(doc_id, self._safe_version(version)))

    def read_legacy_pages(self, doc_id: str) -> list[dict[str, Any]]:
        """旧格式 v1 解析页的可复查读取路径，迁移后原文件保持只读不动。"""
        return self._read_jsonl(self.legacy_pages_path(doc_id))

    def resolve_consumption_input(
        self, doc_id: str, version: str | None = None
    ) -> dict[str, Any]:
        """为分块切分解析出本次任务确认可用的输入页组。

        - 显式指定版本：必须存在且已确认，否则报错，绝不静默换成另一代页；
        - 未指定版本：恰好一个已确认版本时自动选中；零个或多个已确认版本都报错，
          防止迁移切换时掺入未确认补页或在两代已确认页之间随机选择；
        - 返回值带完整页清单与每页摘要，写入分块产物后可逐页复查。
        """
        versions = self.list_versions(doc_id)
        confirmed_map = {v["version"]: v for v in versions if v["confirmed"]}

        if version is not None:
            safe = self._safe_version(version)
            if safe not in confirmed_map:
                existing = [v["version"] for v in versions]
                return {
                    "error": f"版本 {safe} 未确认或不存在，不能作为切分输入",
                    "doc_id": doc_id,
                    "requested_version": safe,
                    "available_versions": existing,
                    "confirmed_versions": sorted(confirmed_map),
                }
            chosen = safe
        else:
            if not confirmed_map:
                return {
                    "error": f"文档 {doc_id} 没有已确认的解析页版本，拒绝在未确认输入上切分",
                    "doc_id": doc_id,
                    "available_versions": [v["version"] for v in versions],
                }
            if len(confirmed_map) > 1:
                return {
                    "error": "存在多个已确认版本，必须显式指定切分输入版本",
                    "doc_id": doc_id,
                    "confirmed_versions": sorted(confirmed_map),
                }
            chosen = next(iter(confirmed_map))

        pages = self.read_versioned_pages(doc_id, chosen)
        return {
            "ok": True,
            "doc_id": doc_id,
            "version": chosen,
            "page_version": V2,
            "pages": pages,
            "page_count": len(pages),
            "page_nums": [p["page_num"] for p in pages],
            "source_path": str(self.versioned_pages_path(doc_id, chosen)),
        }

    def inspect_pages(self, doc_id: str) -> dict[str, Any]:
        """接手人视角的一页查：文档有哪几代页、各自确认状态、迁移台账与旧页保留情况。"""
        state = self.get_migration_state(doc_id)
        return {
            "doc_id": doc_id,
            "legacy": {
                "path": str(self.legacy_pages_path(doc_id)),
                "exists": self.legacy_pages_path(doc_id).exists(),
                "pages": len(self.read_legacy_pages(doc_id)),
            },
            "migration": state,
            "confirmed_versions": self.confirmed_versions(doc_id),
            "versions": self.list_versions(doc_id),
        }
