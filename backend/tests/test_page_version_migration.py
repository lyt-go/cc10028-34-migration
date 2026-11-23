"""解析页 v1→v2 迁移与分块切分版本化消费的全链路验证。

用同一文档的两代页（迁移来的 v1 代 + 新解析环境补出的 v2/v3 代）验证：
1. 无版本 v1 旧页迁移为带版本 v2 记录，页号/摘要可与源记录逐页核对；
2. 迁移失败/中断可从保留的旧页继续，旧页文件始终不变；
3. 老任务恢复与新任务并发读取时，能查出每批页的版本与确认状态；
4. 切分输入只接受已确认代次，未确认补页不会掺入，重跑不重复计页；
5. v1 既有成品不变、v2 产物可读，且能复查一次切分实际使用的页清单。
"""
import json
import os
import sys
import threading
import uuid

import pytest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from app.services.base import DATA_ROOT
from app.services.chunk_split import ChunkSplitService
from app.services.format_parse import FormatParseService
from app.services.page_version import (
    LEGACY_PARSE_VERSION,
    V1,
    V2,
    PageVersionService,
)


def _uid(prefix="DOC"):
    return f"{prefix}-{uuid.uuid4().hex[:10].upper()}"


def _make_legacy_doc(doc_id, page_nums=(1, 2, 3)):
    """构造升级前已落盘、尚未切分的无版本 v1 解析页（模拟老数据）。"""
    svc = FormatParseService()
    records = []
    for num in page_nums:
        text = f"legacy {doc_id} page {num} 内容摘要"
        records.append({
            "doc_id": doc_id,
            "page_num": num,
            "paragraphs": 4,
            "text_length": len(text),
            "has_tables": num % 2 == 1,
            "has_images": num % 2 == 0,
            "parser": "PDF",
            "content_hash": svc._compute_checksum(text),
            "page_summary": text,
        })
    path = DATA_ROOT / "parsed" / f"{doc_id}_parsed.jsonl"
    svc._write_jsonl_atomic(path, records)
    return path, records


class TestLegacyMigration:
    """v1 无版本页 → v2 带版本记录迁移。"""

    def test_migration_copies_pages_with_version_and_keeps_legacy(self):
        doc_id = _uid()
        legacy_path, legacy_records = _make_legacy_doc(doc_id)
        svc = PageVersionService()

        result = svc.migrate_legacy_pages(doc_id)

        assert result["ok"] is True
        assert result["state"] == "已迁移"
        assert result["total_pages"] == len(legacy_records)
        assert result["page_nums"] == [1, 2, 3]

        # v2 记录带版本与格式标记，页号/摘要/校验值与 v1 源记录逐页一致。
        v2_pages = svc.read_versioned_pages(doc_id, LEGACY_PARSE_VERSION)
        assert len(v2_pages) == len(legacy_records)
        for src, dst in zip(legacy_records, v2_pages):
            assert dst["page_version"] == V2
            assert dst["version"] == V1
            assert dst["page_num"] == src["page_num"]
            assert dst["content_hash"] == src["content_hash"]
            assert dst["page_summary"] == src["page_summary"]
            assert dst["migrated_from"] == "parsed_v1"

        # 旧格式文件原样保留，仍是可复查的读取路径。
        assert svc.read_legacy_pages(doc_id) == legacy_records
        inspect = svc.inspect_pages(doc_id)
        assert inspect["legacy"]["exists"] is True
        assert inspect["legacy"]["pages"] == 3
        assert {v["version"] for v in inspect["versions"]} == {V1}

    def test_migration_is_idempotent(self):
        doc_id = _uid()
        _make_legacy_doc(doc_id)
        svc = PageVersionService()

        first = svc.migrate_legacy_pages(doc_id)
        second = svc.migrate_legacy_pages(doc_id)

        assert first["ok"] and second.get("idempotent") is True
        assert second["migrated_pages"] == first["migrated_pages"]
        v2_path = svc.versioned_pages_path(doc_id, LEGACY_PARSE_VERSION)
        assert len(svc._read_jsonl(v2_path)) == 3

    def test_migration_failure_keeps_legacy_and_allows_retry(self):
        doc_id = _uid()
        legacy_path, records = _make_legacy_doc(doc_id)
        svc = PageVersionService()

        # 旧页缺失：迁移失败，但返回可定位的原因，不产生半成品。
        missing_doc = _uid("MISS")
        fail = svc.migrate_legacy_pages(missing_doc)
        assert fail["ok"] is False and fail["state"] == "迁移失败"
        assert not svc.versioned_pages_path(missing_doc, LEGACY_PARSE_VERSION).exists()

        # 重复页号的脏 v1 数据：拒绝迁移，旧文件不动。
        dirty = _uid("DIRTY")
        dirty_path, _ = _make_legacy_doc(dirty, page_nums=(1, 1, 2))
        before = dirty_path.read_bytes()
        dirty_result = svc.migrate_legacy_pages(dirty)
        assert dirty_result["ok"] is False
        assert "重复页号" in dirty_result["reason"]
        assert dirty_path.read_bytes() == before
        assert not svc.versioned_pages_path(dirty, LEGACY_PARSE_VERSION).exists()

        # 修复源数据后可从保留的旧页继续迁移。
        fixed_path, fixed_records = _make_legacy_doc(dirty, page_nums=(1, 2, 3))
        retry = svc.migrate_legacy_pages(dirty)
        assert retry["ok"] is True
        assert retry["state"] == "已迁移"
        assert len(svc.read_versioned_pages(dirty, LEGACY_PARSE_VERSION)) == 3

    def test_interrupted_migration_resumes_from_retained_legacy(self):
        doc_id = _uid()
        legacy_path, records = _make_legacy_doc(doc_id)
        svc = PageVersionService()

        # 模拟上次迁移中断：台账停在「迁移中」且没有 v2 产物。
        svc._save_migration_state(doc_id, {
            "doc_id": doc_id,
            "state": "迁移中",
            "legacy_path": str(legacy_path),
            "total_pages": 3,
            "migrated_pages": 0,
        })
        resumed = svc.migrate_legacy_pages(doc_id)
        assert resumed["ok"] is True
        assert resumed["state"] == "已迁移"
        assert resumed["migrated_pages"] == 3
        # 旧页始终未被改写。
        assert svc.read_legacy_pages(doc_id) == records


class TestTwoGenerationsAndConfirmation:
    """同一文档两代页：迁移代与新解析环境补页并存时的确认与查询。"""

    def _two_generation_doc(self):
        doc_id = _uid()
        _make_legacy_doc(doc_id, page_nums=(1, 2, 3))
        page_svc = PageVersionService()
        page_svc.migrate_legacy_pages(doc_id)
        # 新解析环境对同一文档生成不同版本的补页（页号 4、5，默认未确认）。
        FormatParseService().produce_supplemental_pages(doc_id, "v2", page_nums=[4, 5])
        return doc_id, page_svc

    def test_inspect_distinguishes_two_page_batches(self):
        doc_id, page_svc = self._two_generation_doc()
        info = page_svc.inspect_pages(doc_id)

        versions = {v["version"]: v for v in info["versions"]}
        assert set(versions) == {V1, V2}
        assert versions[V1]["page_nums"] == [1, 2, 3]
        assert versions[V2]["page_nums"] == [4, 5]
        # 补页刚生成时两代都未确认，接手人能直接查出确认状态。
        assert versions[V1]["confirmed"] is False
        assert versions[V2]["confirmed"] is False
        assert info["confirmed_versions"] == []

    def test_unconfirmed_supplemental_pages_never_enter_split_input(self):
        doc_id, page_svc = self._two_generation_doc()
        chunk_svc = ChunkSplitService()

        # 没有任何已确认版本：拒绝切分，而不是随便抓一批页。
        refused = chunk_svc.split_versioned_document(doc_id)
        assert "error" in refused
        assert "已确认" in refused["error"]

        # 显式点名未确认的补页版本同样被拒绝，避免静默换批。
        refused_v2 = chunk_svc.split_versioned_document(doc_id, version=V2)
        assert "error" in refused_v2
        assert refused_v2["requested_version"] == V2

    def test_confirm_then_split_uses_only_confirmed_page_set(self):
        doc_id, page_svc = self._two_generation_doc()
        # 只确认迁移来的 v1 代，补页 v2 仍未确认。
        confirm = page_svc.confirm_version(doc_id, V1)
        assert confirm["ok"] is True
        assert page_svc.confirmed_versions(doc_id) == [V1]
        # 迁移台账联动到「已确认」。
        assert page_svc.get_migration_state(doc_id)["state"] == "已确认"

        chunk_svc = ChunkSplitService()
        run_id = f"RUN-{doc_id}-FIRST"
        result = chunk_svc.split_versioned_document(doc_id, run_id=run_id)

        assert "error" not in result, result
        assert result["version"] == V1
        assert result["page_version"] == V2  # 记录格式是带版本的 v2
        assert result["page_nums"] == [1, 2, 3]  # 未确认补页 4、5 没有掺入

        chunks = chunk_svc._read_jsonl(DATA_ROOT / "chunks" / f"{doc_id}__v1_chunks.jsonl")
        assert chunks and all(c["version"] == V1 and c["page_version"] == V2 for c in chunks)
        assert {c["page"] for c in chunks} <= {1, 2, 3}

        manifest = chunk_svc.read_chunk_manifest(run_id)
        assert manifest["version"] == V1
        assert [p["page_num"] for p in manifest["pages"]] == [1, 2, 3]
        # 清单保留每页来源摘要与校验值，可回溯到迁移的 v1 源页。
        for page in manifest["pages"]:
            assert page["content_hash"] and page["page_summary"]

    def test_rerun_same_run_is_idempotent_and_does_not_double_count(self):
        doc_id, page_svc = self._two_generation_doc()
        page_svc.confirm_version(doc_id, V1)
        chunk_svc = ChunkSplitService()
        run_id = f"RUN-{doc_id}-RERUN"

        first = chunk_svc.split_versioned_document(doc_id, run_id=run_id)
        chunk_file = DATA_ROOT / "chunks" / f"{doc_id}__v1_chunks.jsonl"
        mtime_before = chunk_file.stat().st_mtime_ns
        second = chunk_svc.split_versioned_document(doc_id, run_id=run_id)

        assert second.get("idempotent") is True
        assert second["chunks_count"] == first["chunks_count"]
        assert chunk_file.stat().st_mtime_ns == mtime_before  # 重跑没有重写产物
        manifest = chunk_svc.read_chunk_manifest(run_id)
        assert manifest["chunks_count"] == first["chunks_count"]

    def test_confirmed_v2_split_uses_supplemental_pages_with_explicit_version(self):
        doc_id, page_svc = self._two_generation_doc()
        page_svc.confirm_version(doc_id, V1)
        page_svc.confirm_version(doc_id, V2)
        chunk_svc = ChunkSplitService()

        # 两代都已确认时，不指定版本必须报错，禁止随机选批。
        ambiguous = chunk_svc.split_versioned_document(doc_id)
        assert "error" in ambiguous and "显式指定" in ambiguous["error"]

        # 显式选择 v2 补页代，切分只看到页 4、5。
        result = chunk_svc.split_versioned_document(doc_id, version=V2, run_id=f"RUN-{doc_id}-V2")
        assert result["version"] == V2
        assert result["page_nums"] == [4, 5]
        chunks = chunk_svc._read_jsonl(DATA_ROOT / "chunks" / f"{doc_id}__v2_chunks.jsonl")
        assert {c["page"] for c in chunks} == {4, 5}
        assert all(c["version"] == V2 for c in chunks)

    def test_rewriting_a_version_resets_confirmation(self):
        doc_id, page_svc = self._two_generation_doc()
        page_svc.confirm_version(doc_id, V2)
        assert V2 in page_svc.confirmed_versions(doc_id)

        # 同代内容被重新解析覆盖，旧确认必须失效，防止「新内容套旧确认」。
        FormatParseService().produce_supplemental_pages(doc_id, "v2", page_nums=[4, 5, 6])
        assert page_svc.confirmed_versions(doc_id) == []
        refused = ChunkSplitService().split_versioned_document(doc_id, version=V2)
        assert "error" in refused


class TestLegacyArtifactsAndReadPath:
    """既有 v1 成品不变、旧格式读取路径可复查。"""

    def test_legacy_split_path_unchanged_after_upgrade(self):
        doc_id = _uid()
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])

        legacy_file = DATA_ROOT / "parsed" / f"{doc_id}_parsed.jsonl"
        before = legacy_file.read_bytes()
        result = ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        assert "error" not in result
        assert result["chunks"] > 0
        # 旧路径产物文件名/位置保持升级前约定。
        chunk_file = DATA_ROOT / "chunks" / f"{doc_id}_parsed_chunks.jsonl"
        assert chunk_file.exists()
        chunks = ChunkSplitService()._read_jsonl(chunk_file)
        assert all(c["page_version"] == V1 and c["version"] == V1 for c in chunks)
        # 迁移/切分都不改写旧解析页。
        PageVersionService().migrate_legacy_pages(doc_id)
        assert legacy_file.read_bytes() == before

    def test_legacy_read_path_still_available_after_migration(self):
        doc_id = _uid()
        _, legacy_records = _make_legacy_doc(doc_id)
        svc = PageVersionService()
        svc.migrate_legacy_pages(doc_id)
        svc.confirm_version(doc_id, V1)

        # 老任务恢复走旧格式读取路径，内容与迁移前完全一致。
        legacy_input = ChunkSplitService().read_legacy_input(f"{doc_id}_parsed")
        assert [r["page_num"] for r in legacy_input] == [r["page_num"] for r in legacy_records]
        assert legacy_input == legacy_records


class TestConcurrentReaders:
    """老任务恢复与新任务执行并发读取两批输入。"""

    def test_concurrent_migration_and_inspection_is_serialized(self):
        doc_id = _uid()
        _make_legacy_doc(doc_id)
        svc = PageVersionService()
        errors = []

        def migrate():
            try:
                svc.migrate_legacy_pages(doc_id)
            except Exception as exc:  # pragma: no cover - 并发失败留痕
                errors.append(exc)

        def inspect():
            try:
                for _ in range(20):
                    svc.inspect_pages(doc_id)
                    svc.list_versions(doc_id)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        t1 = threading.Thread(target=migrate)
        t2 = threading.Thread(target=inspect)
        t1.start(); t2.start()
        t1.join(); t2.join()

        assert not errors
        state = svc.get_migration_state(doc_id)
        assert state["state"] == "已迁移"
        assert len(svc.read_versioned_pages(doc_id, V1)) == 3

    def test_concurrent_splits_on_same_run_share_one_manifest(self):
        doc_id = _uid()
        _make_legacy_doc(doc_id, page_nums=(1, 2))
        svc = PageVersionService()
        svc.migrate_legacy_pages(doc_id)
        svc.confirm_version(doc_id, V1)
        chunk_svc = ChunkSplitService()
        run_id = f"RUN-{doc_id}-CONCURRENT"
        results = []
        barrier = threading.Barrier(2)

        def worker():
            barrier.wait()
            results.append(chunk_svc.split_versioned_document(doc_id, run_id=run_id))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all("error" not in r for r in results)
        assert sum(1 for r in results if r.get("idempotent")) == 1
        manifest = chunk_svc.read_chunk_manifest(run_id)
        assert manifest["chunks_count"] == results[0]["chunks_count"]
        assert manifest["page_nums"] == [1, 2]
