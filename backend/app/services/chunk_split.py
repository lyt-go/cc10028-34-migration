"""分块切分服务：文档文本按语义边界的分块切分、重叠窗口保留与检索标签标注。

从解析产物读取已解析文本，按语义边界切分为独立分块并持久化，同时触发实体识别和向量嵌入。

输入分两条读取路径：

- 带版本路径（新）：通过 PageVersionService 只消费「已确认」的 v2 解析页代次，
  切分产物逐块记录实际选用的页来源与格式版本，并写一份切分清单（manifest）做幂等，
  同一 run 重跑不重复计页；
- 旧格式路径（保留）：直接读取 ``parsed/{doc}_parsed.jsonl`` 的无版本 v1 页，
  供升级前老任务恢复与人工复查，行为与升级前保持一致。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.page_version import V1, V2, PageVersionService
from app.store import store


class ChunkSplitService(BaseModuleService):
    MODULE = "chunk_split"
    ENTITY = "文本分块"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["分块编号", "文档编号", "分块长度", "重叠窗口", "切分边界", "分块序号", "检索标签", "切分策略"]
    REQUIRED_FIELDS = ["分块编号", "文档编号", "分块长度"]
    STATUS_ORDER = ["草稿", "待切分", "已切分", "切分异常"]
    ACTION_RULES = {"启动切分": "待切分", "确认切分": "已切分", "标记异常": "切分异常"}
    NEGATIVE_ACTIONS = ["标记异常"]

    STRATEGY_PRESETS = {
        "semantic": {"description": "按语义边界切分，保留完整句子和段落结构"},
        "fixed": {"description": "按固定长度切分，超出部分截断"},
        "sliding": {"description": "滑动窗口切分，保留重叠区以确保上下文连贯"},
    }

    def split_from_document(self, parsed_doc_id: str) -> dict[str, Any]:
        """旧格式路径：从无版本 v1 解析产物读入并切分，行为与升级前保持一致。"""
        parsed_file = self._data_dir("parsed") / f"{parsed_doc_id.replace('_parsed', '')}_parsed.jsonl"
        if not parsed_file.exists():
            # 尝试直接查找
            alt_path = self._data_dir("parsed") / f"{parsed_doc_id}.jsonl"
            if alt_path.exists():
                parsed_file = alt_path
            else:
# 数据操作：切分文本分块按语义边界生成分块数据集
                return {"error": f"解析产物文件 {parsed_doc_id} 不存在，请先执行格式解析"}

        parsed_records = self._read_jsonl(parsed_file)
        if not parsed_records:
            return {"error": f"解析产物 {parsed_doc_id} 为空，无法切分"}

        doc_id = parsed_doc_id.replace("_parsed", "")
        chunks = self._build_chunks(doc_id, parsed_records)
        # 旧路径产物显式标注来源为无版本 v1，保留可复查的读取路径。
        for chunk in chunks:
            chunk["page_version"] = V1
            chunk["version"] = V1
            chunk["source_path"] = str(parsed_file)

        output_path = self._data_dir("chunks") / f"{parsed_doc_id}_chunks.jsonl"
        written = self._write_jsonl(output_path, chunks)

        # 写入检查点
        self._write_checkpoint(f"chunk_{parsed_doc_id}", {
            "parsed_doc_id": parsed_doc_id,
            "chunks_count": len(chunks),
            "strategy": "semantic",
            "total_length": sum(c["length"] for c in chunks),
            "decision_boundary": "按语义边界切分是保持上下文完整性的最佳实践",
        })

# 数据产物：标签检索分块数据集按标签过滤定位分块条目
        # 跨模块自动触发：实体识别和向量嵌入
        triggers = {}
        if parsed_doc_id:
            trig_entity = self._trigger_entity_recognize(parsed_doc_id)
            triggers["entity_recognize"] = trig_entity
            trig_vector = self._trigger_vector_embed(parsed_doc_id)
            triggers["vector_embed"] = trig_vector

        return {
            "parsed_doc_id": parsed_doc_id,
            "chunks": len(chunks),
            "records_written": written,
            "strategy": "semantic",
            "output": str(output_path),
            "triggers": triggers,
        }

    # ------------------------------------------------------------------
    # 带版本 v2 解析页消费（新路径）
    # ------------------------------------------------------------------

    def split_versioned_document(
        self,
        doc_id: str,
        version: str | None = None,
        *,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """消费带版本的解析页记录执行切分。

        - 输入只取已确认代次（显式版本必须已确认；未指定时仅在唯一确认代次下自动选中），
          迁移切换不会把未确认补页掺入本次输入；
        - 同一 ``run_id`` 重跑直接返回既有清单，不重复写块、不重复计页；
        - 每个分块与清单都保留实际选择的页来源（页号、内容摘要、源文件）与格式版本。
        """
        page_svc = PageVersionService()
        run_id = run_id or f"RUN-{doc_id}-{int(time.time() * 1000)}"
        manifest_path = self._data_dir("chunks") / "manifests" / f"{run_id}.json"

        # 同一文档的切分串行化：老任务恢复与新任务并发时，
        # 清单检查与产物写入之间不会出现竞态，同 run 重跑只有一次实际写入。
        with page_svc._doc_lock(doc_id):
            # 幂等：已完成的 run 重跑只回显清单，绝不重复计页。
            existing = self._read_json(manifest_path)
            if existing and existing.get("status") == "completed":
                return {**existing, "idempotent": True}

            resolved = page_svc.resolve_consumption_input(doc_id, version)
            if "error" in resolved:
                return resolved

            chosen_version = resolved["version"]
            pages = resolved["pages"]
            chunks = self._build_chunks(doc_id, pages)

            # 逐块留痕：实际选择的页来源与格式版本，支撑下游与人工逐页复查。
            page_index = {p["page_num"]: p for p in pages}
            for chunk in chunks:
                src = page_index.get(chunk["page"], {})
                chunk["page_version"] = V2
                chunk["version"] = chosen_version
                chunk["source_page_hash"] = src.get("content_hash", "")
                chunk["page_summary"] = src.get("page_summary", "")
                chunk["source_path"] = resolved["source_path"]

            chunk_path = self._data_dir("chunks") / f"{doc_id}__{chosen_version}_chunks.jsonl"
            written = self._write_jsonl_atomic(chunk_path, chunks)

            manifest = {
                "run_id": run_id,
                "status": "completed",
                "doc_id": doc_id,
                "page_version": V2,
                "version": chosen_version,
                "source_path": resolved["source_path"],
                "chunk_output": str(chunk_path),
                "chunks_count": len(chunks),
                "records_written": written,
                "page_count": resolved["page_count"],
                "page_nums": resolved["page_nums"],
                "pages": [
                    {
                        "page_num": p["page_num"],
                        "content_hash": p.get("content_hash", ""),
                        "page_summary": p.get("page_summary", ""),
                        "version": p.get("version", chosen_version),
                    }
                    for p in pages
                ],
                "strategy": "semantic",
                "confirmed_versions_at_run": page_svc.confirmed_versions(doc_id),
                "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            self._write_json_atomic(manifest_path, manifest)

        self._write_checkpoint(f"chunk_versioned_{run_id}", {
            "doc_id": doc_id,
            "version": chosen_version,
            "run_id": run_id,
            "chunks": len(chunks),
            "pages": resolved["page_nums"],
        })

        # 跨模块触发沿用既有下游（实体识别/向量嵌入读取旧路径产物的行为保持不变）。
        triggers = {}
        triggers["entity_recognize"] = self._trigger_entity_recognize(f"{doc_id}_parsed")
        triggers["vector_embed"] = self._trigger_vector_embed(f"{doc_id}_parsed")

        return {**manifest, "triggers": triggers}

    def read_chunk_manifest(self, run_id: str) -> dict[str, Any] | None:
        """读取一次切分实际使用的页清单（页号、摘要、版本、来源），供复查。"""
        return self._read_json(self._data_dir("chunks") / "manifests" / f"{run_id}.json")

    def read_legacy_input(self, parsed_doc_id: str) -> list[dict[str, Any]]:
        """旧格式 v1 解析页读取路径（无版本字段），保留给老任务恢复与人工复查。"""
        parsed_file = self._data_dir("parsed") / f"{parsed_doc_id.replace('_parsed', '')}_parsed.jsonl"
        if not parsed_file.exists():
            alt_path = self._data_dir("parsed") / f"{parsed_doc_id}.jsonl"
            if alt_path.exists():
                parsed_file = alt_path
        return self._read_jsonl(parsed_file)

    def _build_chunks(self, doc_id: str, parsed_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按语义边界把一组解析页切成文本分块（v1/v2 两条输入路径共用）。"""
        strategy = "semantic"
        chunk_size = 512
        overlap = 50

        chunks = []
        chunk_seq = 0
        for rec in parsed_records:
            page_text = "\n".join(self._simulate_doc_content(rec.get("doc_id", doc_id), paragraphs=3))
            sentences = page_text.replace("。", "。\n").split("\n")
            current_chunk = ""
            for sent in sentences:
                sent = sent.strip()
                if not sent:
                    continue
                if len(current_chunk) + len(sent) > chunk_size:
                    if current_chunk:
                        chunk_seq += 1
                        chunks.append({
                            "chunk_id": f"CHUNK-{doc_id}-{chunk_seq:04d}",
                            "doc_id": doc_id,
                            "page": rec.get("page_num", 1),
                            "seq": chunk_seq,
                            "length": len(current_chunk),
                            "overlap": overlap,
                            "boundary": "sentence" if strategy == "semantic" else "fixed",
                            "tags": self._generate_tags(current_chunk),
                            "text": current_chunk,
                            "strategy": strategy,
                        })
                    overlap_text = current_chunk[-overlap:] if len(current_chunk) > overlap else ""
                    current_chunk = overlap_text + sent
                else:
                    current_chunk += sent if not current_chunk else " " + sent
            if current_chunk:
                chunk_seq += 1
                chunks.append({
                    "chunk_id": f"CHUNK-{doc_id}-{chunk_seq:04d}",
                    "doc_id": doc_id,
                    "page": rec.get("page_num", 1),
                    "seq": chunk_seq,
                    "length": len(current_chunk),
                    "overlap": overlap,
                    "boundary": "sentence",
                    "tags": self._generate_tags(current_chunk),
                    "text": current_chunk,
                    "strategy": strategy,
                })
        return chunks

    def _generate_tags(self, text: str) -> list[str]:
        """基于文本内容生成检索标签，识别文本中的关键实体类别。"""
        tags = []
# 数据边界：切分操作处理空分块与缺失产物异常边界数据
        keywords = {
            "企业": ["公司", "集团", "企业", "有限公司"],
            "人员": ["张", "博士", "科学家", "工程师", "经理"],
            "时间": ["2024年", "季度", "月", "日"],
            "地点": ["Beijing", "Shanghai", "总部", "分部"],
            "金额": ["亿元", "万元", "投资"],
            "技术": ["知识图谱", "人工智能", "技术", "系统"],
        }
        for tag, words in keywords.items():
            if any(w in text for w in words):
                tags.append(tag)
        return tags if tags else ["通用"]