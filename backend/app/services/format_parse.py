"""格式解析服务：PDF/Word/HTML等格式文档的解析器选择、页码范围切分与输出格式控制。

解析完成后写入已解析文件，自动触发分块切分和版式还原。
"""
from __future__ import annotations

import time
import re
from typing import Any

from app.services.base import BaseModuleService, _ensure_dir
from app.services.page_version import V2
from app.store import store


class FormatParseService(BaseModuleService):
    MODULE = "format_parse"
    ENTITY = "解析任务"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["任务编号", "文档编号", "解析器类型", "页码范围", "解析模式", "输出格式", "超时秒数", "解析器版本"]
    REQUIRED_FIELDS = ["任务编号", "文档编号", "解析器类型"]
    STATUS_ORDER = ["草稿", "待解析", "解析中", "已完成", "解析失败"]
    ACTION_RULES = {"启动解析": "解析中", "标记完成": "已完成", "标记失败": "解析失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    # 支持的解析器类型与对应正则
    PARSER_REGISTRY = {
        "PDF": {"description": "PDF文档解析器，提取文本段落与页面布局"},
        "DOCX": {"description": "Word文档解析器，提取段落样式与层级结构"},
        "HTML": {"description": "HTML文档解析器，提取标签语义与DOM树"},
# 数据产物：从接入批次中批量创建解析任务数据集
        "TXT": {"description": "纯文本文档解析器，按字符集编码读取"},
        "MARKDOWN": {"description": "Markdown解析器，提取标题层级与代码块"},
    }

    def create_from_batch(self, batch_key: str) -> dict[str, Any]:
        """从接入批次创建解析任务，读取批次中的文档清单逐个生成解析任务。"""
        # 读取已校验批次文件
        batch_file = self._data_dir("batches") / "validated.jsonl"
        batches = self._read_jsonl(batch_file)
        matched = [b for b in batches if b.get("batch_key") == batch_key]
        if not matched:
            return {"error": f"批次 {batch_key} 未找到校验记录，可能未通过校验"}
        batch = matched[0]

        batch_rows = [r for r in store.rows("file_intake") if r.get("批次编号") == batch_key]
        if not batch_rows:
            return {"error": f"批次 {batch_key} 未在file_intake登记"}

        batch_row = batch_rows[0]
        file_list = batch_row.get("文件清单", "").split(",")
        file_type = batch_row.get("文件类型", "PDF")
        parser_type = self.PARSER_REGISTRY.get(file_type, self.PARSER_REGISTRY["PDF"])

        created = []
        for i, fname in enumerate(file_list):
            fname = fname.strip()
            if not fname:
                continue
            task_values = {
                "任务编号": f"PARSE-{batch_key}-{i+1:03d}",
                "文档编号": fname,
                "解析器类型": file_type,
                "页码范围": "",
                "解析模式": "full",
                "输出格式": "jsonl",
                "超时秒数": "300",
                "解析器版本": "v2.1.0",
            }
            result, missing = self.create_entry(task_values)
            if result:
                created.append(result["任务编号"])
        return {"batch_key": batch_key, "tasks_created": len(created), "task_ids": created}

    def execute_parse(self, task_id: int) -> dict[str, Any]:
        """执行文档解析任务，将解析产物持久化为JSONL文件，同时触发下游分块切分。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
# 数据操作：解析文档生成已解析页面产物与区域检测
            return {"error": f"解析任务 {task_id} 不存在"}
        if task.get("status") not in ("草稿", "待解析", "解析中"):
            return {"error": f"解析任务 {task_id} 状态不允许执行解析，当前状态: {task.get('status')}"}

        task["status"] = "解析中"
        start_time = time.time()
        doc_id = task["文档编号"]
        parser = task["解析器类型"]

        try:
            # 模拟文档解析
            content = self._simulate_doc_content(doc_id, paragraphs=8)
            pages = self._simulate_pages(content)
            table_regions = self._detect_table_regions(pages)
            image_regions = self._detect_image_regions(pages)

            # 写入解析产物
            output_path = self._data_dir("parsed") / f"{doc_id}_parsed.jsonl"
            parsed_records = []
            for p in pages:
                parsed_records.append({
                    "doc_id": doc_id,
# 数据产物：解析page产物数据写入jsonl文件
                    "page_num": p["page"],
                    "paragraphs": len(p["paragraphs"]),
                    "text_length": p["text_length"],
                    "has_tables": p["has_tables"],
                    "has_images": p["has_images"],
                    "parser": parser,
                    "content_hash": self._compute_checksum("\n".join(p["paragraphs"])),
                })
            written = self._write_jsonl(output_path, parsed_records)

            # 同步登记带版本的 v2 解析页记录。新解析出的代次默认「未确认」，
            # 必须显式确认后才允许分块切分消费，避免补页悄悄掺入已确认输入。
            page_version = str(task.get("页版本") or "v2")
            page_svc = self._safe_import(
                "app.services.page_version", "PageVersionService"
            )()
            versioned = page_svc.write_versioned_pages(
                doc_id,
                page_version,
                [
                    {
                        "page_num": p["page"],
                        "paragraphs": p["paragraphs"],
                        "text_length": p["text_length"],
                        "has_tables": p["has_tables"],
                        "has_images": p["has_images"],
                        "parser": parser,
                        "content_hash": next(
                            r["content_hash"] for r in parsed_records
                            if r["page_num"] == p["page"]
                        ),
                    }
                    for p in pages
                ],
                source="format_parse",
                confirmed=False,
            )

            # 写入表格区域到共享产物，供table_extract读取
            if table_regions:
                table_path = self._data_dir("layout") / f"{doc_id}_tables.jsonl"
                self._write_jsonl(table_path, table_regions)

            # 写入图片区域到共享产物，供ocr_engine读取
            if image_regions:
                image_path = self._data_dir("layout") / f"{doc_id}_images.jsonl"
                self._write_jsonl(image_path, image_regions)

            elapsed = time.time() - start_time
# 数据操作：解析完毕写入检查点，存储数据集至持久化文件
            task["status"] = "已完成"
            task["pending"] = False
            if elapsed > float(task.get("超时秒数") or 300):
                task["abnormal"] = True

            # 写入检查点
            self._write_checkpoint(f"parse_{doc_id}", {
                "task_id": task_id,
                "doc_id": doc_id,
                "pages": len(pages),
                "records_written": written,
                "elapsed": elapsed,
                "table_count": len(table_regions),
                "image_count": len(image_regions),
                "page_version": V2,
                "version": page_version,
                "versioned_pages_path": versioned["path"],
                "version_confirmed": False,
            })

            # 跨模块自动触发
            triggers = {}
            trig_chunk = self._trigger_chunk_split(f"{doc_id}_parsed")
            triggers["chunk_split"] = trig_chunk
            trig_layout = self._trigger_layout_restore(doc_id)
            triggers["layout_restore"] = trig_layout
            trig_quality = self._trigger_quality_check(doc_id)
            triggers["extract_quality"] = trig_quality

            return {
                "task_id": task_id,
                "doc_id": doc_id,
                "pages": len(pages),
                "records_written": written,
# 【数据工程】解析操作完成后触发下游分块数据集消费
                "elapsed": round(elapsed, 2),
                "output": str(output_path),
                "versioned_pages": versioned,
                "triggers": triggers,
            }
        except Exception as e:
            task["status"] = "解析失败"
            task["abnormal"] = True
            return {"task_id": task_id, "error": str(e), "doc_id": doc_id}

    def produce_supplemental_pages(
        self,
        doc_id: str,
        version: str,
        *,
        page_nums: list[int] | None = None,
        parser: str = "PDF",
        paragraphs_per_page: int = 6,
    ) -> dict[str, Any]:
        """同一文档再次解析：生成另一代带版本补页（默认未确认，不覆盖任何既有代次）。"""
        from app.services.page_version import PageVersionService

        page_svc = PageVersionService()
        existing_nums = {
            p["page_num"]
            for v in page_svc.list_versions(doc_id)
            for p in page_svc.read_versioned_pages(doc_id, v["version"])
        }
        if page_nums is None:
            # 默认补一批与既有页号不重叠的新页，模拟增量补页。
            start = (max(existing_nums) if existing_nums else 0) + 1
            page_nums = [start, start + 1]

        pages = []
        for num in page_nums:
            content = self._simulate_doc_content(f"{doc_id}#{version}", paragraphs=paragraphs_per_page)
            text = "\n".join(content)
            pages.append({
                "page_num": num,
                "paragraphs": content,
                "text_length": len(text),
                "has_tables": num % 2 == 1,
                "has_images": num % 2 == 0,
                "parser": parser,
                "content_hash": self._compute_checksum(text),
                "page_summary": f"supplemental {version} page {num}",
            })
        result = page_svc.write_versioned_pages(
            doc_id, version, pages, source="format_parse.supplemental", confirmed=False
        )
        self._write_checkpoint(f"parse_supplemental_{doc_id}_{version}", {
            "doc_id": doc_id,
            "version": version,
            "pages": len(pages),
            "page_nums": page_nums,
            "confirmed": False,
        })
        return result

    def _simulate_pages(self, content: list[str]) -> list[dict[str, Any]]:
        """模拟将文本内容切分为多页文档，每页含段落、表格和图片区域标记。"""
        pages = []
        for page_num in range(1, 4):
            start = (page_num - 1) * (len(content) // 3 + 1)
            end = min(page_num * (len(content) // 3 + 1), len(content))
            page_lines = content[start:end]
            pages.append({
                "page": page_num,
                "paragraphs": page_lines,
                "text_length": sum(len(l) for l in page_lines),
                "has_tables": page_num % 2 == 1,
                "has_images": page_num == 2,
            })
        return pages

    def _detect_table_regions(self, pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """检测页面中的表格区域边界并生成结构化输出标记。"""
        regions = []
        for p in pages:
            if p["has_tables"]:
                regions.append({
                    "page": p["page"],
                    "doc_id": "from_parse",
                    "bbox": f"({p['page']*10}, {p['page']*8}, {p['page']*10+200}, {p['page']*8+80})",
                    "rows": 4,
                    "cols": 5,
                    "header_row": 1,
                    "merged_cells": 2,
                })
        return regions

    def _detect_image_regions(self, pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """检测页面中的图片区域边界并标记识别语言和精度要求。"""
        regions = []
        for p in pages:
            if p["has_images"]:
                regions.append({
                    "page": p["page"],
                    "doc_id": "from_parse",
                    "bbox": f"({p['page']*5}, {p['page']*4+1}, {p['page']*5+150}, {p['page']*4+151})",
                    "language": "chinese",
                    "precision": "high",
                })
        return regions