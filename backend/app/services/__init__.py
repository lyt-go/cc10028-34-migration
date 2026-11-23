"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.file_intake import FileIntakeService
from app.services.format_parse import FormatParseService
from app.services.layout_restore import LayoutRestoreService
from app.services.table_extract import TableExtractService
from app.services.ocr_engine import OcrEngineService
from app.services.chunk_split import ChunkSplitService
from app.services.entity_recognize import EntityRecognizeService
from app.services.relation_extract import RelationExtractService
from app.services.term_normalize import TermNormalizeService
from app.services.sensitive_filter import SensitiveFilterService
from app.services.vector_embed import VectorEmbedService
from app.services.kg_build import KgBuildService
from app.services.extract_quality import ExtractQualityService
from app.services.annot_review import AnnotReviewService
from app.services.doc_version import DocVersionService
from app.services.extract_lineage import ExtractLineageService
from app.services.result_deliver import ResultDeliverService
from app.services.parse_retry import ParseRetryService
from app.services.page_version import PageVersionService

SERVICES: dict[str, BaseModuleService] = {
    "file_intake": FileIntakeService(),
    "format_parse": FormatParseService(),
    "layout_restore": LayoutRestoreService(),
    "table_extract": TableExtractService(),
    "ocr_engine": OcrEngineService(),
    "chunk_split": ChunkSplitService(),
    "entity_recognize": EntityRecognizeService(),
    "relation_extract": RelationExtractService(),
    "term_normalize": TermNormalizeService(),
    "sensitive_filter": SensitiveFilterService(),
    "vector_embed": VectorEmbedService(),
    "kg_build": KgBuildService(),
    "extract_quality": ExtractQualityService(),
    "annot_review": AnnotReviewService(),
    "doc_version": DocVersionService(),
    "extract_lineage": ExtractLineageService(),
    "result_deliver": ResultDeliverService(),
    "parse_retry": ParseRetryService(),
    "page_version": PageVersionService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
