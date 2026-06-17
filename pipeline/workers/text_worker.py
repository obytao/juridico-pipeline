"""
text_worker.py - Extração de texto de PDFs para Markdown.

Responsabilidades:
  - Receber o caminho do documento no storage
  - Rodar os extratores Python (pdfplumber / pymupdf)
  - Salvar o .md resultante no storage
  - Atualizar status do documento no banco
  - Disparar callback de normalização

NOTA: Coloque aqui seus scripts existentes de PDF -> Markdown.
Este arquivo é o ponto de integração com o que você já tem.
"""

import os
import hashlib
from pathlib import Path

from celery import shared_task
from loguru import logger

from pipeline.db import get_session
from pipeline.models import Document, DocumentStatus
from pipeline.storage import StorageBackend

storage = StorageBackend()


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=30,
    name="pipeline.workers.text_worker.extract_text_task",
)
def extract_text_task(self, doc_id: str) -> dict:
    """
    Celery task: extrai texto do PDF e salva como .md.

    Args:
        doc_id: ID do documento no banco

    Returns:
        dict com doc_id e caminho do .md gerado
    """
    logger.info(f"[TEXT_WORKER] Iniciando extração de texto para doc {doc_id}")

    with get_session() as session:
        doc = session.get(Document, doc_id)
        if not doc:
            raise ValueError(f"Documento {doc_id} não encontrado")

        doc.status_text = DocumentStatus.PROCESSING
        session.commit()
        pdf_path = doc.original_path
        case_id = doc.case_id

    try:
        # Baixar PDF do storage
        local_pdf = storage.download(pdf_path)

        # Verificar cache por hash do arquivo
        file_hash = _compute_hash(local_pdf)
        cached_md = _check_cache(file_hash)
        if cached_md:
            logger.info(f"[TEXT_WORKER] Cache hit para doc {doc_id}")
            md_content = cached_md
        else:
            # Extrair texto com pdfplumber (mais confiável para documentos tabulares)
            md_content = _extract_with_pdfplumber(local_pdf)

            # Fallback: pymupdf para documentos com layout complexo
            if not md_content or len(md_content.strip()) < 100:
                logger.warning(f"[TEXT_WORKER] pdfplumber retornou pouco conteúdo, tentando pymupdf")
                md_content = _extract_with_pymupdf(local_pdf)

            _save_cache(file_hash, md_content)

        # Salvar .md no storage
        md_path = f"cases/{case_id}/markdown/{doc_id}_text.md"
        storage.save_text(md_path, md_content)

        # Atualizar banco
        with get_session() as session:
            doc = session.get(Document, doc_id)
            doc.text_md_path = md_path
            doc.status_text = DocumentStatus.DONE
            session.commit()

        logger.info(f"[TEXT_WORKER] Texto extraído com sucesso para doc {doc_id} -> {md_path}")
        return {"doc_id": doc_id, "md_path": md_path, "case_id": case_id}

    except Exception as exc:
        logger.error(f"[TEXT_WORKER] Erro ao extrair texto do doc {doc_id}: {exc}")
        with get_session() as session:
            doc = session.get(Document, doc_id)
            doc.status_text = DocumentStatus.FAILED
            session.commit()
        raise self.retry(exc=exc)


def _extract_with_pdfplumber(pdf_path: str) -> str:
    """
    Extrai texto usando pdfplumber.
    Bom para documentos com tabelas (CNIS, decisões, etc).

    SUBSTITUA/COMPLEMENTE com seu extrator existente.
    """
    import pdfplumber

    lines = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, 1):
            lines.append(f"\n## Página {i}\n")

            # Texto normal
            text = page.extract_text()
            if text:
                lines.append(text)

            # Tabelas -> markdown
            for table in page.extract_tables():
                if table:
                    lines.append(_table_to_markdown(table))

    return "\n".join(lines)


def _extract_with_pymupdf(pdf_path: str) -> str:
    """
    Extrai texto usando pymupdf (fitz).
    Melhor para PDFs com layout complexo ou colunas.

    SUBSTITUA/COMPLEMENTE com seu extrator existente.
    """
    import fitz  # pymupdf

    lines = []
    doc = fitz.open(pdf_path)
    for i, page in enumerate(doc, 1):
        lines.append(f"\n## Página {i}\n")
        lines.append(page.get_text("text"))
    doc.close()
    return "\n".join(lines)


def _table_to_markdown(table: list) -> str:
    """Converte tabela do pdfplumber para formato Markdown."""
    if not table or not table[0]:
        return ""

    rows = []
    header = [str(cell or "") for cell in table[0]]
    rows.append("| " + " | ".join(header) + " |")
    rows.append("|" + "---|" * len(header))

    for row in table[1:]:
        cells = [str(cell or "") for cell in row]
        rows.append("| " + " | ".join(cells) + " |")

    return "\n".join(rows) + "\n"


def _compute_hash(file_path: str) -> str:
    """Computa hash SHA256 do arquivo para cache."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _check_cache(file_hash: str) -> str | None:
    """Verifica se já existe extrato em cache para este arquivo."""
    cache_path = Path(f".cache/text/{file_hash}.md")
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8")
    return None


def _save_cache(file_hash: str, content: str):
    """Salva extrato em cache local."""
    cache_dir = Path(".cache/text")
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / f"{file_hash}.md").write_text(content, encoding="utf-8")
