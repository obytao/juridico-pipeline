"""
image_worker.py - Extração e análise de imagens de documentos PDF.

Responsabilidades:
  - Isolar imagens do PDF por página
  - Classificar tipo da imagem (carimbo, tabela, assinatura, gráfico, texto)
  - Chamar Claude Vision para analisar imagens relevantes
  - Salvar .md com descrição e relevância jurídica de cada imagem
  - Atualizar status do documento

NOTA: Integra com sua habilidade Claude existente de análise de imagens.
"""

import os
import base64
from pathlib import Path
from typing import List, Dict

from celery import shared_task
from loguru import logger
import fitz  # pymupdf
import anthropic

from pipeline.db import get_session
from pipeline.models import Document, DocumentStatus
from pipeline.storage import StorageBackend

storage = StorageBackend()
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# Tipos de imagem que merecem análise visual completa (custo maior)
HIGH_VALUE_IMAGE_TYPES = [
    "laudo", "carimbo", "assinatura", "tabela", "grafico", "documento_externo"
]


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name="pipeline.workers.image_worker.extract_images_task",
)
def extract_images_task(self, doc_id: str) -> dict:
    """
    Celery task: extrai e analisa imagens do documento.

    Args:
        doc_id: ID do documento no banco

    Returns:
        dict com doc_id, caminho do .md de imagens e case_id
    """
    logger.info(f"[IMAGE_WORKER] Iniciando extração de imagens para doc {doc_id}")

    with get_session() as session:
        doc = session.get(Document, doc_id)
        if not doc:
            raise ValueError(f"Documento {doc_id} não encontrado")

        doc.status_image = DocumentStatus.PROCESSING
        session.commit()
        pdf_path = doc.original_path
        case_id = doc.case_id

    try:
        local_pdf = storage.download(pdf_path)

        # Extrair imagens do PDF
        images = _extract_images_from_pdf(local_pdf, doc_id, case_id)
        logger.info(f"[IMAGE_WORKER] {len(images)} imagens extraídas do doc {doc_id}")

        # Analisar cada imagem relevante com Claude Vision
        analyses = []
        for img_meta in images:
            analysis = _analyze_image_with_claude(img_meta, doc_id, case_id)
            analyses.append(analysis)

        # Montar .md consolidado das imagens
        md_content = _build_images_markdown(analyses)
        md_path = f"cases/{case_id}/markdown/{doc_id}_images.md"
        storage.save_text(md_path, md_content)

        # Atualizar banco
        with get_session() as session:
            doc = session.get(Document, doc_id)
            doc.image_md_path = md_path
            doc.status_image = DocumentStatus.DONE
            session.commit()

        logger.info(f"[IMAGE_WORKER] Imagens analisadas para doc {doc_id} -> {md_path}")
        return {"doc_id": doc_id, "md_path": md_path, "case_id": case_id}

    except Exception as exc:
        logger.error(f"[IMAGE_WORKER] Erro ao processar imagens do doc {doc_id}: {exc}")
        with get_session() as session:
            doc = session.get(Document, doc_id)
            doc.status_image = DocumentStatus.FAILED
            session.commit()
        raise self.retry(exc=exc)


def _extract_images_from_pdf(pdf_path: str, doc_id: str, case_id: str) -> List[Dict]:
    """Extrai todas as imagens do PDF e salva no storage."""
    doc = fitz.open(pdf_path)
    images_meta = []

    for page_num, page in enumerate(doc, 1):
        image_list = page.get_images(full=True)
        for img_idx, img in enumerate(image_list):
            xref = img[0]
            base_img = doc.extract_image(xref)

            img_bytes = base_img["image"]
            img_ext = base_img["ext"]
            img_filename = f"{doc_id}_p{page_num}_i{img_idx}.{img_ext}"
            img_storage_path = f"cases/{case_id}/images/{img_filename}"

            storage.save_bytes(img_storage_path, img_bytes)

            images_meta.append({
                "image_id": img_filename,
                "doc_id": doc_id,
                "page": page_num,
                "storage_path": img_storage_path,
                "bytes": img_bytes,
                "ext": img_ext,
            })

    doc.close()
    return images_meta


def _analyze_image_with_claude(img_meta: Dict, doc_id: str, case_id: str) -> Dict:
    """
    Chama Claude Vision para analisar uma imagem.
    Usa sua habilidade existente de análise de imagens.

    Retorna dict com: tipo, texto_ocr, descrição, relevância_jurídica, confiança.
    """
    try:
        img_b64 = base64.standard_b64encode(img_meta["bytes"]).decode("utf-8")
        media_type = f"image/{img_meta['ext']}"
        if img_meta["ext"] == "jpg":
            media_type = "image/jpeg"

        response = client.messages.create(
            model=os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-5"),
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": img_b64,
                            },
                        },
                        {
                            "type": "text",
                            "text": (
                                "Analise esta imagem extraida de um documento jurídico previdenciário brasileiro. "
                                "Responda SOMENTE em JSON com os campos:\n"
                                "- kind: tipo da imagem (carimbo|assinatura|tabela|grafico|foto|texto|outro)\n"
                                "- ocr_text: texto legível na imagem (string)\n"
                                "- visual_summary: descrição objetiva do que a imagem mostra\n"
                                "- legal_relevance: relevância jurídica da imagem para um processo previdenciário\n"
                                "- confidence: sua confiança na análise de 0.0 a 1.0"
                            ),
                        },
                    ],
                }
            ],
        )

        import json
        content = response.content[0].text
        # Limpar possíveis blocos de código
        content = content.strip().strip("```json").strip("```").strip()
        analysis = json.loads(content)
        analysis["image_id"] = img_meta["image_id"]
        analysis["page"] = img_meta["page"]
        analysis["storage_path"] = img_meta["storage_path"]
        return analysis

    except Exception as e:
        logger.warning(f"[IMAGE_WORKER] Falha ao analisar imagem {img_meta['image_id']}: {e}")
        return {
            "image_id": img_meta["image_id"],
            "page": img_meta["page"],
            "storage_path": img_meta["storage_path"],
            "kind": "desconhecido",
            "ocr_text": "",
            "visual_summary": "Não foi possível analisar esta imagem.",
            "legal_relevance": "Indeterminada",
            "confidence": 0.0,
        }


def _build_images_markdown(analyses: List[Dict]) -> str:
    """Monta o .md consolidado com todas as análises de imagens."""
    lines = ["# Imagens Extraídas do Documento\n"]

    for a in analyses:
        lines.append(f"## Imagem: {a.get('image_id', '?')} (Página {a.get('page', '?')})")
        lines.append(f"- **Tipo:** {a.get('kind', '?')}")
        lines.append(f"- **Confiança:** {a.get('confidence', 0):.0%}")
        lines.append(f"- **Relevância Jurídica:** {a.get('legal_relevance', '')}")
        lines.append(f"- **Descrição:** {a.get('visual_summary', '')}")
        if a.get("ocr_text"):
            lines.append(f"\n**Texto na Imagem:**\n```\n{a['ocr_text']}\n```")
        lines.append("")

    return "\n".join(lines)
