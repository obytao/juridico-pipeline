"""
json_worker.py - Gera a ficha do caso (case_sheet.json) via LLM.

*** ESTE É O ÚNCO PONTO DA PIPELINE QUE USA IA PARA EXTRAÇÃO ***

Fluxo:
  1. Consolida todos os .md (texto + imagens) do caso
  2. Chama Claude com structured output para gerar o JSON da ficha
  3. Valida o JSON gerado contra o schema (Pydantic)
  4. Salva no banco e no storage
  5. Identifica campos de baixa confiança para revisão humana
"""

import os
import json
from typing import List

from celery import shared_task
from loguru import logger
import anthropic

from pipeline.db import get_session
from pipeline.models import Case, Document, CaseStatus
from pipeline.storage import StorageBackend
from pipeline.schemas import CaseSheet

storage = StorageBackend()
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.80"))


@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=60,
    name="pipeline.workers.json_worker.generate_case_sheet_task",
)
def generate_case_sheet_task(self, case_id: str) -> dict:
    """
    Celery task: gera a ficha do caso em JSON via Claude.

    Args:
        case_id: ID do caso

    Returns:
        dict com case_id e caminho do JSON gerado
    """
    logger.info(f"[JSON_WORKER] Gerando ficha do caso {case_id}")

    with get_session() as session:
        case = session.get(Case, case_id)
        docs = session.query(Document).filter(Document.case_id == case_id).all()
        case.status = CaseStatus.AWAITING_JSON
        session.commit()

    try:
        # 1. Consolidar todos os .md do caso
        consolidated_md = _consolidate_documents(docs)
        logger.info(f"[JSON_WORKER] Consolidated {len(docs)} documentos ({len(consolidated_md)} chars)")

        # 2. Chamar Claude para extrair a ficha estruturada
        case_sheet_raw = _call_llm_for_case_sheet(consolidated_md, case_id)

        # 3. Validar com Pydantic
        case_sheet = CaseSheet.model_validate(case_sheet_raw)
        case_sheet_dict = case_sheet.model_dump()

        # 4. Salvar JSON
        json_path = f"cases/{case_id}/case_sheet.json"
        storage.save_text(json_path, json.dumps(case_sheet_dict, ensure_ascii=False, indent=2))

        # 5. Identificar campos de baixa confiança
        low_confidence = _find_low_confidence_fields(case_sheet_dict)
        if low_confidence:
            logger.warning(
                f"[JSON_WORKER] Campos de baixa confiança no caso {case_id}: {low_confidence}. "
                f"Revisão humana recomendada."
            )

        # 6. Atualizar status
        with get_session() as session:
            case = session.get(Case, case_id)
            case.status = CaseStatus.JSON_GENERATED
            case.case_sheet_path = json_path
            case.low_confidence_fields = low_confidence
            session.commit()

        logger.info(f"[JSON_WORKER] Ficha gerada com sucesso para caso {case_id}")
        return {"case_id": case_id, "json_path": json_path, "low_confidence": low_confidence}

    except Exception as exc:
        logger.error(f"[JSON_WORKER] Erro ao gerar ficha do caso {case_id}: {exc}")
        with get_session() as session:
            case = session.get(Case, case_id)
            case.status = CaseStatus.FAILED
            session.commit()
        raise self.retry(exc=exc)


def _consolidate_documents(docs: List) -> str:
    """
    Junta todos os .md (texto + imagens) de todos os documentos do caso
    em um único payload de contexto para o LLM.
    """
    parts = []
    for doc in docs:
        parts.append(f"\n\n---\n# Documento: {doc.id}\n")

        if doc.text_md_path:
            try:
                text_content = storage.load_text(doc.text_md_path)
                parts.append(f"## Conteúdo Textual\n{text_content}")
            except Exception as e:
                parts.append(f"## Conteúdo Textual\n[Erro ao carregar: {e}]")

        if doc.image_md_path:
            try:
                image_content = storage.load_text(doc.image_md_path)
                parts.append(f"## Análise de Imagens\n{image_content}")
            except Exception as e:
                parts.append(f"## Análise de Imagens\n[Erro ao carregar: {e}]")

    return "\n".join(parts)


def _call_llm_for_case_sheet(consolidated_md: str, case_id: str) -> dict:
    """
    Chama Claude para extrair a ficha do caso em JSON estruturado.
    USA STRUCTURED OUTPUT para garantir formato correto.
    """
    system_prompt = """Você é um assistente jurídico especializado em direito previdenciário brasileiro.
    Sua tarefa é analisar documentos de um caso e extrair os dados estruturados para preencher
    uma ficha do caso em formato JSON.

    REGRAS FUNDAMENTAIS:
    - Extraia APENAS informações que existem nos documentos. NUNCA invente fatos.
    - Se uma informação não estiver clara, use null e confidence baixo (< 0.5).
    - Inclua sempre a fonte (document_id, página, trecho) para cada fato extraído.
    - Identifique conflitos entre documentos e registre em 'conflicts'.
    - Responda SOMENTE com o JSON válido, sem texto adicional."""

    user_prompt = f"""Analise os seguintes documentos do caso {case_id} e gere a ficha do caso:

{consolidated_md}

Gere o JSON da ficha do caso seguindo exatamente o schema CaseSheet definido no sistema.
    """

    response = client.messages.create(
        model=os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-5"),
        max_tokens=4096,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    content = response.content[0].text.strip()
    # Limpar possíveis markdown code blocks
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
        content = content.strip()

    return json.loads(content)


def _find_low_confidence_fields(case_sheet: dict, prefix: str = "") -> List[str]:
    """Identifica recursivamente campos com confidence abaixo do threshold."""
    low = []
    for key, value in case_sheet.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if key == "confidence" and isinstance(value, float):
            if value < CONFIDENCE_THRESHOLD:
                low.append(prefix)
        elif isinstance(value, dict):
            low.extend(_find_low_confidence_fields(value, full_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    low.extend(_find_low_confidence_fields(item, f"{full_key}[{i}]"))
    return low
