"""
template_worker.py - Preenche o modelo da peça jurídica a partir do case_sheet.json.

Estratégia Híbrida:
  1. Campos DETERMINÍSTICOS: substituído diretamente do JSON (sem IA)
  2. Blocos ARGUMENTATIVOS: gerados por Claude com base na ficha do caso
  3. Campos condicionais: avaliados por regras de negócio, não por LLM

Isso minimiza o uso de IA ao máximo possível.
"""

import os
import json
from pathlib import Path
from typing import Any

from celery import shared_task
from loguru import logger
from jinja2 import Environment, FileSystemLoader, StrictUndefined
import anthropic

from pipeline.db import get_session
from pipeline.models import Case, CaseStatus
from pipeline.storage import StorageBackend

storage = StorageBackend()
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

TEMPLATES_DIR = Path(os.environ.get("TEMPLATES_DIR", "./templates"))


@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=30,
    name="pipeline.workers.template_worker.fill_template_task",
)
def fill_template_task(self, case_id: str) -> dict:
    """
    Celery task: preenche o template da peça jurídica.

    Args:
        case_id: ID do caso

    Returns:
        dict com case_id e caminho do .md da peça gerada
    """
    logger.info(f"[TEMPLATE_WORKER] Preenchendo template para caso {case_id}")

    with get_session() as session:
        case = session.get(Case, case_id)
        json_path = case.case_sheet_path
        template_name = case.metadata.get("template", "peticao_inicial_previdenciaria.j2")
        case.status = CaseStatus.DRAFTING
        session.commit()

    try:
        # 1. Carregar a ficha do caso
        case_sheet_raw = storage.load_text(json_path)
        case_sheet = json.loads(case_sheet_raw)

        # 2. Identificar quais blocos precisam de LLM
        llm_blocks = _identify_llm_blocks(template_name)

        # 3. Gerar blocos argumentativos com Claude
        generated_blocks = {}
        for block_key, block_config in llm_blocks.items():
            generated_blocks[block_key] = _generate_argument_block(
                block_key, block_config, case_sheet, case_id
            )

        # 4. Montar contexto completo para o template
        template_context = {
            **_flatten_case_sheet(case_sheet),
            **generated_blocks,
        }

        # 5. Renderizar o template com Jinja2 (deterministic)
        rendered_md = _render_template(template_name, template_context)

        # 6. Salvar o rascunho
        draft_path = f"cases/{case_id}/draft/peca_juridica.md"
        storage.save_text(draft_path, rendered_md)

        with get_session() as session:
            case = session.get(Case, case_id)
            case.draft_path = draft_path
            case.status = CaseStatus.AWAITING_REVIEW
            session.commit()

        logger.info(f"[TEMPLATE_WORKER] Rascunho gerado para caso {case_id} -> {draft_path}")
        return {"case_id": case_id, "draft_path": draft_path}

    except Exception as exc:
        logger.error(f"[TEMPLATE_WORKER] Erro ao preencher template do caso {case_id}: {exc}")
        raise self.retry(exc=exc)


def _flatten_case_sheet(case_sheet: dict, prefix: str = "") -> dict:
    """
    Aplana o case_sheet JSON em um dict plano para uso no Jinja2.
    Ex: case_sheet.cliente.nome -> {"cliente_nome": "João"}
    """
    flat = {}
    for key, value in case_sheet.items():
        full_key = f"{prefix}_{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(_flatten_case_sheet(value, full_key))
        elif isinstance(value, list):
            flat[full_key] = value
        else:
            flat[full_key] = value if value is not None else ""
    return flat


def _identify_llm_blocks(template_name: str) -> dict:
    """
    Carrega o mapa de blocos que precisam de geração por LLM.
    Definido em templates/rules/<template_name>.json
    """
    rules_path = TEMPLATES_DIR / "rules" / template_name.replace(".j2", ".json")
    if not rules_path.exists():
        logger.warning(f"[TEMPLATE_WORKER] Não encontrado arquivo de regras: {rules_path}")
        return {}
    with open(rules_path, encoding="utf-8") as f:
        rules = json.load(f)
    return rules.get("llm_blocks", {})


def _generate_argument_block(
    block_key: str,
    block_config: dict,
    case_sheet: dict,
    case_id: str,
) -> str:
    """
    Gera um bloco argumentativo via Claude.
    Só chamado para campos que exigem elaboração jurídica.
    """
    instruction = block_config.get("instruction", "")
    inputs = block_config.get("inputs", [])

    # Montar contexto mínimo para o bloco
    context_parts = []
    for input_key in inputs:
        keys = input_key.split(".")
        value = case_sheet
        for k in keys:
            value = value.get(k, {}) if isinstance(value, dict) else None
        if value:
            context_parts.append(f"{input_key}: {json.dumps(value, ensure_ascii=False)}")

    context_str = "\n".join(context_parts)

    prompt = f"""Você é um advogado especialista em direito previdenciário brasileiro.
Com base nos dados do caso abaixo, redija o bloco jurídico solicitado.

DADOS DO CASO:
{context_str}

INSTRUÇÃO: {instruction}

Regras:
- Use linguagem jurídica formal
- Não invente fatos. Use apenas os dados fornecidos.
- Seja objetivo e preciso
- Não inclua títulos ou cabeçalhos, apenas o conteúdo do bloco"""

    try:
        response = client.messages.create(
            model=os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-5"),
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        result = response.content[0].text.strip()
        logger.info(f"[TEMPLATE_WORKER] Bloco '{block_key}' gerado ({len(result)} chars)")
        return result
    except Exception as e:
        logger.error(f"[TEMPLATE_WORKER] Falha ao gerar bloco '{block_key}': {e}")
        return f"[ERRO: Não foi possível gerar este bloco: {block_key}]"


def _render_template(template_name: str, context: dict) -> str:
    """Renderiza o template Jinja2 com o contexto fornecido."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        undefined=StrictUndefined,
        autoescape=False,
    )
    template = env.get_template(template_name)
    return template.render(**context)
