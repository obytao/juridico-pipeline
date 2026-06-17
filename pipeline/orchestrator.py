"""
Orchestrator - Coordena toda a esteira de automação jurídica.

Fluxo:
  1. Recebe lote de documentos de um caso
  2. Cria Job e subtarefas por documento
  3. Dispara workers de texto e imagem em paralelo (fan-out)
  4. Aguarda todos ficarem 'normalized' (fan-in)
  5. Consolida .md e chama IA para gerar case_sheet.json
  6. Aciona template_worker para preencher a peça
  7. Exporta PDF e registra auditoria
"""

import os
import uuid
from datetime import datetime
from typing import List, Optional

from celery import group, chain
from loguru import logger

from pipeline.models import Case, Document, Task, CaseStatus, DocumentStatus
from pipeline.storage import StorageBackend
from pipeline.workers.text_worker import extract_text_task
from pipeline.workers.image_worker import extract_images_task
from pipeline.workers.merge_worker import merge_document_task
from pipeline.workers.json_worker import generate_case_sheet_task
from pipeline.workers.template_worker import fill_template_task
from pipeline.workers.render_worker import render_pdf_task
from pipeline.db import get_session


class Orchestrator:
    """
    Ponto de entrada da pipeline.
    Recebe documentos brutos e coordena todo o processamento.
    """

    def __init__(self):
        self.storage = StorageBackend()

    def ingest_case(
        self,
        client_name: str,
        document_paths: List[str],
        case_metadata: Optional[dict] = None,
    ) -> str:
        """
        Inicia um novo caso na pipeline.

        Args:
            client_name: Nome do cliente
            document_paths: Lista de caminhos para os PDFs
            case_metadata: Metadados opcionais (tipo de ação, template desejado, etc.)

        Returns:
            case_id: ID único do caso criado
        """
        case_id = str(uuid.uuid4())
        logger.info(f"[ORCHESTRATOR] Iniciando caso {case_id} para cliente '{client_name}'")

        with get_session() as session:
            # 1. Criar registro do caso
            case = Case(
                id=case_id,
                client_name=client_name,
                status=CaseStatus.UPLOADED,
                metadata=case_metadata or {},
                created_at=datetime.utcnow(),
            )
            session.add(case)

            # 2. Registrar cada documento
            documents = []
            for path in document_paths:
                doc_id = str(uuid.uuid4())
                stored_path = self.storage.upload(path, f"cases/{case_id}/raw/")
                doc = Document(
                    id=doc_id,
                    case_id=case_id,
                    original_path=stored_path,
                    status_text=DocumentStatus.PENDING,
                    status_image=DocumentStatus.PENDING,
                    created_at=datetime.utcnow(),
                )
                session.add(doc)
                documents.append(doc)

            session.commit()

        # 3. Disparar workers em paralelo para cada documento (fan-out)
        self._dispatch_extraction_tasks(case_id, documents)
        return case_id

    def _dispatch_extraction_tasks(self, case_id: str, documents: List[Document]):
        """
        Fan-out: dispara texto + imagem para cada documento simultaneamente.
        """
        logger.info(f"[ORCHESTRATOR] Disparando {len(documents)} documentos em paralelo")

        for doc in documents:
            # Texto e imagem rodam em paralelo por documento
            group(
                extract_text_task.s(doc.id),
                extract_images_task.s(doc.id),
            ).apply_async(link=merge_document_task.s(doc.id, case_id))

    def on_document_normalized(self, doc_id: str, case_id: str):
        """
        Callback acionado quando texto + imagem de um documento estão prontos.
        Se TODOS os documentos do caso estiverem normalizados, avança para JSON.
        """
        with get_session() as session:
            pending = session.query(Document).filter(
                Document.case_id == case_id,
                Document.status_text != DocumentStatus.DONE,
            ).count() + session.query(Document).filter(
                Document.case_id == case_id,
                Document.status_image != DocumentStatus.DONE,
            ).count()

        if pending == 0:
            logger.info(f"[ORCHESTRATOR] Todos documentos normalizados para caso {case_id}. Avançando para JSON.")
            self._generate_case_sheet(case_id)
        else:
            logger.debug(f"[ORCHESTRATOR] Caso {case_id}: {pending} subtarefas ainda pendentes.")

    def _generate_case_sheet(self, case_id: str):
        """
        Dispara a geração da ficha do caso via LLM.
        Após gerado, aciona o preenchimento do template.
        """
        with get_session() as session:
            case = session.get(Case, case_id)
            case.status = CaseStatus.AWAITING_JSON
            session.commit()

        chain(
            generate_case_sheet_task.s(case_id),
            fill_template_task.s(case_id),
            render_pdf_task.s(case_id),
        ).apply_async()

    def get_case_status(self, case_id: str) -> dict:
        """Retorna o status atual do caso para consulta externa."""
        with get_session() as session:
            case = session.get(Case, case_id)
            docs = session.query(Document).filter(Document.case_id == case_id).all()

            return {
                "case_id": case_id,
                "client_name": case.client_name,
                "status": case.status.value,
                "documents": [
                    {
                        "id": d.id,
                        "status_text": d.status_text.value,
                        "status_image": d.status_image.value,
                    }
                    for d in docs
                ],
                "created_at": case.created_at.isoformat(),
            }
