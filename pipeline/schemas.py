"""
schemas.py - Schemas Pydantic para validação da ficha do caso.

Este é o schema central da pipeline. Toda informação extraída
deve passar por aqui antes de ser usada para gerar a peça.

ADAPTE os campos conforme seus tipos de processo:
- Previdenciário (INSS, BPC/LOAS)
- Acidentário
- Assistencial
"""

from typing import List, Optional, Any
from pydantic import BaseModel, Field


class Source(BaseModel):
    """Fonte de um fato extraído de um documento."""
    document_id: str
    page: Optional[int] = None
    excerpt: Optional[str] = None


class Fact(BaseModel):
    """Fato extraído com rastreabilidade e confiança."""
    value: Optional[Any] = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    sources: List[Source] = []
    conflicts: List[str] = []  # Descrição de conflitos entre documentos


class Cliente(BaseModel):
    nome: Fact
    cpf: Fact
    rg: Optional[Fact] = None
    nit_pis: Optional[Fact] = None
    data_nascimento: Optional[Fact] = None
    endereco: Optional[Fact] = None
    telefone: Optional[Fact] = None
    email: Optional[Fact] = None
    representante_legal: Optional[Fact] = None  # Para menores/incapazes


class Beneficio(BaseModel):
    tipo: Fact  # Ex: "Aposentadoria por Incapacidade", "BPC/LOAS", "Auxílio-Doença"
    numero_beneficio: Optional[Fact] = None
    der: Optional[Fact] = None  # Data de Entrada do Requerimento
    dcb: Optional[Fact] = None  # Data de Cessacao do Beneficio
    competencia: Optional[Fact] = None
    status_administrativo: Optional[Fact] = None  # Indeferido, Cessado, etc.
    motivo_cessacao: Optional[Fact] = None
    rmi: Optional[Fact] = None  # Renda Mensal Inicial


class Incapacidade(BaseModel):
    doenca_cid: Optional[Fact] = None
    data_inicio_incapacidade: Optional[Fact] = None
    incapacidade_atual: Optional[Fact] = None  # True/False
    incapacidade_total: Optional[Fact] = None  # True/False
    reabilitacao_possivel: Optional[Fact] = None
    laudo_pericial_inss: Optional[Fact] = None
    laudo_pericial_particular: Optional[Fact] = None
    historico_medico: Optional[Fact] = None


class TempoContribuicao(BaseModel):
    total_meses_cnis: Optional[Fact] = None
    total_meses_declarados: Optional[Fact] = None
    competencias_ausentes: Optional[Fact] = None  # Lista de períodos sem contribuição
    qualidade_segurado_der: Optional[Fact] = None  # True/False
    data_perda_qualidade: Optional[Fact] = None
    periodos_controversos: Optional[Fact] = None


class Tese(BaseModel):
    fundamento_principal: Fact
    fundamentos_secundarios: List[Fact] = []
    jurisprudencia_aplicavel: List[str] = []
    pedido_principal: Fact
    pedidos_secundarios: List[Fact] = []
    valor_causa_estimado: Optional[Fact] = None


class Risco(BaseModel):
    descricao: str
    severidade: str  # "alto", "medio", "baixo"
    recomendacao: str


class CaseSheet(BaseModel):
    """
    Ficha completa do caso.
    Gerada pelo json_worker e consumida pelo template_worker.
    """
    case_id: Optional[str] = None
    cliente: Cliente
    beneficio: Beneficio
    incapacidade: Optional[Incapacidade] = None
    tempo_contribuicao: Optional[TempoContribuicao] = None
    tese: Tese
    riscos: List[Risco] = []
    lacunas: List[str] = []  # Informações ausentes ou não encontradas
    documentos_analisados: List[str] = []
    observacoes: Optional[str] = None

    class Config:
        populate_by_name = True
