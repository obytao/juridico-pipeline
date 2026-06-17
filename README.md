# juridico-pipeline

Esteira de automação jurídica para geração de peças previdenciárias a partir de documentos do cliente.

---

## Como funciona

```
PDFs do cliente
     ↓
[Orchestrator] → cria Job + registra documentos
     ↓ (fan-out paralelo)
[text_worker]         [image_worker]
PDF → .md texto       imagens isoladas → Claude Vision → .md imagens
     ↓ (fan-in quando ambos terminam)
[merge_worker] → consolida os .md do documento
     ↓ (quando TODOS os documentos do caso estiverem prontos)
[json_worker] → Claude extrai case_sheet.json (1 chamada de IA por caso)
     ↓
[template_worker] → campos diretos via Jinja2 + blocos argumentativos via Claude
     ↓
[render_worker] → DOCX / PDF final
     ↓
[review_queue] → revisão humana (campos com confiança < 0.80)
```

---

## Onde entra a IA

| Etapa | IA? | Motivo |
|---|---|---|
| Extracão texto PDF | **Não** | pdfplumber / pymupdf |
| Isolamento imagens | **Não** | pymupdf |
| Análise de imagens | **Sim** | Claude Vision por imagem |
| Ficha do caso JSON | **Sim** | Claude, 1 chamada por caso |
| Campos diretos da peça | **Não** | Jinja2 renderiza do JSON |
| Blocos argumentativos | **Sim** | Claude, 1 chamada por bloco |
| Renderização PDF | **Não** | WeasyPrint / python-docx |

---

## Estrutura de pastas

```
juridico-pipeline/
├── pipeline/
│   ├── orchestrator.py       # Coordena toda a esteira
│   ├── schemas.py            # CaseSheet Pydantic (schema central)
│   ├── models.py             # Modelos SQLAlchemy (banco)
│   ├── db.py                 # Conexão com banco
│   ├── storage.py            # Abstracão de storage (local / S3)
│   └── workers/
│       ├── text_worker.py      # PDF → Markdown (texto)
│       ├── image_worker.py     # PDF → imagens → Claude Vision
│       ├── merge_worker.py     # Consolida .md por documento
│       ├── json_worker.py      # .md consolidado → case_sheet.json via Claude
│       ├── template_worker.py  # case_sheet.json → peça jurídica
│       └── render_worker.py    # Markdown → DOCX/PDF
├── templates/
│   ├── peticao_inicial_previdenciaria.j2   # Template Jinja2
│   └── rules/
│       └── peticao_inicial_previdenciaria.json # Regras dos blocos LLM
├── .env.example
├── requirements.txt
└── README.md
```

---

## Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/obytao/juridico-pipeline.git
cd juridico-pipeline

# 2. Crie o ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate      # Windows

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Configure as variáveis de ambiente
cp .env.example .env
# edite o .env com suas chaves

# 5. Suba o banco e o Redis
docker-compose up -d postgres redis

# 6. Rode as migrações
alembic upgrade head

# 7. Inicie os workers
celery -A pipeline.celery_app worker --loglevel=info --concurrency=4
```

---

## Uso básico

```python
from pipeline.orchestrator import Orchestrator

orchestrator = Orchestrator()

# Injeta um caso na pipeline
case_id = orchestrator.ingest_case(
    client_name="João da Silva",
    document_paths=[
        "/documentos/cnis.pdf",
        "/documentos/laudo_medico.pdf",
        "/documentos/decisao_inss.pdf",
    ],
    case_metadata={
        "template": "peticao_inicial_previdenciaria.j2",
        "tipo_acao": "aposentadoria_incapacidade",
    }
)

print(f"Caso {case_id} iniciado na pipeline.")

# Consulta o status
status = orchestrator.get_case_status(case_id)
print(status)
```

---

## Integrando seus extratores existentes

O `text_worker.py` tem dois pontos de integração sinalizados com `SUBSTITUA/COMPLEMENTE`:

- `_extract_with_pdfplumber()`: substitua pelo seu extrator principal
- `_extract_with_pymupdf()`: substitua pelo seu extrator de fallback

O `image_worker.py` já usa Claude Vision nativamente. Se você já tem uma habilidade do Claude configurada para análise de imagens, ajuste o prompt em `_analyze_image_with_claude()` para seguir o mesmo padrão da sua habilidade.

---

## Adicionando novos templates

1. Crie o template Jinja2 em `templates/seu_template.j2`
2. Crie as regras dos blocos LLM em `templates/rules/seu_template.json`
3. Passe `template: "seu_template.j2"` nos metadados ao chamar `ingest_case()`

---

## Arquivos pendentes (TODOs)

- [ ] `pipeline/models.py` — Modelos SQLAlchemy
- [ ] `pipeline/db.py` — Conexão com banco
- [ ] `pipeline/storage.py` — Abstracão de storage
- [ ] `pipeline/workers/merge_worker.py` — Merge de .md
- [ ] `pipeline/workers/render_worker.py` — Renderização DOCX/PDF
- [ ] `docker-compose.yml` — Infraestrutura local
- [ ] `pipeline/celery_app.py` — Configuração do Celery

---

## Tecnologias

- **Python 3.11+**
- **Celery + Redis** — fila e workers assíncronos
- **PostgreSQL + SQLAlchemy** — persistência de estado
- **Claude (Anthropic)** — extração estruturada e redação
- **Pydantic** — validação do schema da ficha do caso
- **Jinja2** — renderização determinística do template
- **pdfplumber / pymupdf** — extração de texto de PDFs
- **WeasyPrint** — geração de PDF final
