"""
Página Técnica: Arquitetura AWS — como o projeto escalaria em produção.
"""

import streamlit as st

st.set_page_config(page_title="Arquitetura AWS · Visão Técnica", layout="wide")
st.title("☁️ Arquitetura AWS — Produção")
st.caption("Como o projeto escalaria em um ambiente de produção real na AWS")
st.markdown("---")

st.markdown(
    """
    A arquitetura de produção foi desenhada em **3 camadas desacopladas**:
    Ingestão, Processamento e Consumo. Cada camada pode escalar independentemente.
    """
)

# ---------------------------------------------------------------------------
# Diagrama completo
# ---------------------------------------------------------------------------
st.subheader("Diagrama Completo")

st.code(
    """
┌──────────────────────────────────────────────────────────────────────┐
│                         FONTES DE DADOS                               │
│                                                                        │
│  [Portal PBH — CKAN API]    [IBGE]      [Parceiros Privados]         │
│   8 datasets públicos        Censo        Dados imobiliários          │
│   Atualização: mensal/anual  PIB/IPCA     Transações, mobilidade      │
└────────────────────┬─────────────────────────────────────────────────┘
                     │
          ┌──────────▼──────────┐
          │   Amazon EventBridge │  ← Agendamento de ingestão
          │                      │    Mensal: PBH/IBGE
          │   Regras de Schedule │    On-demand: Parceiros
          └──────────┬──────────┘
                     │ trigger
                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      CAMADA DE INGESTÃO                               │
│                                                                        │
│   AWS Lambda (Python)                                                  │
│   ┌──────────────────────────────────────────────────────────┐       │
│   │  lambda_extract_ckan.py    → datasets do portal PBH      │       │
│   │  lambda_extract_ibge.py    → censo, indicadores          │       │
│   │  lambda_extract_parceiros  → APIs privadas (via secrets) │       │
│   └──────────────────────────────────────────────────────────┘       │
│                     │                                                  │
│                     ▼                                                  │
│   Amazon S3 — Camada RAW  (s3://bh-insights-raw/)                    │
│   Particionamento: /fonte/year=YYYY/month=MM/arquivo.csv             │
│   Lifecycle: arquivos raw → Glacier após 90 dias                     │
└────────────────────┬─────────────────────────────────────────────────┘
                     │  S3 Event Notification (novo arquivo)
                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    CAMADA DE PROCESSAMENTO                             │
│                                                                        │
│   AWS Glue (PySpark gerenciado)                                        │
│   ┌──────────────────────────────────────────────────────────┐       │
│   │  Job: transform_economico       → Pandas/Spark           │       │
│   │  Job: transform_acessibilidade  → Pandas/Spark           │       │
│   │  Job: transform_qualidade       → Pandas/Spark           │       │
│   │  Job: transform_matriz_od       → PySpark (heavy)        │       │
│   │  Job: compute_score_final       → Pandas/Spark           │       │
│   └──────────────────────────────────────────────────────────┘       │
│                     │                                                  │
│   AWS Glue Data Catalog  ← schema registry + metadados               │
│                     │                                                  │
│                     ▼                                                  │
│   Amazon S3 — Camada PROCESSED  (s3://bh-insights-processed/)        │
│   Formato: Parquet com compressão Snappy                              │
│   Particionamento por bairro para queries Athena eficientes          │
│                     │                                                  │
│   Amazon Athena ────┘  ← queries ad-hoc sem mover dados              │
│   (analistas podem explorar direto via SQL)                           │
└────────────────────┬─────────────────────────────────────────────────┘
                     │  leitura do S3 processed/
                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      CAMADA DE CONSUMO                                 │
│                                                                        │
│   Amazon ECR  ← imagem Docker do Streamlit app                        │
│        │                                                               │
│        ▼                                                               │
│   Amazon ECS Fargate  ← container serverless (sem gerenciar EC2)     │
│   ┌──────────────────────────────────────────────────────────┐       │
│   │  Streamlit app                                            │       │
│   │  Lê s3://bh-insights-processed/ via boto3/PyArrow       │       │
│   │  Auto-scaling: 1–4 tasks conforme tráfego               │       │
│   └──────────────────────────────────────────────────────────┘       │
│                     │                                                  │
│   Application Load Balancer  ←  distribui tráfego entre tasks        │
│                     │                                                  │
│   Amazon CloudFront  ← CDN global, cache de assets estáticos         │
│                     │                                                  │
│             [Usuário final / Investidor]                              │
│                                                                        │
│   Serviços de suporte:                                                 │
│   • AWS Secrets Manager  → API keys de parceiros                     │
│   • Amazon CloudWatch    → logs, métricas, alertas de falha          │
│   • AWS IAM              → controle de acesso por serviço            │
└──────────────────────────────────────────────────────────────────────┘
""",
    language="text",
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Detalhamento por camada
# ---------------------------------------------------------------------------
st.subheader("Detalhamento por Camada")

tab1, tab2, tab3 = st.tabs(["🔽 Ingestão", "⚙️ Processamento", "📡 Consumo"])

with tab1:
    st.markdown(
        """
        ### Amazon EventBridge + AWS Lambda

        **EventBridge** age como o agendador de tarefas — substitui um cron job.
        Define regras de quando cada fonte deve ser reingestionada:

        | Fonte | Frequência | Justificativa |
        |---|---|---|
        | Portal PBH (CKAN) | Mensal | Periodicidade de atualização do portal |
        | IBGE indicadores | Anual | Dados do censo e estimativas populacionais |
        | Parceiros privados | On-demand ou diário | Depende do SLA contratado |

        **Lambda** executa as funções de extração sem servidor dedicado.
        Cada função é responsável por uma fonte e grava o CSV bruto no S3 Raw.

        **S3 Raw** usa particionamento por data (`year=YYYY/month=MM`) para
        facilitar reprocessamento histórico e lifecycle policies:
        arquivos com mais de 90 dias migram automaticamente para S3 Glacier,
        reduzindo custo de armazenamento em ~80%.
        """
    )

with tab2:
    st.markdown(
        """
        ### AWS Glue (PySpark gerenciado)

        Glue é essencialmente PySpark gerenciado — elimina o overhead de
        provisionar e manter um cluster Spark.

        **Por que Glue em vez de EMR?**
        EMR requer gerenciar o cluster (ligar, escalar, desligar).
        Glue é serverless: você define o job, ele provisiona o cluster,
        executa e destrói. Para jobs periódicos (mensal), isso é muito mais barato.

        **Glue Data Catalog** funciona como um registro central de schemas —
        todos os jobs compartilham a mesma definição de colunas e tipos,
        evitando divergências entre módulos.

        **Amazon Athena** permite que analistas executem SQL direto sobre
        os Parquets no S3 Processed, sem precisar do app Streamlit.
        Custo: ~$5 por TB escaneado.
        """
    )

with tab3:
    st.markdown(
        """
        ### ECS Fargate + CloudFront

        **ECS Fargate** é a forma mais simples de rodar containers na AWS
        sem gerenciar servidores. A mesma imagem Docker usada localmente
        é enviada ao ECR (registro de containers) e implantada no Fargate.

        **Auto-scaling** ajusta o número de containers (tasks) conforme tráfego:
        - Fora do horário comercial: 1 task (~mínimo custo)
        - Pico de acessos: até 4 tasks em paralelo

        **Application Load Balancer** distribui requisições entre as tasks
        e garante que uma task com falha seja substituída automaticamente.

        **CloudFront** faz cache dos assets estáticos do Streamlit (JS, CSS, imagens)
        nas ~400 edge locations da AWS, reduzindo latência para usuários
        em qualquer região do Brasil.

        **Secrets Manager** armazena credenciais de APIs de parceiros.
        As Lambda functions e os jobs Glue consultam o Secrets Manager
        em runtime — nenhuma chave fica hardcoded no código ou em variáveis
        de ambiente expostas.
        """
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# Estimativa de custo mensal
# ---------------------------------------------------------------------------
st.subheader("💰 Estimativa de Custo Mensal (AWS)")

st.caption("Estimativa para ~1.000 usuários/mês, pipeline rodando mensalmente")

st.dataframe(
    {
        "Serviço": [
            "S3 (raw + processed ~50GB)",
            "Lambda (8 funções × 1×/mês)",
            "Glue (5 jobs × 10 min × 2 DPUs)",
            "ECS Fargate (1 task 24/7, 0.25vCPU/0.5GB)",
            "CloudFront (10GB transferência)",
            "Athena (50GB escaneados)",
            "Outros (CloudWatch, ECR, Secrets)",
        ],
        "Custo estimado (USD/mês)": [
            "~$1,20",
            "~$0,00 (free tier)",
            "~$2,20",
            "~$9,00",
            "~$0,85",
            "~$0,25",
            "~$2,00",
        ],
        "Observação": [
            "Standard + Glacier para raw antigo",
            "Bem abaixo do free tier de 1M req/mês",
            "Job mais pesado: Matriz O-D",
            "Scale up conforme demanda",
            "Usuários no Brasil (baixa latência)",
            "Consultas analíticas esporádicas",
            "Estimativa conservadora",
        ],
    },
    use_container_width=True,
    hide_index=True,
)

st.success("**Total estimado: ~$15–20 USD/mês** para o cenário inicial de baixo tráfego.")
