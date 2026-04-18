"""
Página Técnica: Visão de Futuro — evolução da plataforma e novas fontes.
"""

import streamlit as st

st.set_page_config(page_title="Visão de Futuro · Visão Técnica", layout="wide")
st.title("🔭 Visão de Futuro")
st.caption("Como a plataforma pode evoluir — novas fontes, parceiros e capacidades analíticas")
st.markdown("---")

st.markdown(
    """
    A arquitetura atual foi desenhada para ser **extensível por design**.
    Adicionar uma nova fonte de dados significa criar um novo módulo em
    `etl/transform/` e uma Lambda de extração — sem alterar o restante do pipeline.

    A evolução da plataforma é organizada em **3 horizontes de tempo**.
    """
)

# ---------------------------------------------------------------------------
# Linha do tempo visual
# ---------------------------------------------------------------------------
st.subheader("Roadmap de Evolução")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 📅 Curto Prazo")
    st.caption("3–6 meses · Fontes públicas já disponíveis")
    st.markdown(
        """
        **IBGE — Censo 2022**
        Renda média, densidade populacional e
        perfil etário por setor censitário.
        Enriquece o score com contexto socioeconômico.

        ---

        **IBGE — Cadastro de Empresas (CEMPRE)**
        Série histórica de abertura e fechamento
        de CNPJs. Permite calcular taxa de
        sobrevivência de empresas por bairro
        e setor — sinal de maturidade do ecossistema.

        ---

        **Receita Federal — CNAEs secundários**
        Empresas com múltiplos CNAEs revelam
        diversificação real além da atividade principal.

        ---

        **PBH — GPS dos ônibus (20s)**
        Frequência real das linhas por corredor,
        não apenas a programada. Identifica
        bairros com serviço irregular.
        """
    )

with col2:
    st.markdown("### 📅 Médio Prazo")
    st.caption("6–18 meses · Parcerias com empresas privadas")
    st.markdown(
        """
        **Operadoras de cartão (Cielo, Stone, Rede)**
        Volume e ticket médio de transações por
        categoria e bairro. É a medida mais direta
        de demanda de consumo no território.

        ---

        **Apps de mobilidade (99, Uber, inDrive)**
        Origem e destino de corridas por região
        e hora do dia. Captura a mobilidade
        não coberta pelo transporte público —
        especialmente bairros periféricos.

        ---

        **Plataformas imobiliárias (ZAP, QuintoAndar)**
        Preço do m² comercial e residencial,
        taxa de vacância de imóveis comerciais.
        Indica custo de entrada e tendência
        de valorização do entorno.

        ---

        **Google Places / Foursquare**
        Avaliações, horários de pico e
        popularidade de estabelecimentos.
        Proxy de atratividade percebida
        pelos frequentadores reais.
        """
    )

with col3:
    st.markdown("### 📅 Longo Prazo")
    st.caption("18+ meses · Infraestrutura contínua e preditiva")
    st.markdown(
        """
        **Sensores IoT urbanos**
        Contagem de pedestres e veículos
        em tempo real por corredor. Elimina
        a necessidade de proxies (acidentes)
        para estimar fluxo de tráfego.

        ---

        **Modelo preditivo de score**
        ML treinado sobre o histórico de
        empresas + mobilidade + mercado imobiliário
        para prever valorização futura de bairros.
        Feature engineering sobre série temporal
        do score por bairro.

        ---

        **API REST para investidores**
        Endpoint que retorna o score de
        qualquer bairro com parâmetros
        customizáveis por setor de interesse.
        Permite integração com ferramentas
        de análise dos próprios investidores.

        ---

        **Dashboard white-label**
        Versão customizável da plataforma
        para prefeituras de outras cidades,
        usando a mesma arquitetura AWS.
        """
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# Arquitetura futura com novos dados
# ---------------------------------------------------------------------------
st.subheader("Como Novas Fontes se Integram à Arquitetura")

st.markdown(
    """
    O padrão de integração é o mesmo independente da fonte.
    Adicionar dados de um novo parceiro privado segue 3 passos:
    """
)

st.code(
    """
Passo 1 — Ingestão (nova Lambda)
─────────────────────────────────────────────────────────────
lambda_extract_cielo.py
    • Autentica via Secrets Manager
    • Consulta API da Cielo com filtros de data e região
    • Grava JSON/CSV em s3://bh-insights-raw/cielo/year=YYYY/month=MM/

Passo 2 — Transformação (novo job Glue)
─────────────────────────────────────────────────────────────
transform_transacoes.py (PySpark)
    • Lê do S3 Raw
    • Normaliza nome de bairro (mesmo padrão dos outros módulos)
    • Agrega: volume_transacoes, ticket_medio por bairro e categoria
    • Grava em s3://bh-insights-processed/transacoes_por_bairro/

Passo 3 — Incorporação ao Score
─────────────────────────────────────────────────────────────
score.py (atualização)
    • Carrega transacoes_por_bairro.parquet
    • Adiciona nova dimensão ou enriquece dimensão existente
    • Repondera os pesos conforme relevância do novo sinal
""",
    language="text",
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Governança e qualidade de dados
# ---------------------------------------------------------------------------
st.subheader("Governança e Qualidade de Dados")

st.markdown(
    """
    Com múltiplas fontes — públicas e privadas — a qualidade e a rastreabilidade
    dos dados se tornam críticas. As práticas recomendadas para escala:
    """
)

col_a, col_b = st.columns(2)

with col_a:
    st.markdown(
        """
        **Qualidade de Dados (Great Expectations)**
        Validação automática após cada ingestão:
        - Bairros conhecidos cobertos acima de X%?
        - Valores nulos dentro do threshold esperado?
        - Distribuição de CNAEs consistente com mês anterior?

        Se a validação falhar, o job Glue não avança e
        um alerta é disparado via CloudWatch → SNS → email/Slack.

        **Versionamento de schemas (Glue Data Catalog)**
        Toda alteração de schema (nova coluna, mudança de tipo)
        é registrada com versão. Jobs antigos continuam funcionando
        com a versão anterior enquanto novos usam a mais recente.
        """
    )

with col_b:
    st.markdown(
        """
        **Linhagem de dados (AWS Glue + tags S3)**
        Cada arquivo Parquet gerado inclui metadados:
        - Fonte original
        - Data de ingestão
        - Versão do job que gerou
        - Hash de integridade

        Isso permite rastrear de onde veio qualquer número
        no dashboard — essencial para ganhar confiança de investidores.

        **Contratos de dados com parceiros**
        Parceiros privados assinam um contrato de dados
        definindo: schema esperado, SLA de atualização,
        cobertura mínima de bairros e processo de
        notificação em caso de mudança de formato.
        """
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# Impacto potencial
# ---------------------------------------------------------------------------
st.subheader("Impacto Potencial da Plataforma Evoluída")

st.info(
    """
    Com todas as fontes integradas, a plataforma passaria de uma ferramenta
    de **análise descritiva** (o que existe hoje) para uma ferramenta de
    **inteligência preditiva** — capaz de responder:

    - *"Dado o histórico dos últimos 3 anos, quais bairros têm maior
      probabilidade de valorização nos próximos 18 meses?"*
    - *"Qual setor tem demanda latente não atendida no bairro X?"*
    - *"Como a abertura de uma nova linha de ônibus impactaria o score
      de atratividade dos bairros ao longo do corredor?"*

    Esse nível de insight transformaria a plataforma em um **ativo estratégico**
    não apenas para investidores privados, mas para a própria Prefeitura de
    Belo Horizonte no planejamento urbano.
    """,
    icon="🚀",
)
