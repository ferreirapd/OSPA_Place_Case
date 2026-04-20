"""
Visão Técnica — pipeline, arquitetura e próximos passos.
"""

from pathlib import Path
import sys
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

ETL_DIR = Path(__file__).resolve().parents[2] / "etl"


def show_code(path: Path, label: str) -> None:
    with st.expander(f"Código — {label}"):
        if path.exists():
            st.code(path.read_text(encoding="utf-8"), language="python")
        else:
            st.warning(f"Arquivo não encontrado: {path}")


st.title("Visão Técnica")
st.caption("Pipeline de dados, decisões de engenharia e proposta de arquitetura em produção")
st.markdown("---")

tab_pipeline, tab_arq, tab_futuro = st.tabs([
    "Pipeline ETL", "Arquitetura em produção", "Próximos passos"
])

with tab_pipeline:

    st.markdown(
        """
        O pipeline foi desenvolvido em Python com três estágios sequenciais:
        extração, transformação e composição do score final. Cada estágio é
        independente — dá pra rodar `--skip-extract` em desenvolvimento e
        reprocessar só as transformações sem rebaixar tudo de novo.

        A estrutura de pastas reflete essa separação:
        """
    )

    st.code(
        """
OSPA_Place_Case/
├── data/
│   ├── raw/                        # CSVs brutos — não versionados
│   └── processed/
│       ├── empresas_por_bairro.parquet
│       ├── acessibilidade_por_bairro.parquet
│       ├── qualidade_urbana_por_bairro.parquet
│       ├── matriz_od_agregada.parquet
│       ├── score_final.parquet
│       └── bairros_excluidos.csv   # auditoria de exclusões do ETL
│
├── etl/
│   ├── extract.py
│   ├── pipeline.py
│   └── transform/
│       ├── _io.py          # leitura de CSV, normalização, fuzzy match
│       ├── _spatial.py     # spatial join GeoPandas reutilizável
│       ├── economico.py
│       ├── acessibilidade.py
│       ├── qualidade_urbana.py
│       ├── matriz_od.py    # PySpark
│       └── score.py
│
├── app/
│   ├── main.py
│   ├── components/
│   │   ├── graficos.py
│   │   └── mapas.py
│   └── pages/
│
├── notebooks/
├── validate_etl.py
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
        """,
        language="text",
    )

    st.markdown("---")
    st.subheader("Extract")
    st.markdown(
        """
        O módulo de extração consulta a API CKAN do portal da PBH para resolver
        dinamicamente a URL do recurso mais recente de cada dataset, depois faz
        download com streaming para não carregar o arquivo inteiro na RAM.
        É idempotente: se o arquivo já existe em `data/raw/`, pula.
        """
    )
    show_code(ETL_DIR / "extract.py", "extract.py")

    st.markdown("---")
    st.subheader("Transform")
    st.markdown(
        """
        Cada módulo de transformação segue o mesmo padrão: carrega os CSVs com
        detecção automática de encoding e separador, aplica fuzzy match dos nomes
        de bairro contra uma tabela canônica extraída da própria base de
        atividade econômica, e agrega as métricas por bairro.

        O fuzzy match existe porque o `bairros.csv` da PBH usa subdivisões
        administrativas internas como nomes — tipo "Primeira Seção", "Do Castelo" —
        que não correspondem ao que o resto dos datasets chama de bairro. A base
        canônica são os ~489 nomes populares que aparecem nos registros de
        atividade econômica: CNPJ é registrado no endereço real, não no cadastro
        administrativo.

        Bairros que não encontram match com score acima de 88 são descartados e
        registrados em `data/processed/bairros_excluidos.csv` para auditoria.
        No último pipeline rodado, foram 208 exclusões — quase todas subdivisões
        numeradas do cadastro administrativo sem correspondente popular.

        O módulo da Matriz O-D usa PySpark porque o dataset tem 188k registros de
        pares hexágono H3 × hexágono H3. A agregação por hexágono de origem no
        Spark reduz o problema para ~2.300 hexágonos únicos antes do
        nearest-neighbor em Pandas/NumPy — que é quando faz sentido sair do Spark.
        """
    )

    col_a, col_b = st.columns(2)
    with col_a:
        show_code(ETL_DIR / "transform" / "_io.py", "_io.py")
        show_code(ETL_DIR / "transform" / "_spatial.py", "_spatial.py")
        show_code(ETL_DIR / "transform" / "economico.py", "economico.py")
    with col_b:
        show_code(ETL_DIR / "transform" / "acessibilidade.py", "acessibilidade.py")
        show_code(ETL_DIR / "transform" / "qualidade_urbana.py", "qualidade_urbana.py")
        show_code(ETL_DIR / "transform" / "matriz_od.py", "matriz_od.py")

    st.markdown("---")
    st.subheader("Score final")
    st.markdown(
        """
        O score consolida as três dimensões em um único número por bairro.
        Cada dimensão usa rank percentual internamente para evitar que um outlier
        (tipo o Centro com 26 mil empresas) comprima todos os outros bairros para
        perto de zero numa normalização min-max direta.

        A base da tabela final são os 489 bairros canônicos — não a união das
        fontes, que produziria duplicatas por variações de nome. Bairros sem
        dado em alguma dimensão recebem a média da dimensão como imputação.
        """
    )
    show_code(ETL_DIR / "transform" / "score.py", "score.py")
    show_code(ETL_DIR / "pipeline.py", "pipeline.py")


with tab_arq:

    st.markdown(
        """
        A arquitetura local foi desenhada para ser portável para produção sem
        mudar o código de transformação. O mesmo Pandas e PySpark que rodam no
        Docker rodam no Glue — a diferença é que na AWS o cluster é gerenciado,
        o agendamento é via EventBridge e o armazenamento é no S3.
        """
    )

    st.subheader("Fluxo em produção")

    st.code(
        """
[Portal PBH · IBGE · Parceiros]
            │
     EventBridge (schedule mensal)
            │
     Lambda (extract por fonte)
            │
     S3 Raw  ←── particionado por year/month
            │
     AWS Glue (PySpark gerenciado)
     ├── transform_economico
     ├── transform_acessibilidade
     ├── transform_qualidade
     ├── transform_matriz_od
     └── compute_score
            │
     S3 Processed  (Parquet)
            │
     ECS Fargate (imagem Docker)
            │
     CloudFront → [Investidor / Stakeholder]
        """,
        language="text",
    )

    st.markdown("---")
    st.markdown(
        """
        Algumas decisões que informam esse fluxo:

        - **Glue em vez de EMR** porque os jobs são periódicos (mensais) e
          serverless elimina o overhead de manter cluster ativo. Para a escala
          atual, EMR seria superdimensionado.

        - **S3 particionado por data** permite reprocessamento histórico pontual
          sem rebaixar tudo. Arquivos raw com mais de 90 dias migram
          automaticamente para Glacier via lifecycle policy.

        - **ECS Fargate** roda a mesma imagem Docker usada localmente — zero
          mudança no código do app ao migrar para produção.

        - **Glue Data Catalog** como registro central de schemas. Quando o portal
          da PBH muda o nome de uma coluna (o que acontece), o único lugar que
          precisa ser atualizado é o módulo de transform correspondente.
        """
    )

    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            """
            **Ingestão**
            - Amazon EventBridge
            - AWS Lambda (Python)
            - Amazon S3 (camada raw)
            - AWS Secrets Manager
            """
        )
    with col2:
        st.markdown(
            """
            **Processamento**
            - AWS Glue (PySpark gerenciado)
            - Glue Data Catalog
            - Amazon Athena
            - Amazon S3 (camada processed)
            """
        )
    with col3:
        st.markdown(
            """
            **Consumo**
            - Amazon ECR
            - ECS Fargate
            - Application Load Balancer
            - Amazon CloudFront
            - Amazon CloudWatch
            """
        )


with tab_futuro:

    st.markdown(
        """
        O projeto atual cobre bem o que está disponível nos dados abertos da PBH,
        mas há limites claros no que dados públicos conseguem capturar. As
        evoluções mais relevantes viriam de três frentes.

        **Novas fontes públicas disponíveis hoje** — o IBGE tem dados do Censo
        2022 por setor censitário que dariam contexto socioeconômico real aos
        bairros: renda média, densidade, perfil etário. O Cadastro de Empresas
        (CEMPRE) tem série histórica de abertura e fechamento de CNPJs, o que
        permitiria calcular taxa de sobrevivência de negócios por bairro — sinal
        muito mais forte de maturidade econômica do que simplesmente contar
        empresas ativas.

        **Dados privados via parceria** — o gap mais crítico no score atual é a
        ausência de dado de demanda de consumo. Volume de transações por cartão
        por bairro e categoria (Cielo, Stone, Rede) é provavelmente o dado mais
        direto para quem está decidindo onde abrir um negócio. Plataformas
        imobiliárias dariam preço de m² comercial e taxa de vacância — custo de
        entrada e tendência de valorização. Apps de mobilidade capturam
        deslocamentos não cobertos pelo transporte público, especialmente
        periferias.

        **Capacidades analíticas** — com histórico suficiente, dá pra sair de um
        score estático para modelos preditivos: bairros com perfil similar ao do
        Savassi de 10 anos atrás hoje, por exemplo. Isso muda completamente o
        valor da plataforma para investidores de longo prazo.

        **Qualidade de dados em escala** — em produção faz sentido adicionar
        validação formal com Great Expectations logo após cada ingestão. Se a
        distribuição de CNAEs mudar muito em relação ao mês anterior, ou se
        bairros conhecidos sumirem do dataset, o job para antes de propagar
        dado ruim para o score.
        """
    )