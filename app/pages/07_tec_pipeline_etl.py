"""
Página Técnica: Pipeline ETL — diagrama de fluxo e código por etapa.
"""

from pathlib import Path
import streamlit as st

ETL_DIR = Path(__file__).resolve().parents[2] / "etl"

st.set_page_config(
    page_title="Pipeline ETL · Visão Técnica",
    layout="wide"
)
st.title("🔄 Pipeline ETL")
st.caption("Diagrama de fluxo, decisões técnicas e código-fonte por etapa")
st.markdown("---")


def _show_code(
    path: Path,
    label: str
) -> None:
    """
    Exibe o código-fonte de um arquivo dentro de um expander.

    :param path: Caminho do arquivo Python
    :param label: Título do expander
    """
    with st.expander(f"📄 Ver código — {label}"):
        if path.exists():
            st.code(path.read_text(encoding="utf-8"), language="python")
        else:
            st.warning(f"Arquivo não encontrado: {path}")


# ---------------------------------------------------------------------------
# Visão geral do pipeline
# ---------------------------------------------------------------------------
st.subheader("Visão Geral")

st.markdown(
    """
    O pipeline é composto por **3 estágios sequenciais** orquestrados por `pipeline.py`:

    ```
    Extract → Transform (×4 módulos) → Score
    ```

    Cada estágio é independente e pode ser executado isoladamente,
    facilitando reprocessamento parcial durante desenvolvimento.
    """
)

col1, col2, col3 = st.columns(3)
col1.info("**Extract**\n\nDownload das 8 fontes via CKAN API. Idempotente — pula se já existe.")
col2.info("**Transform**\n\n4 módulos independentes: econômico, acessibilidade, qualidade urbana e Matriz O-D (PySpark).")
col3.info("**Score**\n\nComposição final: merge das dimensões + normalização + score ponderado 0–100.")

st.markdown("---")

# ---------------------------------------------------------------------------
# Etapa 1: Extract
# ---------------------------------------------------------------------------
st.subheader("1. Extract — `etl/extract.py`")

st.markdown(
    """
    Consulta a **API CKAN** do portal da PBH para resolver dinamicamente a URL
    do recurso CSV mais recente de cada dataset, depois faz download com streaming.

    **Destaques técnicos:**
    - `requests.get(..., stream=True)` → não carrega o arquivo inteiro na RAM
    - Fallback de encoding (UTF-8 → Latin-1 → CP1252) nos CSVs do portal
    - Idempotência: arquivo existente = skip (sem `--force`)
    """
)

_show_code(ETL_DIR / "extract.py", "extract.py")

st.markdown("---")

# ---------------------------------------------------------------------------
# Etapa 2a: Transform — Econômico
# ---------------------------------------------------------------------------
st.subheader("2a. Transform — Atividade Econômica")

st.markdown(
    """
    Processa o maior dataset do projeto (potencialmente centenas de milhares de linhas).

    **Pipeline interno:**
    1. Filtra apenas empresas com `SITUACAO == 'ATIVA'`
    2. Normaliza nomes de bairro (strip, upper, remove acentos via NFD)
    3. Extrai divisão CNAE (2 primeiros dígitos → setor macro)
    4. Agrega por bairro: `total_empresas`, `diversidade_setores`, `setor_dominante`

    **Por que normalizar o nome do bairro?**
    O portal da PBH usa grafias inconsistentes entre datasets
    (ex: `"Centro"` vs `"CENTRO"` vs `"Centro "` com espaço).
    A normalização NFD → ASCII remove acentos e a combinação
    `.strip().upper()` elimina variações de case e espaço,
    garantindo que o join entre datasets funcione corretamente.
    """
)

_show_code(ETL_DIR / "transform" / "economico.py", "transform/economico.py")

st.markdown("---")

# ---------------------------------------------------------------------------
# Etapa 2b: Transform — Acessibilidade
# ---------------------------------------------------------------------------
st.subheader("2b. Transform — Acessibilidade Multimodal")

st.markdown(
    """
    Combina **3 fontes** para construir um índice de mobilidade mais robusto
    do que simplesmente contar pontos de ônibus.

    | Fonte | Métrica extraída | Peso no índice |
    |---|---|---|
    | Pontos de Ônibus | `total_pontos_onibus` por bairro | 35% |
    | Embarques por Ponto | `total_embarques_dia` por bairro (via join) | 50% |
    | Acidentes de Trânsito | `total_acidentes` por bairro (proxy de tráfego) | 15% |

    **Por que acidentes como proxy de tráfego?**
    BH não disponibiliza contagem direta de veículos por via.
    O volume de acidentes, apesar de ser um dado negativo em si,
    é altamente correlacionado com o fluxo de tráfego — bairros
    com mais movimento têm mais acidentes registrados.
    """
)

_show_code(ETL_DIR / "transform" / "acessibilidade.py", "transform/acessibilidade.py")

st.markdown("---")

# ---------------------------------------------------------------------------
# Etapa 2c: Transform — Qualidade Urbana
# ---------------------------------------------------------------------------
st.subheader("2c. Transform — Qualidade Urbana")

st.markdown(
    """
    Agrega equipamentos públicos de amenidade por bairro.
    Módulo mais simples do pipeline — serve como base para expansão futura
    com novas fontes (praças, centros culturais, unidades de saúde).
    """
)

_show_code(ETL_DIR / "transform" / "qualidade_urbana.py", "transform/qualidade_urbana.py")

st.markdown("---")

# ---------------------------------------------------------------------------
# Etapa 2d: Transform — Matriz O-D (PySpark)
# ---------------------------------------------------------------------------
st.subheader("2d. Transform — Matriz Origem-Destino (PySpark)")

st.markdown(
    """
    **Por que PySpark aqui e não Pandas?**

    A Matriz O-D é uma tabela esparsa de formato **hexágono H3 × hexágono H3**,
    onde cada célula representa viagens entre dois pontos da cidade.

    ```
    Problema de escala:
    ~500 hexágonos em BH → até 250.000 combinações por mês
    Cada mês = arquivo CSV com centenas de MB
    ```

    Em Pandas, um `crossJoin` para o spatial join seria O(n²) e
    poderia travar com dados de múltiplos meses.
    PySpark paraleliza o processamento nativamente e permite
    escalar para anos de histórico sem alterar o código.

    **Técnicas utilizadas:**
    - `broadcast join`: a tabela de bairros (~600 linhas) é
      enviada a todos os workers, evitando shuffle caro
    - UDF Python para conversão H3 → lat/lng
    - `spark.sql.shuffle.partitions=8` reduz overhead
      em datasets pequenos (padrão é 200)
    """
)

_show_code(ETL_DIR / "transform" / "matriz_od.py", "transform/matriz_od.py")

st.markdown("---")

# ---------------------------------------------------------------------------
# Etapa 3: Score
# ---------------------------------------------------------------------------
st.subheader("3. Score — Composição Final")

st.markdown(
    """
    Merge das 4 dimensões processadas + normalização + score ponderado.

    **Fórmula:**
    ```
    score_final (0–100) =
        40% × score_economico
      + 35% × score_acessibilidade
      + 25% × score_qualidade_urbana

    onde cada score_x = combinação linear de ranks percentuais (0–1)
    ```

    **Por que rank percentual e não min-max direto?**
    Min-max é sensível a outliers — um bairro com 10× mais empresas
    que todos os outros comprimiria os demais próximos de zero.
    O rank percentual distribui os bairros uniformemente no intervalo
    [0, 1] independente da distribuição dos valores brutos.
    """
)

_show_code(ETL_DIR / "transform" / "score.py", "transform/score.py")

st.markdown("---")

# ---------------------------------------------------------------------------
# Orquestrador
# ---------------------------------------------------------------------------
st.subheader("Orquestrador — `etl/pipeline.py`")

st.markdown(
    """
    Chama todas as etapas em sequência com logging de tempo por etapa.

    **Uso:**
    ```bash
    # Roda tudo (download + transform + score)
    docker-compose run app python -m etl.pipeline

    # Pula download (dados já em data/raw/)
    docker-compose run app python -m etl.pipeline --skip-extract
    ```
    """
)

_show_code(ETL_DIR / "pipeline.py", "pipeline.py")