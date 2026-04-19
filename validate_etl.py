"""
Script de validação dos Parquets gerados pelo ETL do projeto BH Investment Insights.

Execução dentro do container:
    docker-compose exec app python validate_etl.py

Ou diretamente no host (se pandas instalado):
    python validate_etl.py
"""

import sys
from pathlib import Path

import pandas as pd

# ── Configuração ──────────────────────────────────────────────────────────────

PROCESSED_DIR = Path("data/processed")

# Cada entrada: (arquivo, coluna_bairro_esperada, colunas_numericas_esperadas)
PARQUET_SPECS: dict[str, dict] = {
    "empresas_por_bairro.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": ["total_empresas"],
        "score_col": None,
    },
    "acessibilidade_por_bairro.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": ["total_pontos_onibus"],
        "score_col": None,
    },
    "qualidade_urbana_por_bairro.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": [],
        "score_col": None,
    },
    "matriz_od_agregada.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": [],
        "score_col": None,
    },
    "score_final.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": ["score_final"],
        "score_col": "score_final",
    },
}

# Bairros de BH que devem existir nos dados — validação de sanidade básica
BAIRROS_CONHECIDOS = [
    "Savassi", "Lourdes", "Centro", "Pampulha", "Buritis",
    "Floresta", "Santa Teresa", "Contorno",
]

SEPARATOR = "─" * 60


# ── Helpers ───────────────────────────────────────────────────────────────────

def secao(titulo: str) -> None:
    """Imprime cabeçalho de seção formatado."""
    print(f"\n{SEPARATOR}")
    print(f"  {titulo}")
    print(SEPARATOR)


def ok(msg: str) -> None:
    print(f"  ✅  {msg}")


def warn(msg: str) -> None:
    print(f"  ⚠️   {msg}")


def erro(msg: str) -> None:
    print(f"  ❌  {msg}")


# ── Etapa 1: Leitura e estrutura ──────────────────────────────────────────────

def validar_estrutura(nome: str, spec: dict) -> pd.DataFrame | None:
    """
    Carrega o Parquet e valida shape, tipos e nulos.

    :param nome: Nome do arquivo Parquet
    :param spec: Dicionário com metadados esperados do arquivo
    :return: DataFrame carregado, ou None se falhou
    """
    secao(f"ETAPA 1 — Estrutura: {nome}")
    caminho = PROCESSED_DIR / nome

    # 1a. Arquivo existe?
    if not caminho.exists():
        erro(f"Arquivo não encontrado: {caminho}")
        return None

    # 1b. Carrega
    try:
        df = pd.read_parquet(caminho)
        ok(f"Carregado com sucesso — shape: {df.shape}")
    except Exception as e:
        erro(f"Falha ao carregar: {e}")
        return None

    # 1c. Colunas
    print(f"\n  Colunas ({len(df.columns)}):")
    for col in df.columns:
        dtype = str(df[col].dtype)
        nulos = df[col].isna().sum()
        pct = nulos / len(df) * 100
        flag = "⚠️ " if pct > 20 else "  "
        print(f"    {flag} {col:<40} dtype={dtype:<15} nulos={nulos} ({pct:.1f}%)")

    # 1d. Coluna de bairro existe?
    col_bairro = spec["col_bairro"]
    if col_bairro in df.columns:
        ok(f"Coluna de bairro '{col_bairro}' encontrada")
    else:
        warn(
            f"Coluna de bairro '{col_bairro}' NÃO encontrada. "
            f"Colunas disponíveis: {df.columns.tolist()}"
        )

    # 1e. Colunas numéricas esperadas
    for col in spec["cols_numericas"]:
        if col in df.columns:
            ok(f"Coluna numérica '{col}' presente")
        else:
            warn(f"Coluna numérica '{col}' ausente — verifique o nome real")

    return df


# ── Etapa 2: Sanidade negocial ────────────────────────────────────────────────

def validar_sanidade(nome: str, df: pd.DataFrame, spec: dict) -> None:
    """
    Valida se os dados fazem sentido do ponto de vista de negócio.

    :param nome: Nome do arquivo para exibição
    :param df: DataFrame já carregado
    :param spec: Dicionário com metadados esperados do arquivo
    """
    secao(f"ETAPA 2 — Sanidade negocial: {nome}")

    col_bairro = spec["col_bairro"]

    # Guard: DataFrame vazio não tem o que validar
    if len(df) == 0:
        warn("DataFrame vazio — ETL não gerou linhas para este arquivo")
        return

    # 2a. Bairros conhecidos presentes?
    if col_bairro in df.columns and df[col_bairro].dtype == object:
        bairros_no_df = df[col_bairro].str.upper().tolist()
        for bairro in BAIRROS_CONHECIDOS:
            encontrado = any(bairro.upper() in b for b in bairros_no_df)
            if encontrado:
                ok(f"Bairro '{bairro}' encontrado")
            else:
                warn(f"Bairro '{bairro}' NÃO encontrado — pode ser nome diferente")
    elif col_bairro in df.columns:
        warn(f"Coluna '{col_bairro}' existe mas dtype={df[col_bairro].dtype} — esperado object/string")
    else:
        warn("Pulando verificação de bairros — coluna de bairro ausente")

    # 2b. Duplicatas por bairro?
    if col_bairro in df.columns:
        dupes = df[col_bairro].duplicated().sum()
        if dupes == 0:
            ok("Sem bairros duplicados")
        else:
            warn(f"{dupes} bairros duplicados — possível problema no GROUP BY do ETL")

    # 2c. Score final: distribuição
    score_col = spec["score_col"]
    if score_col and score_col in df.columns:
        s = df[score_col]
        print(f"\n  Distribuição de '{score_col}':")
        print(f"    min={s.min():.4f}  max={s.max():.4f}  "
              f"mean={s.mean():.4f}  std={s.std():.4f}")

        # Score deve estar entre 0 e 100
        fora_do_range = ((s < 0) | (s > 100)).sum()
        if fora_do_range == 0:
            ok("Todos os scores entre 0 e 100 ✓")
        else:
            warn(f"{fora_do_range} scores fora do intervalo [0, 100]")

        # Top 10 bairros
        if col_bairro in df.columns:
            print("\n  🏆 Top 10 bairros por score:")
            top10 = (
                df[[col_bairro, score_col]]
                .sort_values(score_col, ascending=False)
                .head(10)
                .reset_index(drop=True)
            )
            top10.index += 1
            print(top10.to_string())

    # 2d. Colunas numéricas: valores negativos inesperados
    for col in spec["cols_numericas"]:
        if col in df.columns and col != score_col:
            negativos = (df[col] < 0).sum()
            if negativos > 0:
                warn(f"'{col}' tem {negativos} valores negativos — verifique")
            else:
                ok(f"'{col}' sem valores negativos")


# ── Etapa 3: Compatibilidade Streamlit ───────────────────────────────────────

def validar_streamlit(nome: str, df: pd.DataFrame, spec: dict) -> None:
    """
    Simula o que o Streamlit faz ao carregar o Parquet.

    :param nome: Nome do arquivo para exibição
    :param df: DataFrame já carregado
    :param spec: Dicionário com metadados esperados do arquivo
    """
    secao(f"ETAPA 3 — Compatibilidade Streamlit: {nome}")

    # 3a. Serialização JSON (st.dataframe usa isso internamente)
    try:
        _ = df.to_json(orient="records")
        ok("Serialização JSON (st.dataframe) OK")
    except Exception as e:
        erro(f"Falha na serialização JSON: {e}")

    # 3b. Colunas com tipos object que podem causar problema
    obj_cols = df.select_dtypes(include="object").columns.tolist()
    if obj_cols:
        warn(f"Colunas 'object' que podem precisar de cast: {obj_cols}")
    else:
        ok("Sem colunas dtype=object problemáticas")

    # 3c. Index padrão (Streamlit prefere RangeIndex)
    if isinstance(df.index, pd.RangeIndex):
        ok("Index é RangeIndex — compatível com Streamlit")
    else:
        warn(
            f"Index é '{type(df.index).__name__}' — "
            "considere df.reset_index() antes de passar ao Streamlit"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    """Ponto de entrada: itera sobre todos os Parquets e roda as 3 etapas."""
    print("\n" + "═" * 60)
    print("  BH Investment Insights — Validação de ETL")
    print("═" * 60)

    resultados: dict[str, bool] = {}

    for nome, spec in PARQUET_SPECS.items():
        df = validar_estrutura(nome, spec)
        if df is None:
            resultados[nome] = False
            continue

        validar_sanidade(nome, df, spec)
        validar_streamlit(nome, df, spec)
        resultados[nome] = True

    # Resumo final
    secao("RESUMO FINAL")
    for nome, sucesso in resultados.items():
        if sucesso:
            ok(nome)
        else:
            erro(f"{nome} — falhou na leitura")

    falhas = sum(1 for v in resultados.values() if not v)
    if falhas == 0:
        print("\n  🎉 Todos os Parquets passaram nas validações estruturais.")
        print("  Revise os ⚠️  acima para ajustes negociais antes do Streamlit.\n")
    else:
        print(f"\n  {falhas} arquivo(s) com falha crítica — verifique o ETL.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()