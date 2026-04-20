"""
Script de validação dos Parquets gerados pelo ETL do projeto OSPA Place Data Engineer Case.

Execução dentro do container:
    docker-compose exec app python validate_etl.py

Ou diretamente no host (se pandas instalado):
    python validate_etl.py
"""

import sys
from pathlib import Path
import pandas as pd


PROCESSED_DIR = Path("data/processed")
PARQUET_SPECS: dict[str, dict] = {
    "empresas_por_bairro.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": ["total_empresas"],
        "score_col": None,
        # Nulos esperados em dimensões opcionais - sem threshold de alerta
        "nulos_ok": [],
    },
    "acessibilidade_por_bairro.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": ["total_pontos_onibus"],
        "score_col": None,
        "nulos_ok": [],
    },
    "qualidade_urbana_por_bairro.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": [],
        "score_col": None,
        "nulos_ok": [],
    },
    "matriz_od_agregada.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": [],
        "score_col": None,
        "nulos_ok": [],
    },
    "score_final.parquet": {
        "col_bairro": "bairro",
        "cols_numericas": ["score_final"],
        "score_col": "score_final",
        # No score_final, nulos em colunas de dimensões são esperados:
        # nem todo bairro tem parque ou equipamento esportivo registrado.
        "nulos_ok": [
            "total_empresas", "diversidade_setores",
            "setor_dominante", "setor_dominante_nome",
            "total_parques", "total_equipamentos_esportivos",
            "total_pontos_onibus", "total_embarques_dia", "total_acidentes",
            "total_viagens_originadas",
        ],
    },
}

# Bairros que DEVEM existir em todo parquet que use nomes populares.
# Todos em MAIÚSCULAS para bater com o padrão do ETL.
# Critério de seleção: bairros grandes, sem ambiguidade de nome,
# confirmados no dataset de atividade econômica (~425 bairros).
# Removidos: "CONTORNO" (avenida, não bairro no cadastro PBH),
#            "SANTA TERESA" (aparece às vezes como "STA TERESA").
BAIRROS_CONHECIDOS = [
    "SAVASSI",
    "LOURDES",
    "CENTRO",
    "PAMPULHA",
    "BURITIS",
    "FLORESTA",
    "FUNCIONARIOS",
    "BARREIRO",
]
SEPARATOR = "─" * 60


def secao(titulo: str) -> None:
    """
    Imprime um título de seção formatado para destacar as etapas.
    
    :param titulo: Título da seção
    """
    print(f"\n{SEPARATOR}")
    print(f"  {titulo}")
    print(SEPARATOR)


def ok(msg: str) -> None:
    """
    Imprime uma mensagem de sucesso formatada com um emoji de check.

    :param msg: Mensagem a ser exibida
    """
    print(f"  ✅  {msg}")


def warn(msg: str) -> None:
    """
    Imprime uma mensagem de aviso formatada com um emoji de alerta.
    
    :param msg: Mensagem a ser exibida
    """
    print(f"  ⚠️   {msg}")


def erro(msg: str) -> None:
    """
    Imprime uma mensagem de erro formatada com um emoji de erro.

    :param msg: Mensagem a ser exibida
    """
    print(f"  ❌  {msg}")


def validar_estrutura(
    nome: str,
    spec: dict
) -> pd.DataFrame | None:
    """
    Carrega o Parquet e valida shape, tipos e nulos.

    :param nome: Nome do arquivo Parquet
    :param spec: Dicionário com metadados esperados do arquivo
    :return: DataFrame carregado, ou None se falhou
    """
    secao(f"ETAPA 1 - Estrutura: {nome}")
    caminho = PROCESSED_DIR/nome

    if not caminho.exists():
        erro(f"Arquivo não encontrado: {caminho}")
        return None

    try:
        df = pd.read_parquet(caminho)
        ok(f"Carregado com sucesso - shape: {df.shape}")
    except Exception as e:
        erro(f"Falha ao carregar: {e}")
        return None

    nulos_ok: list[str] = spec.get("nulos_ok", [])

    print(f"\n  Colunas ({len(df.columns)}):")
    for col in df.columns:
        dtype = str(df[col].dtype)
        nulos = df[col].isna().sum()
        pct = nulos / len(df) * 100
        # Só emite ⚠️ se: >20% de nulos E coluna não está na lista de nulos esperados
        flag = "⚠️ " if (pct > 20 and col not in nulos_ok) else "  "
        print(f"    {flag} {col:<40} dtype={dtype:<15} nulos={nulos} ({pct:.1f}%)")

    col_bairro = spec["col_bairro"]
    if col_bairro in df.columns:
        ok(f"Coluna de bairro '{col_bairro}' encontrada")
    else:
        warn(
            f"Coluna de bairro '{col_bairro}' NÃO encontrada. "
            f"Disponíveis: {df.columns.tolist()}"
        )

    for col in spec["cols_numericas"]:
        if col in df.columns:
            ok(f"Coluna numérica '{col}' presente")
        else:
            warn(f"Coluna numérica '{col}' ausente — verifique o nome real")

    return df


def validar_sanidade(
    nome: str,
    df: pd.DataFrame,
    spec: dict
) -> None:
    """
    Valida se os dados fazem sentido do ponto de vista de negócio.

    :param nome: Nome do arquivo para exibição
    :param df: DataFrame já carregado
    :param spec: Dicionário com metadados esperados do arquivo
    """
    secao(f"ETAPA 2 - Sanidade negocial: {nome}")

    col_bairro = spec["col_bairro"]

    if len(df) == 0:
        warn("DataFrame vazio - ETL não gerou linhas para este arquivo")
        return

    if col_bairro in df.columns and df[col_bairro].dtype == object:

        # Normaliza a coluna para MAIÚSCULAS ASCII sem acento - mesmo padrão do ETL
        bairros_norm = (
            df[col_bairro]
            .str.strip()
            .str.upper()
            .str.normalize("NFD")
            .str.encode("ascii", errors="ignore")
            .str.decode("ascii")
        )
        bairros_set = set(bairros_norm.tolist())

        for bairro in BAIRROS_CONHECIDOS:
            # Normaliza o bairro de referência também (já está em upper/ASCII, mas por garantia)
            bairro_norm = (
                bairro.strip().upper()
                .encode("ascii", errors="ignore").decode("ascii")
            )
            if bairro_norm in bairros_set:
                ok(f"Bairro '{bairro}' encontrado")
            else:
                warn(f"Bairro '{bairro}' NÃO encontrado no parquet")

    elif col_bairro in df.columns:
        warn(
            f"Coluna '{col_bairro}' tem dtype={df[col_bairro].dtype} "
            f"- esperado object/string"
        )
    else:
        warn("Pulando verificação de bairros - coluna ausente")

    if col_bairro in df.columns:
        n_unique = df[col_bairro].nunique()
        n_total = len(df)
        if n_unique < n_total:
            warn(
                f"{n_total - n_unique} bairros duplicados "
                f"({n_unique} únicos de {n_total} linhas) - possível bug no GROUP BY"
            )
        else:
            ok(f"Cardinalidade OK: {n_unique} bairros únicos, sem duplicatas")

    score_col = spec["score_col"]
    if score_col and score_col in df.columns:
        s = df[score_col]
        print(f"\n  Distribuição de '{score_col}':")
        print(f"    min={s.min():.2f}  max={s.max():.2f}  "
              f"mean={s.mean():.2f}  std={s.std():.2f}")

        fora = ((s < 0) | (s > 100)).sum()
        if fora == 0:
            ok("Todos os scores no intervalo [0, 100] ✓")
        else:
            erro(
                f"{fora} scores fora de [0, 100] - "
                f"verifique a fórmula de score.py (multiplicação duplicada?)"
            )

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

    for col in spec["cols_numericas"]:
        if col in df.columns and col != score_col:
            negativos = (pd.to_numeric(df[col], errors="coerce") < 0).sum()
            if negativos > 0:
                warn(f"'{col}': {negativos} valores negativos inesperados")
            else:
                ok(f"'{col}' sem valores negativos")

    if "total_acidentes" in df.columns:
        n_uniq = pd.to_numeric(df["total_acidentes"], errors="coerce").nunique()
        if n_uniq <= 1:
            erro(
                "total_acidentes tem apenas 1 valor único - "
                "parquet stale da versão antiga (contagem global). "
                "Rode o ETL completo para regenerar."
            )
        else:
            ok(f"total_acidentes: {n_uniq} valores distintos ✓ (spatial join funcionando)")


def validar_streamlit(
    nome: str,
    df: pd.DataFrame,
    spec: dict
) -> None:
    """
    Simula checagens que o Streamlit faz ao renderizar o DataFrame.

    :param nome: Nome do arquivo para exibição
    :param df: DataFrame já carregado
    :param spec: Dicionário com metadados esperados do arquivo
    """
    secao(f"ETAPA 3 - Compatibilidade Streamlit: {nome}")

    try:
        _ = df.to_json(orient="records")
        ok("Serialização JSON (st.dataframe) OK")
    except Exception as e:
        erro(f"Falha na serialização JSON: {e}")

    # Colunas object são normais (bairro, setor_dominante…) - só avisa se houver
    obj_cols = df.select_dtypes(include="object").columns.tolist()
    if obj_cols:
        print(f"  ℹ️   Colunas string (object): {obj_cols}")
    else:
        ok("Sem colunas dtype=object")

    if isinstance(df.index, pd.RangeIndex):
        ok("Index é RangeIndex - compatível com Streamlit")
    else:
        warn(
            f"Index é '{type(df.index).__name__}' - "
            "considere df.reset_index() antes de passar ao Streamlit"
        )


def main() -> None:
    """
    Ponto de entrada: itera sobre todos os Parquets e roda as 3 etapas.
    """
    print("\n" + "═" * 60)
    print("  OSPA Place Data Engineer Case - Validação de ETL")
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

    secao("RESUMO FINAL")
    for nome, sucesso in resultados.items():
        if sucesso:
            ok(nome)
        else:
            erro(f"{nome} - falhou na leitura")

    falhas = sum(1 for v in resultados.values() if not v)
    if falhas == 0:
        print("\n  🎉 Todos os Parquets passaram nas validações estruturais.")
        print("  Revise os ⚠️  e ❌  acima para ajustes negociais.\n")
    else:
        print(f"\n  {falhas} arquivo(s) com falha crítica - verifique o ETL.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()