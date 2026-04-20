"""
Utilitários compartilhados pelos módulos de transformação.

Responsabilidades:
- Leitura de CSVs brutos da PBH (encoding e separador variáveis)
- Normalização textual de nomes de bairro
- Padronização contra tabela canônica via fuzzy match
- Registry de exclusões (bairros descartados em cada etapa)
"""

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
import pandas as pd


log = logging.getLogger(__name__)

ENCODINGS = ("utf-8", "utf-8-sig", "latin-1", "cp1252")
SEPARATORS = (",", ";", "\t", "|")

# Sistema de referência oficial dos dados geoespaciais da PBH
EPSG_PBH = 31983

# Score mínimo (0-100) aceito no fuzzy match contra a tabela canônica.
# 88 rejeita falsos-positivos (SANTA EFIGENIA x SANTA MONICA) mas
# aceita variações legítimas (SAO JUDAS x SAO JUDAS TADEU).
FUZZY_SCORE_CUTOFF = 88

# Fonte canônica: NOME_BAIRRO de atividade_economica.csv tem ~489 nomes
# populares ("SAVASSI", "CENTRO"). NÃO usar bairros.csv/NOME - contém
# subdivisões administrativas ("Primeira", "Sexta"), não nomes populares.
BAIRROS_CANONICOS_CANDIDATOS = ("NOME_BAIRRO", "NOME_BAIRRO_POPULAR", "BAIRRO")


@dataclass
class ExclusaoRegistry:
    """
    Acumula bairros excluídos ao longo do pipeline para auditoria.
    """

    records: list[dict] = field(default_factory=list)

    def add(
        self,
        etapa: str,
        fonte: str,
        bairro_raw: str,
        motivo: str
    ) -> None:
        """
        Registra uma exclusão de bairro.

        :param etapa: Nome da etapa do ETL (ex: 'acessibilidade')
        :param fonte: Nome do dataset de origem (ex: 'pontos_onibus')
        :param bairro_raw: Valor bruto que não foi mapeado
        :param motivo: Categoria da exclusão (ex: 'score_baixo', 'sem_sjoin')
        """
        self.records.append({
            "etapa": etapa,
            "fonte": fonte,
            "bairro_raw": str(bairro_raw),
            "motivo": motivo,
        })

    def add_many(
        self,
        etapa: str,
        fonte: str,
        bairros: list[str] | set[str],
        motivo: str
    ) -> None:
        """
        Registra múltiplas exclusões de uma vez.

        :param etapa: Nome da etapa do ETL
        :param fonte: Nome do dataset de origem
        :param bairros: Iterável com os valores descartados
        :param motivo: Categoria da exclusão
        """
        for b in bairros:
            self.add(etapa, fonte, b, motivo)

    def reset(self) -> None:
        """
        Limpa todos os registros acumulados.
        """
        self.records.clear()

    def to_frame(self) -> pd.DataFrame:
        """
        Consolida as exclusões em um DataFrame deduplicado.

        :return: DataFrame ordenado por etapa, fonte, motivo
        """
        if not self.records:
            return pd.DataFrame(columns=["etapa", "fonte", "bairro_raw", "motivo"])
        df = pd.DataFrame(self.records).drop_duplicates()
        return df.sort_values(["etapa", "fonte", "motivo", "bairro_raw"]).reset_index(drop=True)


# Instância global - compartilhada por todos os módulos do ETL
EXCLUSOES = ExclusaoRegistry()


def _detect_separator(
    path: Path,
    encoding: str
) -> str:
    """
    Detecta o separador do CSV com Sniffer, com fallback por contagem.

    :param path: Caminho para o CSV
    :param encoding: Encoding já validado
    :return: Caractere separador detectado
    """
    with open(path, "r", encoding=encoding) as f:
        sample = f.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters="".join(SEPARATORS)).delimiter
    except csv.Error:
        first_line = sample.splitlines()[0] if sample else ""
        counts = {sep: first_line.count(sep) for sep in SEPARATORS}
        return max(counts, key=counts.get) if max(counts.values()) > 0 else ","


def load_csv(
    path: Path,
    label: str = ""
) -> pd.DataFrame:
    """
    Carrega CSV detectando encoding e separador automaticamente.

    :param path: Caminho para o CSV
    :param label: Rótulo opcional para logging
    :return: DataFrame com colunas em MAIÚSCULAS e valores como string
    :raises ValueError: Se não conseguir decodificar ou parsear
    """
    last_error: Exception | None = None
    for encoding in ENCODINGS:
        try:
            sep = _detect_separator(path, encoding)
            df = pd.read_csv(
                path, encoding=encoding, sep=sep,
                dtype=str, low_memory=False, on_bad_lines="skip",
            )
            df.columns = df.columns.str.strip().str.upper()
            log.info(
                "%s carregado: %d linhas x %d colunas (encoding=%s, sep=%r)",
                label or path.name, len(df), len(df.columns), encoding, sep,
            )
            return df
        except (UnicodeDecodeError, pd.errors.ParserError) as exc:
            last_error = exc
            continue
    raise ValueError(f"Falha ao ler CSV {path}: {last_error}")


def normalize_bairro(series: pd.Series) -> pd.Series:
    """
    Normaliza nomes de bairro: strip, upper, remove acentos via NFD.

    :param series: Série com nomes em qualquer formato
    :return: Série normalizada (MAIÚSCULAS ASCII sem acento)
    """
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
    )


def find_column(
    df: pd.DataFrame,
    *candidates: str
) -> str | None:
    """
    Retorna a primeira coluna do DataFrame que bate com algum candidato.

    :param df: DataFrame com colunas já normalizadas
    :param candidates: Nomes possíveis em ordem de preferência
    :return: Nome da coluna encontrada, ou None
    """
    cols_upper = {c.upper(): c for c in df.columns}
    for candidate in candidates:
        if candidate.upper() in cols_upper:
            return cols_upper[candidate.upper()]
    return None


def load_bairros_canonicos(eco_csv_path: Path) -> list[str]:
    """
    Carrega a lista canônica de nomes populares de bairros de BH.
    Extrai de NOME_BAIRRO em atividade_economica.csv - fonte mais completa
    e consistente que bairros.csv (que só tem subdivisões administrativas).

    :param eco_csv_path: Caminho para CSV com coluna NOME_BAIRRO
    :return: Lista ordenada de nomes canônicos únicos já normalizados
    :raises FileNotFoundError: Se o arquivo não existir
    :raises ValueError: Se nenhuma coluna candidata for encontrada
    """
    if not eco_csv_path.exists():
        raise FileNotFoundError(f"Fonte canônica não encontrada: {eco_csv_path}")

    df = load_csv(eco_csv_path, "fonte_canonica")
    col_nome = find_column(df, *BAIRROS_CANONICOS_CANDIDATOS)
    if col_nome is None:
        raise ValueError(
            f"Nenhuma coluna de nome encontrada em {eco_csv_path.name}. "
            f"Esperado: {BAIRROS_CANONICOS_CANDIDATOS}. "
            f"Disponíveis: {df.columns.tolist()[:15]}"
        )

    nomes = normalize_bairro(df[col_nome])
    canonicos = sorted(set(nomes[nomes != ""].tolist()))
    log.info("Tabela canônica: %d bairros (fonte=%s)", len(canonicos), eco_csv_path.name)
    return canonicos


def match_bairro_canonico(
    series: pd.Series,
    canonicos: list[str],
    etapa: str,
    fonte: str,
    score_cutoff: int = FUZZY_SCORE_CUTOFF
) -> pd.Series:
    """
    Mapeia nomes contra a tabela canônica via fuzzy match vetorizado.
    Para cada nome único: tenta match exato, cai para rapidfuzz.WRatio.
    Nomes abaixo do cutoff retornam NaN e são registrados em EXCLUSOES.

    :param series: Série com nomes em qualquer formato
    :param canonicos: Lista de nomes canônicos já normalizados
    :param etapa: Nome da etapa do ETL (para auditoria em EXCLUSOES)
    :param fonte: Nome do dataset de origem (para auditoria em EXCLUSOES)
    :param score_cutoff: Score mínimo aceito (0-100)
    :return: Série com nomes canônicos; NaN para não identificáveis
    """
    from rapidfuzz import fuzz, process

    norm = normalize_bairro(series)
    canonicos_set = set(canonicos)
    unique_names = [x for x in norm.dropna().unique() if x != ""]

    mapping: dict[str, str | None] = {}
    nao_matched: list[str] = []
    n_exatos = n_fuzzy = 0

    for nome in unique_names:
        if nome in canonicos_set:
            mapping[nome] = nome
            n_exatos += 1
            continue

        melhor = process.extractOne(
            nome, canonicos,
            scorer=fuzz.WRatio,
            score_cutoff=score_cutoff,
        )
        if melhor is not None:
            mapping[nome] = melhor[0]
            n_fuzzy += 1
        else:
            mapping[nome] = None
            nao_matched.append(nome)

    if nao_matched:
        EXCLUSOES.add_many(etapa, fonte, nao_matched, motivo="score_baixo")

    log.info(
        "Fuzzy match (%s/%s): %d exatos, %d aproximados, %d sem match (cutoff=%d)",
        etapa, fonte, n_exatos, n_fuzzy, len(nao_matched), score_cutoff,
    )
    return norm.map(mapping)


def save_exclusoes(out_path: Path) -> pd.DataFrame:
    """
    Persiste as exclusões acumuladas em CSV e retorna o DataFrame.

    :param out_path: Caminho do arquivo CSV de saída
    :return: DataFrame com as exclusões consolidadas
    """
    df = EXCLUSOES.to_frame()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")

    if df.empty:
        log.info("Nenhuma exclusão registrada - CSV vazio em %s", out_path)
        return df

    resumo = (
        df.groupby(["etapa", "fonte", "motivo"])
        .size()
        .reset_index(name="qtd")
    )
    log.info("Exclusões salvas em %s (%d registros)", out_path, len(df))
    log.info("Resumo por etapa/fonte/motivo:\n%s", resumo.to_string(index=False))
    return df