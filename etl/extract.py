"""
Módulo de extração: baixa todas as fontes do portal de dados abertos da PBH.

Estratégia em duas camadas:
1. Tenta resolver a URL mais recente via API CKAN (com headers de browser)
2. Se a API falhar, usa URL direta hardcoded como fallback garantido

Os IDs de fallback foram obtidos diretamente das páginas do portal em abril/2026.
O download é ignorado se o arquivo já existir (idempotente).
"""

import logging
from pathlib import Path
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

CKAN_BASE = "https://dados.pbh.gov.br/api/3/action"
CKAN_DOWNLOAD = "https://ckan.pbh.gov.br/dataset"

# Headers que simulam um browser — portal bloqueia requisições sem User-Agent
# (retorna 403 para chamadas de scripts puros)
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# Estrutura de cada fonte:
# "nome_pasta": (
#     dataset_id_ckan,       # usado para resolver URL via API
#     fallback_dataset_uuid, # UUID interno do CKAN (da URL do arquivo)
#     fallback_resource_uuid,# UUID do recurso específico
#     fallback_filename,     # nome do arquivo no servidor
#     local_filename,        # nome salvo em data/raw/
# )
#
# URLs de fallback seguem o padrão:
#   https://ckan.pbh.gov.br/dataset/<dataset_uuid>/resource/<resource_uuid>/download/<filename>
#
# IDs coletados diretamente do portal em 18/04/2026.
SOURCES: dict[str, tuple[str, str | None, str | None, str | None, str]] = {
    "atividade_economica": (
        "atividade-economica",
        "33e9dcb1-126f-4cde-8c80-d1927a965430",
        "483693ac-66ff-4a27-9b57-4dbb3b174605",
        "20260401_atividade_economica.csv",
        "atividade_economica.csv",
    ),
    "bairros": (
        "bairro-oficial",
        "d1432f63-6515-43ec-a6bb-ba548af00026",
        "b792f0a3-3a93-4ec5-baac-9d23d5757e4b",
        "20230201_bairro_oficial.csv",
        "bairros.csv",
    ),
    "pontos_onibus": (
        "ponto-de-onibus",
        "495b1cf7-4f9c-4f73-8686-82b0e8d5f95a",
        "a0fdf829-b247-49ce-bad3-291f9722219a",
        "20251001_ponto_onibus.csv",
        "pontos_onibus.csv",
    ),
    "embarque_por_ponto": (
        "estimativa-de-embarque-nos-pontos-de-parada",
        None,
        None,
        None,
        "embarque_por_ponto.csv",
    ),
    "acidentes_transito": (
        "relacao-de-ocorrencias-de-acidentes-de-transito-com-vitima",
        None,
        None,
        None,
        "acidentes_transito.csv",
    ),
    "parques": (
        "parques-municipais",
        None,
        None,
        None,
        "parques.csv",
    ),
    "equipamentos_esportivos": (
        "equipamento-esportivo",
        "3ad662aa-0449-43bb-82fc-50bdddb6b964",
        "354dad45-4fc8-411c-9c36-f701849b2da4",
        "20230301_equipamento_esportivo.csv",
        "equipamentos_esportivos.csv",
    ),
    "matriz_od": (
        "matriz-origem-destino",
        None,
        None,
        None,
        "matriz_od.csv",
    ),
}


def _get_latest_csv_url(dataset_id: str) -> str | None:
    """
    Consulta a API CKAN com headers de browser e retorna a URL do CSV mais recente.

    :param dataset_id: Identificador do dataset no portal da PBH
    :return: URL do arquivo CSV ou None se não encontrado
    """
    url = f"{CKAN_BASE}/package_show"
    try:
        resp = requests.get(
            url,
            params={"id": dataset_id},
            headers=BROWSER_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        resources: list[dict] = resp.json()["result"]["resources"]
        csv_resources = [
            r for r in resources
            if r.get("format", "").upper() == "CSV" and r.get("url")
        ]
        if not csv_resources:
            return None
        # Recurso mais recente = último da lista (convenção do CKAN da PBH)
        return csv_resources[-1]["url"]
    except Exception as exc:
        log.warning("API CKAN falhou para '%s': %s", dataset_id, exc)
        return None


def _build_fallback_url(
    dataset_uuid: str,
    resource_uuid: str,
    filename: str,
) -> str:
    """
    Constrói a URL direta de download a partir dos UUIDs do CKAN.

    :param dataset_uuid: UUID do dataset no CKAN
    :param resource_uuid: UUID do recurso específico
    :param filename: Nome do arquivo no servidor
    :return: URL completa de download
    """
    return f"{CKAN_DOWNLOAD}/{dataset_uuid}/resource/{resource_uuid}/download/{filename}"


def _download(
    url: str,
    dest: Path
) -> bool:
    """
    Faz download de um arquivo para o caminho destino usando streaming.

    :param url: URL pública do arquivo
    :param dest: Caminho local de destino
    :return: True se o download foi realizado, False em caso de erro
    """
    try:
        with requests.get(url, stream=True, headers=BROWSER_HEADERS, timeout=120) as resp:
            resp.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)
        size_mb = dest.stat().st_size / 1024 / 1024
        log.info("  Download concluído: %.2f MB", size_mb)
        return True
    except Exception as exc:
        log.error("Erro ao baixar '%s': %s", url, exc)
        return False


def extract_all(force: bool = False) -> dict[str, Path]:
    """
    Baixa todas as fontes configuradas em SOURCES para data/raw/.
    Tenta a API CKAN primeiro; se falhar, usa URL direta de fallback.
    Pula o download se o arquivo já existir, a menos que force=True.

    :param force: Se True, sobrescreve arquivos já existentes
    :return: Dicionário mapeando nome da fonte para o caminho local do arquivo
    """
    results: dict[str, Path] = {}

    for source_name, (dataset_id, d_uuid, r_uuid, fallback_fname, local_fname) in SOURCES.items():
        dest = RAW_DIR / source_name / local_fname

        if dest.exists() and not force:
            log.info("Já existe, pulando: %s", dest)
            results[source_name] = dest
            continue

        # Tentativa 1: API CKAN dinâmica
        log.info("Resolvendo URL via API CKAN para '%s'...", source_name)
        url = _get_latest_csv_url(dataset_id)

        # Tentativa 2: URL direta hardcoded
        if not url and d_uuid and r_uuid and fallback_fname:
            url = _build_fallback_url(d_uuid, r_uuid, fallback_fname)
            log.info("Usando URL de fallback para '%s'", source_name)

        if not url:
            log.warning(
                "Nenhuma URL disponível para '%s' — adicione o fallback_uuid manualmente.",
                source_name,
            )
            continue

        log.info("Baixando '%s'...", source_name)
        if _download(url, dest):
            results[source_name] = dest
            log.info("✓ '%s' salvo em %s", source_name, dest)
        else:
            log.error("✗ Falha ao baixar '%s'", source_name)

    return results


if __name__ == "__main__":
    paths = extract_all()
    print("\nResumo:")
    for name, path in paths.items():
        size_mb = path.stat().st_size / 1024 / 1024
        print(f"  {name}: {size_mb:.2f} MB")