"""
Módulo de extração: baixa todas as fontes do portal de dados abertos da PBH via API CKAN.

Cada função retorna o caminho local do arquivo salvo.
O download é ignorado se o arquivo já existir (idempotente).
"""

import logging
import os
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

# Dataset IDs e resource IDs da API CKAN da PBH.
# Cada entrada segue o padrão:
#   "nome_pasta": ("dataset_id", "resource_id", "nome_arquivo_local")
#
# Para encontrar os IDs: GET https://dados.pbh.gov.br/api/3/action/package_show?id=<dataset_id>
SOURCES: dict[str, tuple[str, str, str]] = {
    "atividade_economica": (
        "atividade-economica",
        None,   # resolvido dinamicamente — pega o recurso CSV mais recente
        "atividade_economica.csv",
    ),
    "bairros": (
        "bairro-oficial",
        None,
        "bairros.csv",
    ),
    "pontos_onibus": (
        "ponto-de-onibus",
        None,
        "pontos_onibus.csv",
    ),
    "embarque_por_ponto": (
        "estimativa-de-embarque-nos-pontos-de-parada",
        None,
        "embarque_por_ponto.csv",
    ),
    "acidentes_transito": (
        "relacao-de-ocorrencias-de-acidentes-de-transito-com-vitima",
        None,
        "acidentes_transito.csv",
    ),
    "parques": (
        "parques-municipais",
        None,
        "parques.csv",
    ),
    "equipamentos_esportivos": (
        "equipamentos-esportivos",
        None,
        "equipamentos_esportivos.csv",
    ),
    "matriz_od": (
        "matriz-origem-destino",
        None,
        "matriz_od.csv",
    ),
}

CKAN_BASE = "https://dados.pbh.gov.br/api/3/action"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_latest_csv_url(dataset_id: str) -> str | None:
    """
    Consulta a API CKAN e retorna a URL do recurso CSV mais recente do dataset.

    :param dataset_id: Identificador do dataset no portal da PBH
    :return: URL do arquivo CSV ou None se não encontrado
    """
    url = f"{CKAN_BASE}/package_show"
    try:
        resp = requests.get(url, params={"id": dataset_id}, timeout=30)
        resp.raise_for_status()
        resources: list[dict] = resp.json()["result"]["resources"]
        csv_resources = [
            r for r in resources
            if r.get("format", "").upper() == "CSV" and r.get("url")
        ]
        if not csv_resources:
            return None
        # Prefere o recurso mais recente (último da lista, convenção do CKAN da PBH)
        return csv_resources[-1]["url"]
    except Exception as exc:
        log.warning("Falha ao consultar dataset '%s': %s", dataset_id, exc)
        return None


def _download(url: str, dest: Path) -> bool:
    """
    Faz download de um arquivo para o caminho destino usando streaming.

    :param url: URL pública do arquivo
    :param dest: Caminho local de destino
    :return: True se o download foi realizado, False em caso de erro
    """
    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)
        return True
    except Exception as exc:
        log.error("Erro ao baixar '%s': %s", url, exc)
        return False


# ---------------------------------------------------------------------------
# Interface pública
# ---------------------------------------------------------------------------

def extract_all(force: bool = False) -> dict[str, Path]:
    """
    Baixa todas as fontes configuradas em SOURCES para data/raw/.

    Pula o download se o arquivo já existir, a menos que force=True.

    :param force: Se True, sobrescreve arquivos já existentes
    :return: Dicionário mapeando nome da fonte para o caminho local do arquivo
    """
    results: dict[str, Path] = {}

    for source_name, (dataset_id, resource_id, filename) in SOURCES.items():
        dest = RAW_DIR / source_name / filename

        if dest.exists() and not force:
            log.info("Já existe, pulando: %s", dest)
            results[source_name] = dest
            continue

        log.info("Resolvendo URL para '%s'...", source_name)
        url = resource_id or _get_latest_csv_url(dataset_id)

        if not url:
            log.warning("URL não encontrada para '%s', pulando.", source_name)
            continue

        log.info("Baixando '%s' → %s", source_name, dest)
        if _download(url, dest):
            results[source_name] = dest
            log.info("✓ '%s' salvo em %s", source_name, dest)
        else:
            log.error("✗ Falha ao baixar '%s'", source_name)

    return results


if __name__ == "__main__":
    paths = extract_all()
    for name, path in paths.items():
        size_mb = path.stat().st_size / 1024 / 1024
        print(f"  {name}: {path} ({size_mb:.2f} MB)")
