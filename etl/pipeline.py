"""
Orquestrador do pipeline ETL completo.

Executa as etapas em ordem:
1. Extract  — baixa todas as fontes do portal da PBH
2. Transform — processa cada dimensão (econômica, acessibilidade, qualidade, O-D)
3. Score    — compõe o score final por bairro

Uso:
    python -m etl.pipeline              # extrai + transforma tudo
    python -m etl.pipeline --skip-extract  # pula download (dados já em raw/)
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from etl.extract import extract_all
from etl.transform import acessibilidade, economico, matriz_od, qualidade_urbana, score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("pipeline")

# Adiciona a raiz do projeto ao path para imports relativos funcionarem
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _step(
    name: str,
    fn,
    *args,
    **kwargs
) -> any:
    """
    Executa uma etapa do pipeline com logging de tempo e tratamento de erro.

    :param name: Nome da etapa para exibição no log
    :param fn: Função a executar
    :return result: Resultado da função executada
    """
    log.info("=" * 60)
    log.info("ETAPA: %s", name)
    log.info("=" * 60)
    t0 = time.time()
    try:
        result = fn(*args, **kwargs)
        elapsed = time.time() - t0
        log.info("✓ '%s' concluída em %.1fs", name, elapsed)
        return result
    except Exception as exc:
        log.error("✗ Falha em '%s': %s", name, exc, exc_info=True)
        raise


def run(skip_extract: bool = False) -> None:
    """
    Executa o pipeline ETL completo.

    :param skip_extract: Se True, pula a etapa de download
    """
    t_total = time.time()
    log.info("Pipeline BH Investment Insights — iniciando")

    if not skip_extract:
        _step("Extract — download das fontes", extract_all)
    else:
        log.info("Extract pulada (--skip-extract)")

    _step("Transform — Atividade Econômica", economico.run)
    _step("Transform — Acessibilidade Multimodal", acessibilidade.run)
    _step("Transform — Qualidade Urbana", qualidade_urbana.run)
    _step("Transform — Matriz O-D (PySpark)", matriz_od.run)
    _step("Compose — Score Final", score.run)

    elapsed_total = time.time() - t_total
    log.info("=" * 60)
    log.info("Pipeline concluído em %.1fs", elapsed_total)
    log.info("Saídas em: data/processed/")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL pipeline — BH Investment Insights")
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Pula o download das fontes (usa dados já em data/raw/)",
    )
    args = parser.parse_args()
    run(skip_extract=args.skip_extract)