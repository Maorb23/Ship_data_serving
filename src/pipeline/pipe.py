from __future__ import annotations

import logging
from pathlib import Path

from bl.bl import Bl
from elt_utils.db.db_ops import DBops
from elt_utils.transform.config import Config
from elt_utils.transform.delta import Delta
from elt_utils.transform.dqa import Dqa
from elt_utils.transform.publish import Publish

logger = logging.getLogger(__name__)


def run_pipe(config_path: str | None = None, source_db_override: str | None = None) -> dict:
    """
    Entry point for the pipeline. Orchestrates all stages in order:
    init → delta → dqa → bl → publish
    """

    # Load config with optional overrides
    default_config_path = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
    cfg = Config(config_path or str(default_config_path)).configs

    if source_db_override:
        source_path = Path(source_db_override)
        if not source_path.is_absolute():
            source_path = (Path(cfg["project_root"]) / source_path).resolve()
        cfg["pipeline"]["source_db"] = str(source_path)

    logger.info(
        "Pipeline starting | source_db=%s | target_db=%s",
        cfg["pipeline"]["source_db"],
        cfg["pipeline"]["target_db"],
    )
    # Validate config and SQL files before starting pipeline
    db_ops = DBops(
        db_path=cfg["pipeline"]["source_db"],
        sql_dir=cfg["pipeline"]["sql_dir"],
        target_db_path=cfg["pipeline"]["target_db"],
    )

    try:
        # Initialize target tables if they don't exist
        Publish.initialize_tables(db_ops, cfg)
        logger.info("Target tables initialized")

        # Extract delta from source DB
        delta_df = Delta.delta(db_ops, cfg)
        logger.info("Delta extracted | rows=%d", len(delta_df))

        # Perform DQA on the delta, separating clean rows from rejects
        clean_df, rejects_df = Dqa.dqa(delta_df, cfg)
        logger.info("DQA complete | clean_rows=%d | rejected_rows=%d", len(clean_df), len(rejects_df))

        # Fetch lookback data, meaning the most recent rows from the target table for each ship, 
        # to use in enrichment calculations
        lookback_df = Delta.fetch_target_lookback(db_ops, cfg)
        logger.info("Lookback fetched | rows=%d", len(lookback_df))

        # Calculate enrichments such as ROT and acceleration using the clean 
        # delta and lookback data
        enriched_df = Bl.calc_enrichments(clean_df, lookback_df)
        logger.info("Enrichment complete | rows=%d", len(enriched_df))

        # Publish clean, enriched rows to target table and rejected rows to rejects table
        publish_stats = Publish.publish(db_ops, enriched_df, rejects_df, cfg)

        summary = {
            "source_db": cfg["pipeline"]["source_db"],
            "target_db": cfg["pipeline"]["target_db"],
            "delta_rows": int(len(delta_df)),
            "clean_rows": int(len(clean_df)),
            "rejected_rows": int(len(rejects_df)),
            "published_rows": int(publish_stats["inserted_rows"]),
        }

        logger.info(
            "Publish complete | published_rows=%d | rejected_rows=%d",
            summary["published_rows"],
            summary["rejected_rows"],
        )
        logger.info("Pipeline completed successfully")
        return summary
    finally:
        db_ops.close()
