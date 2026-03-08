"""
Main entry point for running the ship positions ELT pipeline.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.pipe import run_pipe


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the ship positions ELT pipeline.")
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "src" / "config" / "config.yaml"),
        help="Path to pipeline config YAML.",
    )
    parser.add_argument(
        "--source-db",
        default=None,
        help="Optional source DB override (relative to project root or absolute path).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Console log level.",
    )

    args = parser.parse_args()
    _configure_logging(args.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting pipeline")

    summary = run_pipe(config_path=args.config, source_db_override=args.source_db)
    logger.info("Pipeline summary: %s", summary)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()