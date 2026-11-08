"""Main orchestration entry point for Phase 1 of the Serverless ETL & Data Quality Pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Add project root to sys.path if not present
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.generator import generate_and_save_datasets
from src.explorer import generate_statistics_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Phase1Pipeline")


def run_phase1_pipeline(
    raw_path: str = "data/raw/retail_transactions.csv",
    clean_path: str = "data/clean/retail_transactions_clean.csv",
    report_path: str = "reports/statistics_report.json",
    record_count: int = 10_200,
    seed: int = 42,
) -> None:
    """Executes end-to-end Phase 1 execution: generation, corruption injection, and statistical exploration."""
    logger.info("=================================================================")
    logger.info("Starting Phase 1: Serverless ETL & Data Quality Pipeline")
    logger.info("=================================================================")

    # Step 1: Generate clean reference and corrupted raw dataset
    logger.info(f"Step 1: Generating datasets (Target Records: {record_count})...")
    raw_df, clean_df = generate_and_save_datasets(
        raw_output_path=raw_path,
        clean_output_path=clean_path,
        record_count=record_count,
        seed=seed,
    )

    logger.info(f"-> Generated {len(raw_df):,} raw records with injected data quality issues.")
    logger.info(f"-> Generated {len(clean_df):,} pristine clean reference records.")

    # Step 2: Run exploration and generate JSON statistics report
    logger.info("Step 2: Performing data quality exploration & generating profiling report...")
    report = generate_statistics_report(
        raw_csv_path=raw_path,
        clean_csv_path=clean_path,
        report_output_path=report_path,
    )

    summary = report["summary"]
    logger.info("================== Quality Audit Summary ==================")
    logger.info(f"Raw Records:                {summary['raw_total_records']:,}")
    logger.info(f"Duplicates Injected:        {summary['raw_anomalies_detected']['duplicates']:,}")
    logger.info(f"Missing Values:             {summary['raw_anomalies_detected']['missing_values']:,}")
    logger.info(f"Invalid Amounts:            {summary['raw_anomalies_detected']['invalid_amounts']:,}")
    logger.info(f"Malformed Dates:            {summary['raw_anomalies_detected']['malformed_dates']:,}")
    logger.info(f"Clean Reference Anomalies:  {sum(summary['clean_anomalies_detected'].values())} (Pristine)")
    logger.info("===========================================================")
    logger.info(f"Report JSON successfully saved to: {report_path}")
    logger.info("Phase 1 execution completed successfully.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1: Serverless ETL & Data Quality Pipeline Orchestrator")
    parser.add_argument("--records", type=int, default=10_200, help="Total records to generate (default: 10200)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--raw-path", type=str, default="data/raw/retail_transactions.csv")
    parser.add_argument("--clean-path", type=str, default="data/clean/retail_transactions_clean.csv")
    parser.add_argument("--report-path", type=str, default="reports/statistics_report.json")
    args = parser.parse_args()

    run_phase1_pipeline(
        raw_path=args.raw_path,
        clean_path=args.clean_path,
        report_path=args.report_path,
        record_count=args.records,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
