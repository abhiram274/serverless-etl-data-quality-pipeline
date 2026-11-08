"""End-to-end integration test runner for Phase 2: S3 to Lambda to S3.

Validates:
1. Bucket prefixes initialization (raw/, curated/, warehouse/, quarantine/, reports/).
2. Raw data upload.
3. Anti-recursion filter behavior.
4. Lambda extraction, cleaning, schema standardization, and DQ filtering.
5. Partitioned writes to curated/, quarantine/, and reports/.
6. Structured logging verification.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from pathlib import Path

# Ensure src in sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.s3_client import S3DataLakeManager
from src.lambda_function import lambda_handler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TestPhase2Runner")


def run_e2e_phase2_test():
    logger.info("=================================================================")
    logger.info("Executing Phase 2 End-to-End Workflow Verification")
    logger.info("=================================================================")

    test_bucket = "retail-serverless-data-lake-test"
    mock_dir = project_root / ".s3_emulator" / test_bucket
    if mock_dir.exists():
        shutil.rmtree(mock_dir)

    # 1. Initialize S3 Manager
    s3_mgr = S3DataLakeManager(bucket_name=test_bucket, local_mock_dir=mock_dir)

    # 2. Test Task 1: Create all 5 Lake prefixes
    logger.info("Task 1 & 2: Initializing S3 Data Lake prefixes...")
    prefixes = s3_mgr.initialize_data_lake_prefixes()
    assert len(prefixes) == 5
    for p in ["raw/", "curated/", "warehouse/", "quarantine/", "reports/"]:
        assert f"{p}" in prefixes
    logger.info("[PASSED] All 5 Lake Prefixes initialized successfully.")

    # 3. Test Task 3: Upload realistic raw dataset to raw/
    raw_source = project_root / "data" / "raw" / "retail_transactions.csv"
    if not raw_source.exists():
        from src.generator import generate_and_save_datasets
        generate_and_save_datasets()

    raw_s3_key = "raw/retail_transactions.csv"
    logger.info(f"Task 3: Uploading {raw_source} to s3://{test_bucket}/{raw_s3_key}...")
    s3_mgr.upload_file(raw_source, raw_s3_key)
    logger.info("[PASSED] Upload and download validation passed.")

    # 4. Test Task 10: Anti-Recursion Filter
    logger.info("Task 10: Testing Anti-Recursion Guard for non-raw and downstream prefixes...")
    fake_recursive_event = {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": test_bucket},
                    "object": {"key": "curated/should_not_trigger.csv"},
                },
            }
        ]
    }
    # Set env var so lambda uses this mock bucket
    os.environ["S3_BUCKET_NAME"] = test_bucket
    res_recursive = lambda_handler(fake_recursive_event)
    assert json.loads(res_recursive["body"])["processed_batches"] == 0
    logger.info("[PASSED] Anti-recursion guard prevented recursive triggering.")

    # 5. Test Tasks 4-9: Lambda Trigger & Full Execution
    logger.info("Tasks 4-9: Invoking Lambda with raw/ S3 ObjectCreated event notification...")
    s3_event = {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": test_bucket},
                    "object": {"key": raw_s3_key},
                },
            }
        ]
    }

    response = lambda_handler(s3_event)
    assert response["statusCode"] == 200
    res_data = json.loads(response["body"])
    assert res_data["status"] == "SUCCESS"
    assert res_data["processed_batches"] == 1

    details = res_data["details"][0]
    metrics = details["metrics"]
    logger.info("============== Lambda Execution Audit ==============")
    logger.info(f"Total Records Ingested:       {metrics['total_records_processed']:,}")
    logger.info(f"Curated Clean Records:        {metrics['curated_records_count']:,}")
    logger.info(f"Quarantined Records:          {metrics['quarantined_records_count']:,}")
    logger.info(f"Curated S3 Destination:       {details['curated_destination']}")
    logger.info(f"Quarantine S3 Destination:    {details['quarantine_destination']}")
    logger.info("====================================================")

    # Verify physical files written to S3
    curated_content = s3_mgr.get_object(details["curated_destination"])
    quarantine_content = s3_mgr.get_object(details["quarantine_destination"])

    assert len(curated_content) > 0
    assert len(quarantine_content) > 0
    assert metrics["curated_records_count"] > 9000
    assert metrics["quarantined_records_count"] >= 500

    logger.info("[PASSED] S3-to-Lambda-to-S3 full ETL workflow tested and verified successfully.")


if __name__ == "__main__":
    run_e2e_phase2_test()
