"""CloudWatch metrics publisher and Amazon SNS alerting manager.

Publishes custom CloudWatch metrics for record processing throughput and quality,
and dispatches incident notifications to Amazon SNS when failure/rejection thresholds trip.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    boto3 = None
    ClientError = Exception
    BOTO3_AVAILABLE = False


class CloudWatchMonitoringService:
    """Publishes custom pipeline metrics and evaluations to AWS CloudWatch."""

    NAMESPACE = "RetailServerlessETL"

    def __init__(self, region_name: str = "us-east-1", mock_mode: Optional[bool] = None):
        self.region_name = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.mock_mode = mock_mode if mock_mode is not None else not BOTO3_AVAILABLE
        self._cw_client = boto3.client("cloudwatch", region_name=self.region_name) if (BOTO3_AVAILABLE and not self.mock_mode) else None
        self.published_metrics: List[Dict[str, Any]] = []

    def publish_pipeline_metrics(
        self,
        batch_id: str,
        total_input: int,
        valid_records: int,
        rejected_records: int,
        duration_seconds: float,
        error_count: int = 0,
    ) -> Dict[str, Any]:
        """Publishes key operational telemetry metrics to CloudWatch namespace RetailServerlessETL."""
        pass_rate_pct = round((valid_records / total_input * 100), 2) if total_input > 0 else 0.0
        rejection_rate_pct = round((rejected_records / total_input * 100), 2) if total_input > 0 else 0.0

        metric_data = [
            {"MetricName": "TotalInputRecords", "Value": total_input, "Unit": "Count"},
            {"MetricName": "ValidCuratedRecords", "Value": valid_records, "Unit": "Count"},
            {"MetricName": "RejectedQuarantinedRecords", "Value": rejected_records, "Unit": "Count"},
            {"MetricName": "QualityPassRatePct", "Value": pass_rate_pct, "Unit": "Percent"},
            {"MetricName": "QualityRejectionRatePct", "Value": rejection_rate_pct, "Unit": "Percent"},
            {"MetricName": "ExecutionDurationSeconds", "Value": duration_seconds, "Unit": "Seconds"},
            {"MetricName": "PipelineExecutionErrors", "Value": error_count, "Unit": "Count"},
        ]

        dimensions = [{"Name": "BatchId", "Value": batch_id}, {"Name": "PipelineEnvironment", "Value": os.getenv("ENVIRONMENT", "production")}]
        for m in metric_data:
            m["Dimensions"] = dimensions

        if self._cw_client:
            self._cw_client.put_metric_data(Namespace=self.NAMESPACE, MetricData=metric_data)
            logger.info(f"Published {len(metric_data)} metrics to CloudWatch namespace '{self.NAMESPACE}'.")
        else:
            self.published_metrics.extend(metric_data)
            logger.info(f"[MOCK] Published {len(metric_data)} metrics to CloudWatch.")

        return {
            "namespace": self.NAMESPACE,
            "metrics_count": len(metric_data),
            "pass_rate_pct": pass_rate_pct,
            "rejection_rate_pct": rejection_rate_pct,
        }


class SNSNotificationService:
    """Manages publishing operational incident alerts and threshold warnings to Amazon SNS."""

    def __init__(
        self,
        topic_arn: Optional[str] = None,
        region_name: str = "us-east-1",
        rejection_threshold_pct: float = 5.0,
        mock_mode: Optional[bool] = None,
    ):
        self.topic_arn = topic_arn or os.getenv("SNS_ALERTS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:RetailETLAlertsTopic")
        self.region_name = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.rejection_threshold_pct = float(os.getenv("REJECTION_ALERT_THRESHOLD_PCT", rejection_threshold_pct))
        self.mock_mode = mock_mode if mock_mode is not None else not BOTO3_AVAILABLE
        self._sns_client = boto3.client("sns", region_name=self.region_name) if (BOTO3_AVAILABLE and not self.mock_mode) else None
        self.sent_notifications: List[Dict[str, Any]] = []

    def evaluate_and_notify(
        self,
        batch_id: str,
        total_input: int,
        rejected_count: int,
        error_message: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Evaluates pipeline metrics against SLA thresholds and sends alerts if breached."""
        rejection_pct = round((rejected_count / total_input * 100), 2) if total_input > 0 else 0.0

        subject = None
        message_body = None

        if error_message:
            subject = f"CRITICAL: ETL Pipeline Execution Failure [Batch: {batch_id}]"
            message_body = (
                f"Retail Serverless ETL Pipeline Failure Detected!\n"
                f"Batch ID: {batch_id}\n"
                f"Error: {error_message}\n"
                f"Input Records: {total_input}\n"
                f"Action Required: Check CloudWatch Logs group /aws/lambda/RetailServerlessETLFunction."
            )
        elif rejection_pct > self.rejection_threshold_pct:
            subject = f"ALERT: High Data Quality Rejection Rate ({rejection_pct}%) [Batch: {batch_id}]"
            message_body = (
                f"Data Quality Rejection Alert!\n"
                f"Batch ID: {batch_id}\n"
                f"Total Ingested: {total_input}\n"
                f"Quarantined Records: {rejected_count}\n"
                f"Rejection Rate: {rejection_pct}% (Threshold: {self.rejection_threshold_pct}%)\n"
                f"Action Required: Inspect S3 quarantine/ directory and audit JSON report."
            )

        if subject and message_body:
            notification = {
                "TopicArn": self.topic_arn,
                "Subject": subject,
                "Message": message_body,
            }
            if self._sns_client:
                self._sns_client.publish(**notification)
                logger.warning(f"Dispatched SNS alert: {subject}")
            else:
                self.sent_notifications.append(notification)
                logger.warning(f"[MOCK] Dispatched SNS alert: {subject}")
            return notification
        return None
