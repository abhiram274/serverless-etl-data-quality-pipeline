"""S3 Storage Client and utilities for data lake ingestion and retrieval.

Supports standard AWS S3 execution via boto3 when credentials and boto3 are available,
and includes a transparent local filesystem fallback emulator for offline testing and staging.
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Transparent boto3 import check
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    boto3 = None
    ClientError = Exception
    BOTO3_AVAILABLE = False


class S3DataLakeManager:
    """Manages S3 bucket operations, lifecycle prefixes, uploads, downloads, and encryption/versioning."""

    DEFAULT_PREFIXES = [
        "raw/",
        "curated/",
        "warehouse/",
        "quarantine/",
        "reports/",
    ]

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region_name: str = "us-east-1",
        local_mock_dir: Optional[str | Path] = None,
    ):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "retail-serverless-data-lake")
        self.region_name = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.local_mock_dir = Path(local_mock_dir) if local_mock_dir else None

        # Determine whether to use real AWS S3 client or offline emulator
        if BOTO3_AVAILABLE and not self.local_mock_dir:
            self._s3_client = boto3.client("s3", region_name=self.region_name)
        else:
            self._s3_client = None

    def initialize_data_lake_prefixes(self) -> List[str]:
        """Creates standard lake directory markers for all five layers."""
        created_prefixes = []
        for prefix in self.DEFAULT_PREFIXES:
            key = prefix if prefix.endswith("/") else f"{prefix}/"
            self.put_object(
                key=key,
                body=b"",
                content_type="application/x-directory",
            )
            created_prefixes.append(key)
            logger.info(f"Initialized data lake prefix: s3://{self.bucket_name}/{key}")
        return created_prefixes

    def put_object(
        self,
        key: str,
        body: bytes | str | io.BytesIO,
        content_type: str = "text/csv",
        metadata: Optional[Dict[str, str]] = None,
        server_side_encryption: str = "AES256",
    ) -> Dict[str, Any]:
        """Uploads raw bytes or text to S3 with AES256 server-side encryption."""
        if isinstance(body, str):
            payload = body.encode("utf-8")
        elif isinstance(body, io.BytesIO):
            payload = body.getvalue()
        else:
            payload = body

        if self._s3_client:
            kwargs: Dict[str, Any] = {
                "Bucket": self.bucket_name,
                "Key": key,
                "Body": payload,
                "ContentType": content_type,
                "ServerSideEncryption": server_side_encryption,
            }
            if metadata:
                kwargs["Metadata"] = metadata
            response = self._s3_client.put_object(**kwargs)
            return response
        else:
            # Emulated local S3 filesystem operation
            base_dir = self.local_mock_dir or Path(".s3_emulator") / self.bucket_name
            if key.endswith("/"):
                target_dir = base_dir / key
                target_dir.mkdir(parents=True, exist_ok=True)
                return {
                    "ETag": '"directory-marker"',
                    "ServerSideEncryption": server_side_encryption,
                    "MockPath": str(target_dir),
                }

            target_path = base_dir / key
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(payload)
            return {
                "ETag": f'"{hash(payload)}"',
                "ServerSideEncryption": server_side_encryption,
                "MockPath": str(target_path),
            }

    def upload_file(
        self,
        local_file_path: str | Path,
        s3_key: str,
        content_type: str = "text/csv",
        server_side_encryption: str = "AES256",
    ) -> bool:
        """Uploads a local disk file to S3."""
        file_path = Path(local_file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Local file does not exist: {file_path}")

        data = file_path.read_bytes()
        self.put_object(
            key=s3_key,
            body=data,
            content_type=content_type,
            server_side_encryption=server_side_encryption,
        )
        logger.info(f"Uploaded {file_path} to s3://{self.bucket_name}/{s3_key}")
        return True

    def get_object(self, key: str) -> bytes:
        """Retrieves raw object bytes from S3."""
        if self._s3_client:
            response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response["Body"].read()
        else:
            base_dir = self.local_mock_dir or Path(".s3_emulator") / self.bucket_name
            target_path = base_dir / key
            if not target_path.exists():
                raise FileNotFoundError(f"Key s3://{self.bucket_name}/{key} not found at {target_path}")
            return target_path.read_bytes()

    def download_file(self, s3_key: str, local_destination_path: str | Path) -> Path:
        """Downloads an S3 object to local disk storage."""
        data = self.get_object(s3_key)
        dest_path = Path(local_destination_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(data)
        logger.info(f"Downloaded s3://{self.bucket_name}/{s3_key} to {dest_path}")
        return dest_path

    def list_objects(self, prefix: str = "") -> List[str]:
        """Lists all keys in the bucket matching the given prefix."""
        if self._s3_client:
            paginator = self._s3_client.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for item in page.get("Contents", []):
                    keys.append(item["Key"])
            return keys
        else:
            base_dir = self.local_mock_dir or Path(".s3_emulator") / self.bucket_name
            target_dir = base_dir / prefix
            if not target_dir.exists():
                return []
            if target_dir.is_file():
                return [prefix]
            return [str(p.relative_to(base_dir)).replace("\\", "/") for p in base_dir.rglob("*") if p.is_file()]
