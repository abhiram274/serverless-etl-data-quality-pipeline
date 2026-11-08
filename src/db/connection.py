"""Database connectivity manager for PostgreSQL warehouse.

Supports standard production PostgreSQL via psycopg (psycopg3 or psycopg2)
and provides an integrated SQLite driver fallback for offline local verification.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Generator, Optional

logger = logging.getLogger(__name__)

# Check for psycopg or psycopg2
try:
    import psycopg
    PSYCOPG_AVAILABLE = True
except ImportError:
    try:
        import psycopg2 as psycopg
        PSYCOPG_AVAILABLE = True
    except ImportError:
        psycopg = None
        PSYCOPG_AVAILABLE = False


class DatabaseConnection:
    """Manages transactional database connections, supporting PostgreSQL and SQLite fallback."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        use_sqlite: bool = False,
        sqlite_db_path: str = "data/warehouse.db",
    ):
        self.host = host or os.getenv("RDS_HOST", "localhost")
        self.port = int(port or os.getenv("RDS_PORT", 5432))
        self.dbname = dbname or os.getenv("RDS_DB_NAME", "retail_warehouse")
        self.user = user or os.getenv("RDS_USERNAME", "postgres")
        self.password = password or os.getenv("RDS_PASSWORD", "")
        self.use_sqlite = use_sqlite or not PSYCOPG_AVAILABLE
        self.sqlite_db_path = sqlite_db_path

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Context manager yielding a transactional database connection with automatic commit/rollback."""
        if self.use_sqlite:
            os.makedirs(os.path.dirname(self.sqlite_db_path) or ".", exist_ok=True)
            conn = sqlite3.connect(self.sqlite_db_path)
            conn.execute("PRAGMA foreign_keys = ON;")
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Database transaction error: {e}")
                raise
            finally:
                conn.close()
        else:
            if not PSYCOPG_AVAILABLE:
                raise RuntimeError("psycopg is not installed. Install psycopg or use SQLite fallback.")
            conn = psycopg.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
            )
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"PostgreSQL transaction error: {e}")
                raise
            finally:
                conn.close()
