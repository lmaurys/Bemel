import os
from typing import Optional


def _load_env_files() -> None:
    """Load simple KEY=VALUE pairs from .env files if present and not already set.

    Supported locations (first found wins for each key):
      - <repo-root>/.env
      - <repo-root>/tools/.env
    """
    here = os.path.abspath(os.path.dirname(__file__))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    candidates = [
        os.path.join(root, ".env"),
        os.path.join(root, "tools", ".env"),
    ]

    def load_file(path: str) -> None:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    # Do not override if already present in the environment
                    if key and key not in os.environ:
                        os.environ[key] = val
        except FileNotFoundError:
            return

    for p in candidates:
        load_file(p)


# Attempt to load environment from .env files before reading settings
_load_env_files()


class Settings:
    """Environment-driven settings for Azure SQL and ETL behavior."""

    # Full connection string (optional). If set, overrides discrete settings below.
    connection_string = os.getenv('AZURE_SQL_CONNECTION_STRING')

    # Discrete connection settings (used when connection_string not provided)
    server = os.getenv('AZURE_SQL_SERVER')       # e.g., dbbemel.database.windows.net
    database = os.getenv('AZURE_SQL_DATABASE')   # e.g., dbbemel
    user = os.getenv('AZURE_SQL_USER')           # for SQL auth
    password = os.getenv('AZURE_SQL_PASSWORD')   # for SQL auth
    driver = os.getenv('AZURE_SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
    authentication = os.getenv('AZURE_SQL_AUTHENTICATION')  # e.g., 'ActiveDirectoryDefault' or 'SqlPassword'

    # General
    schema = os.getenv('DWH_SCHEMA', 'Dwh2')
    xml_root = os.getenv('XML_ROOT')  # optional override; defaults to repo XMLS_COL if None


def build_connection_string() -> Optional[str]:
    """Build a pyodbc-compatible connection string from environment variables."""
    if Settings.connection_string:
        return Settings.connection_string
    if not Settings.server or not Settings.database:
        return None
    # Base parts
    parts = [
        f"DRIVER={{{{}}}}".format(Settings.driver),
        f"SERVER=tcp:{Settings.server},1433",
        f"DATABASE={Settings.database}",
        "Encrypt=Yes",
        "TrustServerCertificate=No",
        "Connection Timeout=30",
    ]
    if Settings.authentication:
        parts.append(f"Authentication={Settings.authentication}")
    # If SQL auth is intended, include UID/PWD
    if Settings.user and Settings.password:
        parts.append(f"UID={Settings.user}")
        parts.append(f"PWD={Settings.password}")
    return ";".join(parts)


def has_db_settings() -> bool:
    return build_connection_string() is not None
