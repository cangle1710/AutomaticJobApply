"""Shared pytest fixtures for the ApplyPilot test suite.

Provides isolated database connections, sample profiles, and mock LLM
clients so that tests never touch the user's real ~/.applypilot data.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from applypilot.database import init_db, close_connection


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_db(tmp_path):
    """Yield (connection, db_path) backed by a temp SQLite file.

    Each test gets its own database, fully isolated from the user's real DB.
    The connection is closed automatically after the test.
    """
    db_path = tmp_path / "test.db"
    conn = init_db(db_path)
    yield conn, db_path
    close_connection(db_path)


@pytest.fixture()
def db_conn(tmp_db):
    """Shorthand: just the connection from tmp_db."""
    conn, _ = tmp_db
    return conn


@pytest.fixture()
def db_path(tmp_db):
    """Shorthand: just the path from tmp_db."""
    _, path = tmp_db
    return path


# ---------------------------------------------------------------------------
# Profile fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_profile():
    """Minimal valid user profile for validation tests."""
    return {
        "personal": {
            "full_name": "Jane Developer",
            "email": "jane@example.com",
            "phone": "555-123-4567",
        },
        "resume_facts": {
            "preserved_companies": ["Acme Corp", "Startup Inc"],
            "preserved_projects": ["OpenSourceLib"],
            "preserved_school": "State University",
        },
        "skills_boundary": {
            "languages": ["Python", "TypeScript", "JavaScript"],
            "frameworks": ["React", "FastAPI", "Flask"],
            "databases": ["PostgreSQL", "Redis"],
            "tools": ["Docker", "Git", "GitHub Actions"],
        },
    }


@pytest.fixture()
def sample_profile_path(tmp_path, sample_profile):
    """Write the sample profile to a temp file and return its path."""
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(sample_profile), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# LLM fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_llm_client():
    """Mock LLMClient with a configurable .chat() return value.

    Default response is a valid scoring result.  Override via::

        mock_llm_client.chat.return_value = "custom response"
    """
    client = MagicMock()
    client.chat.return_value = (
        "SCORE: 8\nKEYWORDS: Python, FastAPI\nREASONING: Strong match."
    )
    client.ask.return_value = "mocked response"
    return client
