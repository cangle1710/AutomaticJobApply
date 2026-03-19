"""Unit tests for applypilot.apply.launcher — reset_failed, acquire_job, and prompt bugs.

Tests cover:
- reset_failed(): resets all failed/manual/stuck jobs back to retryable state
- acquire_job(): atomically claims the next eligible job from the DB
- prompt.build_prompt(): None-safe job dict access (bug fixes in title and url)
"""

import json
import shutil
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from applypilot.database import init_db, close_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_job(conn, url, title="Test Job", site="testsite",
                apply_status=None, apply_attempts=0, fit_score=8,
                application_url=None):
    """Insert a minimal job row for testing."""
    conn.execute("""
        INSERT OR REPLACE INTO jobs
            (url, title, site, apply_status, apply_attempts, fit_score,
             application_url, strategy)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'native_scraper')
    """, (url, title, site, apply_status, apply_attempts, fit_score,
          application_url))
    conn.commit()


# ---------------------------------------------------------------------------
# reset_failed() tests
# ---------------------------------------------------------------------------

class TestResetFailed:
    """Tests for launcher.reset_failed()."""

    def test_resets_failed_jobs(self, tmp_path):
        """Jobs with apply_status='failed' are cleared to NULL with attempts=0."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        _insert_job(conn, "https://example.com/job1", apply_status="failed",
                    apply_attempts=2)

        with patch("applypilot.database.DB_PATH", db_path), \
             patch("applypilot.apply.launcher.get_connection",
                   return_value=conn):
            from applypilot.apply import launcher
            count = launcher.reset_failed()

        assert count == 1
        row = conn.execute(
            "SELECT apply_status, apply_attempts, apply_error FROM jobs WHERE url = ?",
            ("https://example.com/job1",)
        ).fetchone()
        assert row["apply_status"] is None
        assert row["apply_attempts"] == 0
        assert row["apply_error"] is None
        close_connection(db_path)

    def test_resets_manual_jobs(self, tmp_path):
        """Jobs with apply_status='manual' are also reset."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        _insert_job(conn, "https://example.com/job2", apply_status="manual",
                    apply_attempts=99)

        with patch("applypilot.apply.launcher.get_connection",
                   return_value=conn):
            from applypilot.apply import launcher
            count = launcher.reset_failed()

        assert count == 1
        row = conn.execute(
            "SELECT apply_status FROM jobs WHERE url = ?",
            ("https://example.com/job2",)
        ).fetchone()
        assert row["apply_status"] is None
        close_connection(db_path)

    def test_does_not_reset_applied_jobs(self, tmp_path):
        """Jobs with apply_status='applied' are never touched."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        _insert_job(conn, "https://example.com/job3", apply_status="applied",
                    apply_attempts=1)

        with patch("applypilot.apply.launcher.get_connection",
                   return_value=conn):
            from applypilot.apply import launcher
            count = launcher.reset_failed()

        assert count == 0
        row = conn.execute(
            "SELECT apply_status FROM jobs WHERE url = ?",
            ("https://example.com/job3",)
        ).fetchone()
        assert row["apply_status"] == "applied"
        close_connection(db_path)

    def test_does_not_reset_in_progress_jobs(self, tmp_path):
        """Jobs with apply_status='in_progress' are not touched by reset_failed."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        _insert_job(conn, "https://example.com/job4", apply_status="in_progress",
                    apply_attempts=0)

        with patch("applypilot.apply.launcher.get_connection",
                   return_value=conn):
            from applypilot.apply import launcher
            count = launcher.reset_failed()

        assert count == 0
        row = conn.execute(
            "SELECT apply_status FROM jobs WHERE url = ?",
            ("https://example.com/job4",)
        ).fetchone()
        assert row["apply_status"] == "in_progress"
        close_connection(db_path)

    def test_resets_multiple_failed_jobs(self, tmp_path):
        """All failed jobs are reset in a single call."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        for i in range(5):
            _insert_job(conn, f"https://example.com/job{i}",
                        apply_status="failed", apply_attempts=3)

        with patch("applypilot.apply.launcher.get_connection",
                   return_value=conn):
            from applypilot.apply import launcher
            count = launcher.reset_failed()

        assert count == 5
        rows = conn.execute(
            "SELECT apply_status FROM jobs WHERE apply_status IS NOT NULL"
        ).fetchall()
        assert len(rows) == 0
        close_connection(db_path)

    def test_returns_zero_when_nothing_to_reset(self, tmp_path):
        """Returns 0 when there are no failed jobs."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        _insert_job(conn, "https://example.com/clean", apply_status=None,
                    apply_attempts=0)

        with patch("applypilot.apply.launcher.get_connection",
                   return_value=conn):
            from applypilot.apply import launcher
            count = launcher.reset_failed()

        assert count == 0
        close_connection(db_path)


# ---------------------------------------------------------------------------
# acquire_job() tests
# ---------------------------------------------------------------------------

class TestAcquireJob:
    """Tests for launcher.acquire_job() — atomic job claiming from the DB."""

    def _make_conn(self, tmp_path) -> tuple[sqlite3.Connection, Path]:
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        return conn, db_path

    def test_acquires_eligible_job(self, tmp_path):
        """Returns a job dict and marks it in_progress."""
        conn, db_path = self._make_conn(tmp_path)
        _insert_job(conn, "https://example.com/job", application_url="https://example.com/apply",
                    fit_score=8, apply_status=None, apply_attempts=0)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.apply.launcher._load_blocked", return_value=([], [])), \
             patch("applypilot.apply.launcher.config") as mock_cfg:
            mock_cfg.DEFAULTS = {"max_apply_attempts": 3}
            from applypilot.apply import launcher
            job = launcher.acquire_job(min_score=7, worker_id=0)

        assert job is not None
        assert job["url"] == "https://example.com/job"
        row = conn.execute(
            "SELECT apply_status, agent_id FROM jobs WHERE url = ?",
            ("https://example.com/job",)
        ).fetchone()
        assert row["apply_status"] == "in_progress"
        assert row["agent_id"] == "worker-0"
        close_connection(db_path)

    def test_returns_none_when_queue_empty(self, tmp_path):
        """Returns None when no eligible jobs exist."""
        conn, db_path = self._make_conn(tmp_path)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.apply.launcher._load_blocked", return_value=([], [])), \
             patch("applypilot.apply.launcher.config") as mock_cfg:
            mock_cfg.DEFAULTS = {"max_apply_attempts": 3}
            from applypilot.apply import launcher
            job = launcher.acquire_job(min_score=7, worker_id=0)

        assert job is None
        close_connection(db_path)

    def test_skips_jobs_below_min_score(self, tmp_path):
        """Jobs with fit_score below min_score are not acquired."""
        conn, db_path = self._make_conn(tmp_path)
        _insert_job(conn, "https://example.com/lowscore",
                    application_url="https://example.com/apply",
                    fit_score=5, apply_status=None, apply_attempts=0)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.apply.launcher._load_blocked", return_value=([], [])), \
             patch("applypilot.apply.launcher.config") as mock_cfg:
            mock_cfg.DEFAULTS = {"max_apply_attempts": 3}
            from applypilot.apply import launcher
            job = launcher.acquire_job(min_score=7, worker_id=0)

        assert job is None
        close_connection(db_path)

    def test_skips_jobs_already_in_progress(self, tmp_path):
        """Jobs already marked in_progress are not re-acquired."""
        conn, db_path = self._make_conn(tmp_path)
        _insert_job(conn, "https://example.com/inprog",
                    application_url="https://example.com/apply",
                    fit_score=9, apply_status="in_progress", apply_attempts=0)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.apply.launcher._load_blocked", return_value=([], [])), \
             patch("applypilot.apply.launcher.config") as mock_cfg:
            mock_cfg.DEFAULTS = {"max_apply_attempts": 3}
            from applypilot.apply import launcher
            job = launcher.acquire_job(min_score=7, worker_id=0)

        assert job is None
        close_connection(db_path)

    def test_skips_jobs_with_null_application_url(self, tmp_path):
        """Jobs with application_url=NULL are not acquired (no valid apply URL)."""
        conn, db_path = self._make_conn(tmp_path)
        _insert_job(conn, "https://example.com/noapplyurl",
                    application_url=None,  # no application_url
                    fit_score=9, apply_status=None, apply_attempts=0)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.apply.launcher._load_blocked", return_value=([], [])), \
             patch("applypilot.apply.launcher.config") as mock_cfg:
            mock_cfg.DEFAULTS = {"max_apply_attempts": 3}
            from applypilot.apply import launcher
            job = launcher.acquire_job(min_score=7, worker_id=0)

        assert job is None
        close_connection(db_path)

    def test_skips_jobs_at_max_attempts(self, tmp_path):
        """Jobs that have reached max_apply_attempts are not re-acquired."""
        conn, db_path = self._make_conn(tmp_path)
        _insert_job(conn, "https://example.com/maxed",
                    application_url="https://example.com/apply",
                    fit_score=9, apply_status="failed", apply_attempts=3)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.apply.launcher._load_blocked", return_value=([], [])), \
             patch("applypilot.apply.launcher.config") as mock_cfg:
            mock_cfg.DEFAULTS = {"max_apply_attempts": 3}
            from applypilot.apply import launcher
            job = launcher.acquire_job(min_score=7, worker_id=0)

        assert job is None
        close_connection(db_path)

    def test_acquires_failed_job_under_max_attempts(self, tmp_path):
        """A 'failed' job with attempts < max is eligible for retry."""
        conn, db_path = self._make_conn(tmp_path)
        _insert_job(conn, "https://example.com/retry",
                    application_url="https://example.com/apply",
                    fit_score=8, apply_status="failed", apply_attempts=1)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.apply.launcher._load_blocked", return_value=([], [])), \
             patch("applypilot.apply.launcher.config") as mock_cfg:
            mock_cfg.DEFAULTS = {"max_apply_attempts": 3}
            from applypilot.apply import launcher
            job = launcher.acquire_job(min_score=7, worker_id=0)

        assert job is not None
        assert job["url"] == "https://example.com/retry"
        close_connection(db_path)

    def test_prefers_higher_score_job(self, tmp_path):
        """When multiple jobs are eligible, the one with higher fit_score is acquired first."""
        conn, db_path = self._make_conn(tmp_path)
        _insert_job(conn, "https://example.com/low",
                    application_url="https://example.com/apply-low",
                    fit_score=7, apply_status=None, apply_attempts=0)
        _insert_job(conn, "https://example.com/high",
                    application_url="https://example.com/apply-high",
                    fit_score=10, apply_status=None, apply_attempts=0)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.apply.launcher._load_blocked", return_value=([], [])), \
             patch("applypilot.apply.launcher.config") as mock_cfg:
            mock_cfg.DEFAULTS = {"max_apply_attempts": 3}
            from applypilot.apply import launcher
            job = launcher.acquire_job(min_score=7, worker_id=0)

        assert job is not None
        assert job["url"] == "https://example.com/high"
        close_connection(db_path)

    def test_acquire_by_target_url(self, tmp_path):
        """When target_url is given, that specific job is acquired regardless of status."""
        conn, db_path = self._make_conn(tmp_path)
        # Use a clearly non-manual-ATS URL so the real is_manual_ats returns False
        url = "https://boards.greenhouse.io/testco/jobs/12345"
        _insert_job(conn, url,
                    application_url=url,
                    fit_score=6, apply_status=None, apply_attempts=0)

        with patch("applypilot.apply.launcher.get_connection", return_value=conn), \
             patch("applypilot.config.is_manual_ats", return_value=False):
            from applypilot.apply import launcher
            job = launcher.acquire_job(
                target_url=url,
                min_score=7,
                worker_id=1,
            )

        assert job is not None
        assert job["url"] == url
        row = conn.execute(
            "SELECT agent_id FROM jobs WHERE url = ?",
            (url,)
        ).fetchone()
        assert row["agent_id"] == "worker-1"
        close_connection(db_path)


# ---------------------------------------------------------------------------
# prompt.build_prompt() — None-safety bug fixes
# ---------------------------------------------------------------------------

class TestBuildPromptNullSafety:
    """Tests that build_prompt() handles None title and url without crashing."""

    @pytest.fixture()
    def minimal_profile(self):
        return {
            "personal": {
                "full_name": "Jane Dev",
                "email": "jane@example.com",
                "phone": "5551234567",
                "password": "secret",
                "city": "Chicago",
            },
            "work_authorization": {
                "legally_authorized_to_work": "Yes",
                "require_sponsorship": "No",
            },
            "compensation": {
                "salary_expectation": "120000",
                "salary_currency": "USD",
            },
            "experience": {},
            "availability": {},
            "eeo_voluntary": {},
        }

    @pytest.fixture()
    def resume_pdf(self, tmp_path):
        """Create a fake resume PDF so build_prompt doesn't raise FileNotFoundError."""
        pdf = tmp_path / "Jane_Dev_Resume.pdf"
        pdf.write_bytes(b"%PDF fake")
        return pdf

    def _make_job(self, url="https://example.com/job",
                  title="Software Engineer",
                  site="example",
                  application_url="https://example.com/apply",
                  fit_score=8):
        return {
            "url": url,
            "title": title,
            "site": site,
            "application_url": application_url,
            "fit_score": fit_score,
            "tailored_resume_path": None,
            "full_description": "",
            "cover_letter_path": None,
        }

    def test_build_prompt_with_none_title_does_not_crash(
            self, tmp_path, minimal_profile, resume_pdf):
        """build_prompt() must not raise when job['title'] is None."""
        job = self._make_job(title=None)

        with patch("applypilot.apply.prompt.config") as mock_cfg, \
             patch("applypilot.apply.prompt.config.load_profile",
                   return_value=minimal_profile), \
             patch("applypilot.apply.prompt.config.load_search_config",
                   return_value={}), \
             patch("applypilot.apply.prompt.config.load_blocked_sso",
                   return_value=[]), \
             patch("applypilot.apply.prompt.config.load_env"), \
             patch("applypilot.config.load_blocked_sso", return_value=[]), \
             patch("applypilot.config.load_profile",
                   return_value=minimal_profile), \
             patch("applypilot.config.load_search_config",
                   return_value={}):
            mock_cfg.RESUME_PDF_PATH = resume_pdf
            mock_cfg.APPLY_WORKER_DIR = tmp_path
            mock_cfg.load_profile.return_value = minimal_profile
            mock_cfg.load_search_config.return_value = {}
            mock_cfg.load_blocked_sso.return_value = []
            mock_cfg.load_env.return_value = None
            mock_cfg.RESUME_PATH = tmp_path / "resume.txt"

            from applypilot.apply import prompt as prompt_mod
            # Should not raise TypeError/KeyError
            result = prompt_mod.build_prompt(job=job, tailored_resume="resume text")

        assert "Unknown" in result  # None title replaced with 'Unknown'
        assert "RESULT:APPLIED" in result  # sanity: prompt body present

    def test_build_prompt_with_none_url_uses_application_url(
            self, tmp_path, minimal_profile, resume_pdf):
        """build_prompt() falls back to application_url when job url key yields empty."""
        job = self._make_job(url="https://example.com/joblisting",
                             application_url="https://ats.example.com/apply/123")

        with patch("applypilot.apply.prompt.config") as mock_cfg, \
             patch("applypilot.apply.prompt.config.load_profile",
                   return_value=minimal_profile), \
             patch("applypilot.apply.prompt.config.load_search_config",
                   return_value={}), \
             patch("applypilot.apply.prompt.config.load_blocked_sso",
                   return_value=[]), \
             patch("applypilot.apply.prompt.config.load_env"), \
             patch("applypilot.config.load_blocked_sso", return_value=[]), \
             patch("applypilot.config.load_profile",
                   return_value=minimal_profile), \
             patch("applypilot.config.load_search_config",
                   return_value={}):
            mock_cfg.RESUME_PDF_PATH = resume_pdf
            mock_cfg.APPLY_WORKER_DIR = tmp_path
            mock_cfg.load_profile.return_value = minimal_profile
            mock_cfg.load_search_config.return_value = {}
            mock_cfg.load_blocked_sso.return_value = []
            mock_cfg.load_env.return_value = None
            mock_cfg.RESUME_PATH = tmp_path / "resume.txt"

            from applypilot.apply import prompt as prompt_mod
            result = prompt_mod.build_prompt(job=job, tailored_resume="")

        # application_url takes priority over url in the prompt
        assert "https://ats.example.com/apply/123" in result

    def test_build_prompt_expired_instruction_in_step2(
            self, tmp_path, minimal_profile, resume_pdf):
        """Step 2 of the prompt must contain an early RESULT:EXPIRED instruction."""
        job = self._make_job()

        with patch("applypilot.apply.prompt.config") as mock_cfg, \
             patch("applypilot.apply.prompt.config.load_profile",
                   return_value=minimal_profile), \
             patch("applypilot.apply.prompt.config.load_search_config",
                   return_value={}), \
             patch("applypilot.apply.prompt.config.load_blocked_sso",
                   return_value=[]), \
             patch("applypilot.apply.prompt.config.load_env"), \
             patch("applypilot.config.load_blocked_sso", return_value=[]), \
             patch("applypilot.config.load_profile",
                   return_value=minimal_profile), \
             patch("applypilot.config.load_search_config",
                   return_value={}):
            mock_cfg.RESUME_PDF_PATH = resume_pdf
            mock_cfg.APPLY_WORKER_DIR = tmp_path
            mock_cfg.load_profile.return_value = minimal_profile
            mock_cfg.load_search_config.return_value = {}
            mock_cfg.load_blocked_sso.return_value = []
            mock_cfg.load_env.return_value = None
            mock_cfg.RESUME_PATH = tmp_path / "resume.txt"

            from applypilot.apply import prompt as prompt_mod
            result = prompt_mod.build_prompt(job=job, tailored_resume="")

        # The fix: step 2 must instruct agent to output RESULT:EXPIRED on invalid pages
        assert "RESULT:EXPIRED" in result
        # Ensure the instruction appears in the STEP-BY-STEP section (near step 2)
        step_by_step_idx = result.index("== STEP-BY-STEP ==")
        expired_idx = result.index("RESULT:EXPIRED", step_by_step_idx)
        # Should appear before step 3 instructions
        step3_idx = result.index("3. LOCATION CHECK", step_by_step_idx)
        assert expired_idx < step3_idx, (
            "RESULT:EXPIRED instruction should appear in step 2, before step 3"
        )


# ---------------------------------------------------------------------------
# _is_permanent_failure() tests
# ---------------------------------------------------------------------------

class TestIsPermanentFailure:
    """Tests for the permanent failure classifier."""

    def test_expired_is_permanent(self):
        from applypilot.apply.launcher import _is_permanent_failure
        assert _is_permanent_failure("expired") is True

    def test_captcha_is_permanent(self):
        from applypilot.apply.launcher import _is_permanent_failure
        assert _is_permanent_failure("captcha") is True

    def test_failed_with_expired_reason_is_permanent(self):
        from applypilot.apply.launcher import _is_permanent_failure
        assert _is_permanent_failure("failed:expired") is True

    def test_failed_with_unknown_reason_is_not_permanent(self):
        from applypilot.apply.launcher import _is_permanent_failure
        assert _is_permanent_failure("failed:unknown") is False

    def test_no_result_line_is_not_permanent(self):
        from applypilot.apply.launcher import _is_permanent_failure
        assert _is_permanent_failure("failed:no_result_line") is False

    def test_cloudflare_blocked_is_permanent(self):
        from applypilot.apply.launcher import _is_permanent_failure
        assert _is_permanent_failure("failed:cloudflare_blocked") is True

    def test_site_blocked_prefix_is_permanent(self):
        from applypilot.apply.launcher import _is_permanent_failure
        assert _is_permanent_failure("failed:site_blocked_reason") is True

    def test_login_issue_is_permanent(self):
        from applypilot.apply.launcher import _is_permanent_failure
        assert _is_permanent_failure("login_issue") is True
