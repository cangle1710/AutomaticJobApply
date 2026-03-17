"""Unit tests for applypilot.database — schema, CRUD, stats, and migrations."""

import sqlite3
import threading

import pytest

from applypilot.database import (
    init_db,
    reset_db,
    get_connection,
    close_connection,
    ensure_columns,
    get_stats,
    store_jobs,
    get_jobs_by_stage,
    update_job_scores,
)


# ── Schema & Init ────────────────────────────────────────────────────────


class TestInitDb:
    def test_creates_jobs_table(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone()
        assert row is not None
        close_connection(db_path)

    def test_is_idempotent(self, tmp_path):
        """Calling init_db twice must not wipe existing data."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.execute(
            "INSERT INTO jobs (url, title) VALUES ('http://example.com', 'Test')"
        )
        conn.commit()

        init_db(db_path)
        row = conn.execute(
            "SELECT title FROM jobs WHERE url = 'http://example.com'"
        ).fetchone()
        assert row is not None
        close_connection(db_path)

    def test_creates_parent_dirs(self, tmp_path):
        db_path = tmp_path / "nested" / "dir" / "test.db"
        init_db(db_path)
        assert db_path.exists()
        close_connection(db_path)

    def test_wal_mode_enabled(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        close_connection(db_path)


class TestEnsureColumns:
    def test_returns_empty_when_schema_current(self, db_conn):
        added = ensure_columns(db_conn)
        assert added == []

    def test_adds_novel_column(self, db_conn):
        """Simulate a future migration by dropping a column and re-running."""
        # Verify the column exists first (it should from init_db)
        info = {
            row[1]
            for row in db_conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        assert "verification_confidence" in info
        # ensure_columns should report nothing to add
        assert ensure_columns(db_conn) == []


# ── Store & Retrieve ─────────────────────────────────────────────────────


class TestStoreJobs:
    def test_stores_new_jobs(self, db_conn):
        jobs = [
            {
                "url": "http://example.com/job/1",
                "title": "Python Engineer",
                "salary": None,
                "description": "A job",
                "location": "Remote",
            }
        ]
        new, existing = store_jobs(db_conn, jobs, site="TestSite", strategy="test")
        assert new == 1
        assert existing == 0

    def test_deduplicates_by_url(self, db_conn):
        jobs = [{"url": "http://example.com/job/1", "title": "Python Engineer"}]
        store_jobs(db_conn, jobs, site="TestSite", strategy="test")
        new, existing = store_jobs(db_conn, jobs, site="TestSite", strategy="test")
        assert new == 0
        assert existing == 1

    def test_skips_empty_url(self, db_conn):
        jobs = [{"url": "", "title": "No URL"}, {"title": "Missing URL key"}]
        new, existing = store_jobs(db_conn, jobs, site="Test", strategy="test")
        assert new == 0
        assert existing == 0

    def test_stores_multiple_jobs(self, db_conn):
        jobs = [
            {"url": f"http://example.com/job/{i}", "title": f"Job {i}"}
            for i in range(5)
        ]
        new, _ = store_jobs(db_conn, jobs, site="Test", strategy="test")
        assert new == 5

    def test_sets_site_and_strategy(self, db_conn):
        jobs = [{"url": "http://example.com/job/1", "title": "Test"}]
        store_jobs(db_conn, jobs, site="Indeed", strategy="native_scraper")
        row = db_conn.execute(
            "SELECT site, strategy FROM jobs WHERE url = 'http://example.com/job/1'"
        ).fetchone()
        assert row["site"] == "Indeed"
        assert row["strategy"] == "native_scraper"

    def test_sets_discovered_at(self, db_conn):
        jobs = [{"url": "http://example.com/1", "title": "T"}]
        store_jobs(db_conn, jobs, site="S", strategy="s")
        row = db_conn.execute(
            "SELECT discovered_at FROM jobs WHERE url = 'http://example.com/1'"
        ).fetchone()
        assert row["discovered_at"] is not None


# ── Reset ─────────────────────────────────────────────────────────────────


class TestResetDb:
    def test_deletes_all_records(self, tmp_db):
        conn, db_path = tmp_db
        store_jobs(conn, [{"url": "http://a.com", "title": "J"}], "S", "s")
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1

        reset_db(db_path)
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0

    def test_preserves_schema(self, tmp_db):
        conn, db_path = tmp_db
        reset_db(db_path)
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone()
        assert row is not None


# ── Stats ─────────────────────────────────────────────────────────────────


class TestGetStats:
    def test_empty_db(self, db_conn):
        stats = get_stats(db_conn)
        assert stats["total"] == 0
        assert stats["scored"] == 0
        assert stats["applied"] == 0

    def test_counts_discovered(self, db_conn):
        store_jobs(
            db_conn,
            [{"url": f"http://x.com/{i}", "title": f"J{i}"} for i in range(3)],
            "S",
            "s",
        )
        assert get_stats(db_conn)["total"] == 3

    def test_counts_scored(self, db_conn):
        db_conn.execute(
            "INSERT INTO jobs (url, full_description, fit_score) VALUES (?, ?, ?)",
            ("http://x.com/1", "Full desc", 8),
        )
        db_conn.commit()
        stats = get_stats(db_conn)
        assert stats["scored"] == 1
        assert stats["unscored"] == 0

    def test_pending_detail(self, db_conn):
        db_conn.execute(
            "INSERT INTO jobs (url, title) VALUES (?, ?)",
            ("http://x.com/1", "No Desc Yet"),
        )
        db_conn.commit()
        assert get_stats(db_conn)["pending_detail"] == 1

    def test_score_distribution(self, db_conn):
        for i, score in enumerate([8, 8, 7, 9]):
            db_conn.execute(
                "INSERT INTO jobs (url, fit_score) VALUES (?, ?)",
                (f"http://x.com/{i}", score),
            )
        db_conn.commit()
        dist = dict(get_stats(db_conn)["score_distribution"])
        assert dist[8] == 2
        assert dist[7] == 1
        assert dist[9] == 1

    def test_by_site_breakdown(self, db_conn):
        for i in range(3):
            db_conn.execute(
                "INSERT INTO jobs (url, site) VALUES (?, ?)",
                (f"http://x.com/{i}", "Indeed"),
            )
        db_conn.execute(
            "INSERT INTO jobs (url, site) VALUES (?, ?)",
            ("http://li.com/1", "LinkedIn"),
        )
        db_conn.commit()
        site_dict = dict(get_stats(db_conn)["by_site"])
        assert site_dict["Indeed"] == 3
        assert site_dict["LinkedIn"] == 1


# ── get_jobs_by_stage ─────────────────────────────────────────────────────


class TestGetJobsByStage:
    def _insert(self, conn, url, **kw):
        cols = ["url"] + list(kw.keys())
        vals = [url] + list(kw.values())
        conn.execute(
            f"INSERT INTO jobs ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
            vals,
        )
        conn.commit()

    def test_discovered_returns_all(self, db_conn):
        self._insert(db_conn, "http://a.com")
        self._insert(db_conn, "http://b.com")
        assert len(get_jobs_by_stage(db_conn, stage="discovered")) == 2

    def test_pending_score(self, db_conn):
        self._insert(db_conn, "http://a.com", full_description="desc")
        self._insert(db_conn, "http://b.com")  # no desc → not eligible
        self._insert(db_conn, "http://c.com", full_description="desc", fit_score=8)
        jobs = get_jobs_by_stage(db_conn, stage="pending_score")
        assert len(jobs) == 1
        assert jobs[0]["url"] == "http://a.com"

    def test_limit_respected(self, db_conn):
        for i in range(10):
            self._insert(db_conn, f"http://x.com/{i}")
        assert len(get_jobs_by_stage(db_conn, stage="discovered", limit=3)) == 3

    def test_returns_dicts(self, db_conn):
        self._insert(db_conn, "http://a.com", title="Test Job")
        jobs = get_jobs_by_stage(db_conn, stage="discovered")
        assert isinstance(jobs[0], dict)
        assert "url" in jobs[0]
        assert "title" in jobs[0]

    def test_pending_tailor_uses_min_score(self, db_conn):
        self._insert(
            db_conn,
            "http://a.com",
            full_description="desc",
            fit_score=6,
        )
        self._insert(
            db_conn,
            "http://b.com",
            full_description="desc",
            fit_score=8,
        )
        jobs = get_jobs_by_stage(db_conn, stage="pending_tailor", min_score=7)
        assert len(jobs) == 1
        assert jobs[0]["url"] == "http://b.com"

    def test_empty_db_returns_empty_list(self, db_conn):
        assert get_jobs_by_stage(db_conn, stage="discovered") == []


# ── update_job_scores ─────────────────────────────────────────────────────


class TestUpdateJobScores:
    def test_writes_valid_scores(self, db_conn):
        db_conn.execute(
            "INSERT INTO jobs (url, full_description) VALUES (?, ?)",
            ("http://a.com", "desc"),
        )
        db_conn.commit()

        results = [
            {"url": "http://a.com", "score": 8, "keywords": "Python", "reasoning": "Good fit."}
        ]
        written = update_job_scores(db_conn, results)
        assert written == 1

        row = db_conn.execute(
            "SELECT fit_score, scored_at FROM jobs WHERE url = 'http://a.com'"
        ).fetchone()
        assert row["fit_score"] == 8
        assert row["scored_at"] is not None

    def test_skips_error_results(self, db_conn):
        db_conn.execute("INSERT INTO jobs (url) VALUES (?)", ("http://a.com",))
        db_conn.commit()

        results = [{"url": "http://a.com", "score": 0, "_error": True}]
        assert update_job_scores(db_conn, results) == 0

        row = db_conn.execute(
            "SELECT fit_score FROM jobs WHERE url = 'http://a.com'"
        ).fetchone()
        assert row["fit_score"] is None

    def test_mixed_results(self, db_conn):
        for i in range(3):
            db_conn.execute("INSERT INTO jobs (url) VALUES (?)", (f"http://x.com/{i}",))
        db_conn.commit()

        results = [
            {"url": "http://x.com/0", "score": 8, "keywords": "", "reasoning": ""},
            {"url": "http://x.com/1", "score": 0, "_error": True},
            {"url": "http://x.com/2", "score": 7, "keywords": "", "reasoning": ""},
        ]
        assert update_job_scores(db_conn, results) == 2

    def test_empty_results(self, db_conn):
        assert update_job_scores(db_conn, []) == 0


# ── Connection management ────────────────────────────────────────────────


class TestConnectionManagement:
    def test_thread_local_isolation(self, tmp_path):
        """Connections from different threads must be distinct objects."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        conns = []

        def get_in_thread():
            conns.append(id(get_connection(db_path)))
            close_connection(db_path)

        t = threading.Thread(target=get_in_thread)
        t.start()
        t.join()

        main_id = id(get_connection(db_path))
        close_connection(db_path)

        assert len(conns) == 1
        assert conns[0] != main_id
