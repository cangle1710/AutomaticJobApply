"""ApplyPilot Pipeline Orchestrator.

Runs pipeline stages in sequence or concurrently (streaming mode).

Usage (via CLI):
    applypilot run                        # all stages, sequential
    applypilot run --stream               # all stages, concurrent
    applypilot run discover enrich        # specific stages
    applypilot run score tailor cover     # LLM-only stages
    applypilot run --dry-run              # preview without executing
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from applypilot.config import load_env, ensure_dirs
from applypilot.database import init_db, get_connection, get_stats, reset_db

log = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# Stage definitions
# ---------------------------------------------------------------------------

# Set to False to skip resume tailoring and go straight from score → apply.
# Flip back to True to re-enable the full tailor → cover → pdf flow.
TAILOR_ENABLED = False

STAGE_ORDER = ("discover", "enrich", "score", "tailor", "cover", "pdf")

STAGE_META: dict[str, dict] = {
    "discover": {"desc": "Job discovery (Workday + JobSpy: LinkedIn/Indeed/ZipRecruiter + HiringCafe + SmartExtract)"},
    "enrich":   {"desc": "Detail enrichment (full descriptions + apply URLs)"},
    "score":    {"desc": "LLM scoring (fit 1-10)"},
    "tailor":   {"desc": "Resume tailoring (LLM + validation)"},
    "cover":    {"desc": "Cover letter generation"},
    "pdf":      {"desc": "PDF conversion (tailored resumes + cover letters)"},
}

# Upstream dependency: a stage only finishes when its upstream is done AND
# it has no remaining pending work.
_UPSTREAM: dict[str, str | None] = {
    "discover": None,
    "enrich":   "discover",
    "score":    "enrich",
    "tailor":   "score",
    "cover":    "tailor",
    "pdf":      "cover",
}


# ---------------------------------------------------------------------------
# Individual stage runners
# ---------------------------------------------------------------------------

def _store_job_listings(
    listings: list,
    strategy: str = "workday_api",
    default_site: str = "",
) -> tuple[int, int]:
    """Persist a list of JobListing objects into the jobs table.

    Args:
        listings:     JobListing objects to store.
        strategy:     Value for the strategy column (e.g. "workday_api", "hiring_cafe").
        default_site: Fallback site label when job.company is empty.

    Returns:
        (new_count, duplicate_count)
    """
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    new = 0
    existing = 0

    for job in listings:
        if not job.url:
            continue

        salary = None
        if job.salary_min:
            curr = job.salary_currency or ""
            salary = f"{curr}{int(job.salary_min):,}"
            if job.salary_max:
                salary += f"-{curr}{int(job.salary_max):,}"
            if job.salary_interval:
                salary += f"/{job.salary_interval}"

        description = job.description or None
        # Promote to full_description if substantial enough for scoring
        full_description = description if (description and len(description) > 200) else None
        detail_scraped_at = now if full_description else None

        try:
            conn.execute(
                "INSERT INTO jobs "
                "(url, title, salary, description, location, site, strategy, "
                "discovered_at, full_description, application_url, detail_scraped_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job.url,
                    job.title or None,
                    salary,
                    description,
                    job.location or None,
                    job.company or default_site or strategy,
                    strategy,
                    now,
                    full_description,
                    job.url,
                    detail_scraped_at,
                ),
            )
            new += 1
            log.debug("  [NEW] %-50s | %-25s | %s",
                      (job.title or "?")[:50], (job.company or "?")[:25], job.url)
        except sqlite3.IntegrityError:
            existing += 1

    conn.commit()
    log.info("  Stored [%s]: %d new  |  %d duplicates", strategy, new, existing)
    return new, existing


def _run_workday_native() -> dict:
    """Run the universal async Workday scraper.

    1. Validates all companies in workday_companies.yaml (probes each tenant).
    2. Scrapes all valid tenants concurrently for the configured search queries.
    3. Persists results to the jobs table via _store_job_listings().

    Search queries come from searches.yaml (tier 1 by default, configurable via
    ``workday_max_tier`` in searches.yaml).  Location is not passed to the
    Workday API — it does not filter by location at the search level.  Location
    filtering happens downstream via the pipeline's existing location rules.

    Returns:
        dict with keys: status, companies_valid, companies_total, new, existing
    """
    from applypilot.config import load_search_config
    from applypilot.discovery.orchestrator import run_workday_scrapers, load_workday_companies

    search_cfg = load_search_config() or {}
    queries_cfg = search_cfg.get("queries", [])
    proxy = search_cfg.get("proxy")

    # Default to tier 1 only — Workday scraping is company-specific so we
    # stay focused.  Override with workday_max_tier: 2 in searches.yaml.
    max_tier = search_cfg.get("workday_max_tier", 1)
    tier_queries = [q for q in queries_cfg if q.get("tier", 99) <= max_tier]

    if not tier_queries:
        tier_queries = queries_cfg   # fallback: use everything

    if not tier_queries:
        log.warning("No search queries in searches.yaml — skipping Workday native scraper")
        return {"status": "skipped", "new": 0, "existing": 0}

    # Workday doesn't filter by location at the API level.
    # Limit pages per company to control volume: 1 page = 20 jobs per company.
    # Override with workday_max_pages_per_company in searches.yaml.
    max_pages = search_cfg.get("workday_max_pages_per_company", 1)
    queries = [{"query": q["query"], "location": "", "max_pages": max_pages} for q in tier_queries]

    companies = load_workday_companies()
    if not companies:
        return {"status": "skipped (no companies)", "new": 0, "existing": 0}

    query_labels = ", ".join(f'"{q["query"]}"' for q in queries[:5])
    if len(queries) > 5:
        query_labels += f" … +{len(queries) - 5} more"

    log.info("━━━ WORKDAY DISCOVERY ━━━")
    log.info(
        "  Companies: %d  |  Queries (%d): %s",
        len(companies), len(queries), query_labels,
    )
    console.print(
        f"  [bold cyan]Workday:[/bold cyan] {len(companies)} companies  ×  "
        f"{len(queries)} quer{'y' if len(queries) == 1 else 'ies'}  →  validating..."
    )

    t0 = time.time()
    try:
        listings = asyncio.run(
            run_workday_scrapers(
                queries=queries,
                companies=companies,
                validate=True,
                config={"fetch_detail": True},
                proxy=proxy,
                rate=2.0,   # 2 req/s sustained across all companies
                burst=15,
            )
        )
    except Exception as e:
        log.error("Workday native scraper failed: %s", e, exc_info=True)
        return {"status": f"error: {e}", "new": 0, "existing": 0}

    elapsed = time.time() - t0
    new, existing = _store_job_listings(listings)

    log.info(
        "━━━ WORKDAY COMPLETE  %.0fs  |  %d listings  →  %d new  |  %d dupes ━━━",
        elapsed, len(listings), new, existing,
    )
    console.print(
        f"  [bold cyan]Workday:[/bold cyan] done in [yellow]{elapsed:.0f}s[/yellow]  "
        f"→  [green]{new} new[/green]  |  {existing} dupes  "
        f"([dim]{len(listings)} total fetched[/dim])"
    )

    return {
        "status": "ok",
        "companies_total": len(companies),
        "listings": len(listings),
        "new": new,
        "existing": existing,
    }


def _run_hiring_cafe() -> dict:
    """Run the HiringCafe async scraper and store results."""
    from applypilot.config import load_search_config
    from applypilot.discovery.orchestrator import run_scrapers

    search_cfg = load_search_config() or {}
    queries_cfg = search_cfg.get("queries", [])
    if not queries_cfg:
        return {"status": "skipped (no queries)", "new": 0, "existing": 0}

    proxy = search_cfg.get("proxy")
    max_tier = search_cfg.get("workday_max_tier", 1)
    tier_queries = [q for q in queries_cfg if q.get("tier", 99) <= max_tier] or queries_cfg
    # Limit to first 5 queries to keep HiringCafe volume manageable
    queries = [{"query": q["query"], "location": ""} for q in tier_queries[:5]]

    try:
        listings = asyncio.run(
            run_scrapers(
                queries=queries,
                sources=["hiring_cafe"],
                proxy=proxy,
                rate=2.0,
                burst=5,
            )
        )
    except Exception as e:
        return {"status": f"error: {e}", "new": 0, "existing": 0}

    if not listings:
        return {"status": "ok", "new": 0, "existing": 0}

    new, existing = _store_job_listings(listings, strategy="hiring_cafe", default_site="HiringCafe")
    return {"status": "ok", "new": new, "existing": existing}


def _prune_low_score_jobs(min_score: int = 7, top_n: int = 50) -> int:
    """Delete scored jobs below min_score and cap to top N qualifying globally.

    Called after the score stage to keep the pipeline focused on the best
    matches and avoid doing LLM tailoring/cover work on mediocre fits.

    Args:
        min_score: Minimum fit_score to keep.
        top_n:     Global cap on qualifying jobs after pruning.

    Returns:
        Number of jobs deleted.
    """
    conn = get_connection()

    # Delete all jobs scored below the threshold
    cur = conn.execute(
        "DELETE FROM jobs WHERE fit_score IS NOT NULL AND fit_score < ?",
        (min_score,),
    )
    deleted = cur.rowcount

    # If more than top_n qualifying jobs remain, keep only the top N by score
    qualifying = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE fit_score >= ?", (min_score,)
    ).fetchone()[0]

    if qualifying > top_n:
        to_prune = conn.execute(
            "SELECT url FROM jobs WHERE fit_score >= ? "
            "ORDER BY fit_score DESC, discovered_at DESC "
            "LIMIT -1 OFFSET ?",
            (min_score, top_n),
        ).fetchall()
        for (url,) in to_prune:
            conn.execute("DELETE FROM jobs WHERE url = ?", (url,))
        deleted += len(to_prune)

    conn.commit()

    if deleted:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE fit_score >= ?", (min_score,)
        ).fetchone()[0]
        log.info(
            "Score pruning: removed %d jobs → %d qualifying (score ≥ %d)",
            deleted, remaining, min_score,
        )
        console.print(
            f"  [dim]Score pruning: kept {remaining} qualifying jobs "
            f"(score ≥ {min_score}, top {top_n})[/dim]"
        )

    return deleted


def _db_job_count() -> int:
    """Return current total job count from the database."""
    try:
        return get_connection().execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    except Exception:
        return -1


def _run_discover(workers: int = 1) -> dict:
    """Stage: Job discovery — Workday, JobSpy (LinkedIn/Indeed/ZipRecruiter), HiringCafe, SmartExtract."""
    reset_db()
    console.print("  [dim]Database cleared for fresh run.[/dim]")

    stats: dict = {"workday_native": None, "jobspy": None, "hiring_cafe": None, "smartextract": None}
    stage_t0 = time.time()

    log.info("════ DISCOVER STAGE START ════")

    # ── 1. Workday universal scraper (validates + scrapes companies, 1 page each)
    log.info("── [1/4] Workday universal scraper ──")
    t0 = time.time()
    try:
        result = _run_workday_native()
        stats["workday_native"] = result.get("status", "ok")
    except Exception as e:
        log.error("Workday native scraper failed: %s", e)
        console.print(f"  [red]Workday error:[/red] {e}")
        stats["workday_native"] = f"error: {e}"
    log.info("── [1/4] done in %.0fs  |  DB total: %d jobs ──", time.time() - t0, _db_job_count())

    # ── 2. JobSpy (LinkedIn + Indeed + ZipRecruiter)
    log.info("── [2/4] JobSpy (LinkedIn + Indeed + ZipRecruiter) ──")
    console.print("  [cyan]JobSpy (LinkedIn + Indeed + ZipRecruiter)...[/cyan]")
    t0 = time.time()
    try:
        from applypilot.discovery.jobspy import run_discovery
        run_discovery()
        stats["jobspy"] = "ok"
    except Exception as e:
        log.error("JobSpy crawl failed: %s", e)
        console.print(f"  [red]JobSpy error:[/red] {e}")
        stats["jobspy"] = f"error: {e}"
    log.info("── [2/4] done in %.0fs  |  DB total: %d jobs ──", time.time() - t0, _db_job_count())

    # ── 3. HiringCafe (Algolia-backed job board)
    log.info("── [3/4] HiringCafe ──")
    console.print("  [cyan]HiringCafe...[/cyan]")
    t0 = time.time()
    try:
        result = _run_hiring_cafe()
        stats["hiring_cafe"] = result.get("status", "ok")
        if result.get("new", 0) > 0:
            console.print(f"  [cyan]HiringCafe:[/cyan] {result['new']} new jobs")
    except Exception as e:
        log.error("HiringCafe scraper failed: %s", e)
        console.print(f"  [red]HiringCafe error:[/red] {e}")
        stats["hiring_cafe"] = f"error: {e}"
    log.info("── [3/4] done in %.0fs  |  DB total: %d jobs ──", time.time() - t0, _db_job_count())

    # ── 4. Smart extract (AI-powered direct career sites)
    log.info("── [4/4] Smart extract (direct career sites) ──")
    console.print("  [cyan]Smart extract (direct career sites)...[/cyan]")
    t0 = time.time()
    try:
        from applypilot.discovery.smartextract import run_smart_extract
        run_smart_extract(workers=workers)
        stats["smartextract"] = "ok"
    except Exception as e:
        log.error("Smart extract failed: %s", e)
        console.print(f"  [red]Smart extract error:[/red] {e}")
        stats["smartextract"] = f"error: {e}"
    log.info("── [4/4] done in %.0fs  |  DB total: %d jobs ──", time.time() - t0, _db_job_count())

    total = _db_job_count()
    log.info(
        "════ DISCOVER STAGE COMPLETE  %.0fs  |  %d total jobs in DB ════",
        time.time() - stage_t0, total,
    )
    return stats


def _run_enrich(workers: int = 1) -> dict:
    """Stage: Detail enrichment — scrape full descriptions and apply URLs."""
    try:
        from applypilot.enrichment.detail import run_enrichment
        run_enrichment(workers=workers)
        return {"status": "ok"}
    except Exception as e:
        log.error("Enrichment failed: %s", e)
        return {"status": f"error: {e}"}


def _run_score(min_score: int = 7, top_n: int = 10) -> dict:
    """Stage: LLM scoring — assign fit scores 1-10, then prune low-scoring jobs."""
    try:
        from applypilot.scoring.scorer import run_scoring
        run_scoring()
        pruned = _prune_low_score_jobs(min_score=min_score, top_n=top_n)
        return {"status": "ok", "pruned": pruned}
    except Exception as e:
        log.error("Scoring failed: %s", e)
        return {"status": f"error: {e}"}


def _run_tailor(min_score: int = 7, validation_mode: str = "normal") -> dict:
    """Stage: Resume tailoring — generate tailored resumes for high-fit jobs."""
    if not TAILOR_ENABLED:
        log.info("Tailor stage is disabled (TAILOR_ENABLED=False). Skipping.")
        return {"status": "skipped"}
    try:
        from applypilot.scoring.tailor import run_tailoring
        run_tailoring(min_score=min_score, validation_mode=validation_mode)
        return {"status": "ok"}
    except Exception as e:
        log.error("Tailoring failed: %s", e)
        return {"status": f"error: {e}"}


def _run_cover(min_score: int = 7, validation_mode: str = "normal") -> dict:
    """Stage: Cover letter generation."""
    try:
        from applypilot.scoring.cover_letter import run_cover_letters
        run_cover_letters(min_score=min_score, validation_mode=validation_mode)
        return {"status": "ok"}
    except Exception as e:
        log.error("Cover letter generation failed: %s", e)
        return {"status": f"error: {e}"}


def _run_pdf() -> dict:
    """Stage: PDF conversion — convert tailored resumes and cover letters to PDF."""
    try:
        from applypilot.scoring.pdf import batch_convert
        batch_convert()
        return {"status": "ok"}
    except Exception as e:
        log.error("PDF conversion failed: %s", e)
        return {"status": f"error: {e}"}


# Map stage names to their runner functions
_STAGE_RUNNERS: dict[str, callable] = {
    "discover": _run_discover,
    "enrich":   _run_enrich,
    "score":    _run_score,
    "tailor":   _run_tailor,
    "cover":    _run_cover,
    "pdf":      _run_pdf,
}


# ---------------------------------------------------------------------------
# Stage resolution
# ---------------------------------------------------------------------------

def _resolve_stages(stage_names: list[str]) -> list[str]:
    """Resolve 'all' and validate/order stage names."""
    if "all" in stage_names:
        return list(STAGE_ORDER)

    resolved = []
    for name in stage_names:
        if name not in STAGE_META:
            console.print(
                f"[red]Unknown stage:[/red] '{name}'. "
                f"Available: {', '.join(STAGE_ORDER)}, all"
            )
            raise SystemExit(1)
        if name not in resolved:
            resolved.append(name)

    # Maintain canonical order
    return [s for s in STAGE_ORDER if s in resolved]


# ---------------------------------------------------------------------------
# Streaming pipeline helpers
# ---------------------------------------------------------------------------

class _StageTracker:
    """Thread-safe tracker for which stages have finished producing work."""

    def __init__(self):
        self._events: dict[str, threading.Event] = {
            stage: threading.Event() for stage in STAGE_ORDER
        }
        self._results: dict[str, dict] = {}
        self._lock = threading.Lock()

    def mark_done(self, stage: str, result: dict | None = None) -> None:
        with self._lock:
            self._results[stage] = result or {"status": "ok"}
        self._events[stage].set()

    def is_done(self, stage: str) -> bool:
        return self._events[stage].is_set()

    def wait(self, stage: str, timeout: float | None = None) -> bool:
        return self._events[stage].wait(timeout=timeout)

    def get_results(self) -> dict[str, dict]:
        with self._lock:
            return dict(self._results)


# SQL to count pending work for each stage
_PENDING_SQL: dict[str, str] = {
    "enrich": "SELECT COUNT(*) FROM jobs WHERE detail_scraped_at IS NULL",
    "score":  "SELECT COUNT(*) FROM jobs WHERE full_description IS NOT NULL AND fit_score IS NULL",
    "tailor": (
        "SELECT COUNT(*) FROM jobs WHERE fit_score >= ? "
        "AND full_description IS NOT NULL "
        "AND tailored_resume_path IS NULL "
        "AND COALESCE(tailor_attempts, 0) < 5"
    ),
    "cover": (
        "SELECT COUNT(*) FROM jobs WHERE tailored_resume_path IS NOT NULL "
        "AND (cover_letter_path IS NULL OR cover_letter_path = '') "
        "AND COALESCE(cover_attempts, 0) < 5"
    ),
    "pdf": (
        "SELECT COUNT(*) FROM jobs WHERE tailored_resume_path IS NOT NULL "
        "AND tailored_resume_path LIKE '%.txt'"
    ),
}

# How long to sleep between polling loops in streaming mode (seconds)
_STREAM_POLL_INTERVAL = 10


def _count_pending(stage: str, min_score: int = 7) -> int:
    """Count pending work items for a stage."""
    sql = _PENDING_SQL.get(stage)
    if sql is None:
        return 0
    conn = get_connection()
    if "?" in sql:
        return conn.execute(sql, (min_score,)).fetchone()[0]
    return conn.execute(sql).fetchone()[0]


def _run_stage_streaming(
    stage: str,
    tracker: _StageTracker,
    stop_event: threading.Event,
    min_score: int = 7,
    top_n: int = 10,
    workers: int = 1,
    validation_mode: str = "normal",
) -> None:
    """Run a single stage in streaming mode: loop until upstream done + no work.

    For discover: runs once, then marks done.
    For all others: polls DB for pending work, runs the batch processor,
    and repeats until upstream is done and no pending work remains.
    """
    runner = _STAGE_RUNNERS[stage]
    kwargs: dict = {}
    if stage in ("score", "tailor", "cover"):
        kwargs["min_score"] = min_score
    if stage == "score":
        kwargs["top_n"] = top_n
    if stage in ("tailor", "cover"):
        kwargs["validation_mode"] = validation_mode
    if stage in ("discover", "enrich"):
        kwargs["workers"] = workers

    upstream = _UPSTREAM[stage]

    if stage == "discover":
        # Discover runs once (its sub-scrapers already do their full crawl)
        try:
            result = runner(**kwargs)
            tracker.mark_done(stage, result)
        except Exception as e:
            log.exception("Stage '%s' crashed", stage)
            tracker.mark_done(stage, {"status": f"error: {e}"})
        return

    # For downstream stages: loop until upstream done + no pending work
    passes = 0
    while not stop_event.is_set():
        # Wait for upstream to start producing work (first pass only)
        if passes == 0 and upstream and not tracker.is_done(upstream):
            # Wait a bit for upstream to produce some work before first run
            tracker.wait(upstream, timeout=_STREAM_POLL_INTERVAL)

        pending = _count_pending(stage, min_score)

        if pending > 0:
            try:
                runner(**kwargs)
                passes += 1
            except Exception as e:
                log.error("Stage '%s' error (pass %d): %s", stage, passes, e)
                passes += 1
        else:
            # No work right now
            upstream_done = upstream is None or tracker.is_done(upstream)
            if upstream_done:
                # No work and upstream is done — this stage is finished
                break
            # Upstream still running, wait and retry
            if stop_event.wait(timeout=_STREAM_POLL_INTERVAL):
                break  # Stop requested

    tracker.mark_done(stage, {"status": "ok", "passes": passes})


# ---------------------------------------------------------------------------
# Pipeline orchestrators
# ---------------------------------------------------------------------------

def _run_sequential(ordered: list[str], min_score: int, top_n: int = 10,
                    workers: int = 1, validation_mode: str = "normal") -> dict:
    """Execute stages one at a time (original behavior)."""
    results: list[dict] = []
    errors: dict[str, str] = {}
    pipeline_start = time.time()

    for name in ordered:
        meta = STAGE_META[name]
        console.print(f"\n{'=' * 70}")
        console.print(f"  [bold]STAGE: {name}[/bold] — {meta['desc']}")
        console.print(f"  Started: {datetime.now().strftime('%H:%M:%S')}")
        console.print(f"{'=' * 70}")

        t0 = time.time()
        runner = _STAGE_RUNNERS[name]

        try:
            kwargs: dict = {}
            if name in ("score", "tailor", "cover"):
                kwargs["min_score"] = min_score
            if name == "score":
                kwargs["top_n"] = top_n
            if name in ("tailor", "cover"):
                kwargs["validation_mode"] = validation_mode
            if name in ("discover", "enrich"):
                kwargs["workers"] = workers
            result = runner(**kwargs)
            elapsed = time.time() - t0

            status = "ok"
            if isinstance(result, dict):
                status = result.get("status", "ok")
                if name == "discover":
                    sub_errors = [
                        f"{k}: {v}" for k, v in result.items()
                        if isinstance(v, str) and v.startswith("error")
                    ]
                    if sub_errors:
                        status = "partial"

        except Exception as e:
            elapsed = time.time() - t0
            status = f"error: {e}"
            log.exception("Stage '%s' crashed", name)
            console.print(f"\n  [red]STAGE FAILED:[/red] {e}")

        results.append({"stage": name, "status": status, "elapsed": elapsed})
        if status not in ("ok", "partial"):
            errors[name] = status

        console.print(f"\n  Stage '{name}' completed in {elapsed:.1f}s — {status}")

    total_elapsed = time.time() - pipeline_start
    return {"stages": results, "errors": errors, "elapsed": total_elapsed}


def _run_streaming(ordered: list[str], min_score: int, top_n: int = 10,
                   workers: int = 1, validation_mode: str = "normal") -> dict:
    """Execute stages concurrently with DB as conveyor belt."""
    tracker = _StageTracker()
    stop_event = threading.Event()
    pipeline_start = time.time()

    console.print(f"\n  [bold cyan]STREAMING MODE[/bold cyan] — stages run concurrently")
    console.print(f"  Poll interval: {_STREAM_POLL_INTERVAL}s\n")

    # Mark stages NOT in `ordered` as done so downstream doesn't wait for them
    for stage in STAGE_ORDER:
        if stage not in ordered:
            tracker.mark_done(stage, {"status": "skipped"})

    # Launch each stage in its own thread
    threads: dict[str, threading.Thread] = {}
    start_times: dict[str, float] = {}

    for name in ordered:
        start_times[name] = time.time()
        t = threading.Thread(
            target=_run_stage_streaming,
            args=(name, tracker, stop_event, min_score, top_n, workers, validation_mode),
            name=f"stage-{name}",
            daemon=True,
        )
        threads[name] = t
        t.start()
        console.print(f"  [dim]Started thread:[/dim] {name}")

    # Wait for all threads to finish
    try:
        for name in ordered:
            threads[name].join()
            elapsed = time.time() - start_times[name]
            console.print(
                f"  [green]Completed:[/green] {name} ({elapsed:.1f}s)"
            )
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted — stopping stages...[/yellow]")
        stop_event.set()
        for t in threads.values():
            t.join(timeout=10)

    total_elapsed = time.time() - pipeline_start

    # Build results from tracker
    all_results = tracker.get_results()
    results: list[dict] = []
    errors: dict[str, str] = {}

    for name in ordered:
        r = all_results.get(name, {"status": "unknown"})
        elapsed = time.time() - start_times.get(name, pipeline_start)
        status = r.get("status", "ok")

        results.append({"stage": name, "status": status, "elapsed": elapsed})
        if status not in ("ok", "partial", "skipped"):
            errors[name] = status

    return {"stages": results, "errors": errors, "elapsed": total_elapsed}


def run_pipeline(
    stages: list[str] | None = None,
    min_score: int = 7,
    top_n: int = 10,
    dry_run: bool = False,
    stream: bool = False,
    workers: int = 1,
    validation_mode: str = "normal",
) -> dict:
    """Run pipeline stages.

    Args:
        stages: List of stage names, or None / ["all"] for full pipeline.
        min_score: Minimum fit score for tailor/cover stages.
        top_n: Maximum number of qualifying jobs to keep after scoring.
        dry_run: If True, preview stages without executing.
        stream: If True, run stages concurrently (streaming mode).
        workers: Number of parallel threads for discovery/enrichment stages.

    Returns:
        Dict with keys: stages (list of result dicts), errors (dict), elapsed (float).
    """
    # Bootstrap
    load_env()
    ensure_dirs()
    init_db()

    # Resolve stages
    if stages is None:
        stages = ["all"]
    ordered = _resolve_stages(stages)

    # Banner
    mode = "streaming" if stream else "sequential"
    console.print()
    console.print(Panel.fit(
        f"[bold]ApplyPilot Pipeline[/bold] ({mode})",
        border_style="blue",
    ))
    console.print(f"  Min score:  {min_score}")
    console.print(f"  Workers:    {workers}")
    console.print(f"  Validation: {validation_mode}")
    console.print(f"  Stages:     {' -> '.join(ordered)}")

    # Pre-run stats
    pre_stats = get_stats()
    console.print(f"  DB:        {pre_stats['total']} jobs, {pre_stats['pending_detail']} pending enrichment")

    if dry_run:
        console.print(f"\n  [yellow]DRY RUN[/yellow] — would execute ({mode}):")
        for name in ordered:
            meta = STAGE_META[name]
            console.print(f"    {name:<12s}  {meta['desc']}")
        console.print(f"\n  No changes made.")
        return {"stages": [], "errors": {}, "elapsed": 0.0}

    # Execute
    if stream:
        result = _run_streaming(ordered, min_score, top_n=top_n, workers=workers,
                                validation_mode=validation_mode)
    else:
        result = _run_sequential(ordered, min_score, top_n=top_n, workers=workers,
                                 validation_mode=validation_mode)

    # Summary table
    console.print(f"\n{'=' * 70}")
    summary = Table(title="Pipeline Summary", show_header=True, header_style="bold")
    summary.add_column("Stage", style="bold")
    summary.add_column("Status")
    summary.add_column("Time", justify="right")

    for r in result["stages"]:
        elapsed_str = f"{r['elapsed']:.1f}s"
        status_display = r["status"][:30]
        if r["status"] == "ok":
            style = "green"
        elif r["status"] in ("partial", "skipped"):
            style = "yellow"
        else:
            style = "red"
        summary.add_row(r["stage"], f"[{style}]{status_display}[/{style}]", elapsed_str)

    summary.add_row("", "", "")
    summary.add_row("[bold]Total[/bold]", "", f"[bold]{result['elapsed']:.1f}s[/bold]")
    console.print(summary)

    # Final DB stats
    final = get_stats()
    console.print(f"\n  [bold]DB Final State:[/bold]")
    console.print(f"    Total jobs:     {final['total']}")
    console.print(f"    With desc:      {final['with_description']}")
    console.print(f"    Scored:         {final['scored']}")
    console.print(f"    Tailored:       {final['tailored']}")
    console.print(f"    Cover letters:  {final['with_cover_letter']}")
    console.print(f"    Ready to apply: {final['ready_to_apply']}")
    console.print(f"    Applied:        {final['applied']}")
    console.print(f"{'=' * 70}\n")

    return result
