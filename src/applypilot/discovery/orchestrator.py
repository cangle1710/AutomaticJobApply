"""Async multi-source job discovery orchestrator.

Runs all enabled scrapers concurrently, aggregates their results,
deduplicates by URL, and returns a flat list of ``JobListing`` objects.

A single ``AsyncHTTPClient`` is shared across all scrapers so the global
request rate stays within the configured budget regardless of how many
sources are active.

Usage::

    import asyncio
    from applypilot.discovery.orchestrator import run_scrapers

    listings = asyncio.run(
        run_scrapers(
            queries=[
                {"query": "software engineer", "location": "remote"},
                {"query": "backend engineer",  "location": "New York, NY"},
            ],
            sources=["indeed"],          # omit to run all registered scrapers
        )
    )

Extending
---------
1. Create ``scrapers/<name>.py`` with a class that inherits ``BaseScraper``.
2. Add it to ``REGISTRY`` below.
3. Done — ``run_scrapers`` picks it up automatically.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional, Type

import yaml

from .base import BaseScraper, JobListing
from .http_client import AsyncHTTPClient
from .scrapers.hiring_cafe import HiringCafeScraper
from .scrapers.indeed import IndeedScraper
from .scrapers.linkedin import LinkedInScraper
from .scrapers.workday import WorkdayScraper, _url_hash

log = logging.getLogger("applypilot.discovery.orchestrator")

_WORKDAY_COMPANIES_YAML = (
    Path(__file__).parent.parent / "config" / "workday_companies.yaml"
)

# ---------------------------------------------------------------------------
# Scraper registry — the single place to add / remove sources
# ---------------------------------------------------------------------------

REGISTRY: dict[str, Type[BaseScraper]] = {
    "indeed": IndeedScraper,
    "linkedin": LinkedInScraper,
    "hiring_cafe": HiringCafeScraper,
    # WorkdayScraper is not in REGISTRY — it requires per-company instantiation.
    # Use run_workday_scrapers() instead.
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def run_scrapers(
    queries: list[dict],
    *,
    sources: Optional[list[str]] = None,
    config: Optional[dict] = None,
    proxy: Optional[str] = None,
    rate: float = 2.0,
    burst: int = 5,
) -> list[JobListing]:
    """Run all enabled scrapers concurrently and return deduplicated results.

    Args:
        queries:  List of search dicts.  Required keys: ``query``, ``location``.
                  Extra keys (``max_pages``, ``days_old``, ``remote_only``) are
                  forwarded to each scraper's ``fetch_jobs()``.
        sources:  Scraper names to enable.  Defaults to all in ``REGISTRY``.
        config:   Passed as-is to each scraper constructor (API keys, cookies, …).
        proxy:    HTTP proxy URL forwarded to the shared ``AsyncHTTPClient``.
        rate:     Sustained request rate (req/s) shared across all scrapers.
        burst:    Burst capacity for the shared rate limiter.

    Returns:
        Deduplicated list of ``JobListing`` objects, in discovery order.
    """
    enabled = _resolve_sources(sources)
    if not enabled:
        return []

    cfg = config or {}

    log.info(
        "Starting discovery: %d source(s) × %d query(ies) = %d tasks",
        len(enabled),
        len(queries),
        len(enabled) * len(queries),
    )

    async with AsyncHTTPClient(rate=rate, burst=burst, proxy=proxy) as client:
        tasks = [
            _safe_scrape(
                scraper=REGISTRY[source](client, cfg),
                query=q["query"],
                location=q.get("location", ""),
                query_cfg=q,
            )
            for source in enabled
            for q in queries
        ]
        results: list[list[JobListing] | BaseException] = await asyncio.gather(
            *tasks, return_exceptions=True
        )

    all_listings: list[JobListing] = []
    for result in results:
        if isinstance(result, BaseException):
            log.error("Scraper task raised an exception: %s", result, exc_info=result)
        else:
            all_listings.extend(result)

    unique = _deduplicate(all_listings)
    log.info(
        "Discovery complete: %d unique jobs from %d raw (%.0f%% dedup rate)",
        len(unique),
        len(all_listings),
        100 * (1 - len(unique) / max(len(all_listings), 1)),
    )
    return unique


# ---------------------------------------------------------------------------
# Workday multi-company entry point
# ---------------------------------------------------------------------------


def load_workday_companies(path: Optional[Path] = None) -> list[dict]:
    """Load the company list from workday_companies.yaml.

    Args:
        path: Override the default config path (useful for testing).

    Returns:
        List of company dicts with keys: name, tenant, site_id, wd_host.
    """
    target = path or _WORKDAY_COMPANIES_YAML
    if not target.exists():
        log.error("workday_companies.yaml not found at %s", target)
        return []
    data = yaml.safe_load(target.read_text(encoding="utf-8"))
    companies: list[dict] = data.get("companies", [])
    log.info("Loaded %d companies from %s", len(companies), target.name)
    return companies


async def run_workday_scrapers(
    queries: list[dict],
    *,
    companies: Optional[list[dict]] = None,
    companies_path: Optional[Path] = None,
    validate: bool = True,
    config: Optional[dict] = None,
    proxy: Optional[str] = None,
    rate: float = 1.0,
    burst: int = 10,
    dry_run: bool = False,
) -> list[JobListing]:
    """Scrape all Workday companies concurrently and return deduplicated results.

    One ``WorkdayScraper`` task is created per (company × query) pair and all
    tasks are awaited together.  A single ``AsyncHTTPClient`` is shared so the
    global request rate stays within ``rate`` req/s regardless of company count.

    Args:
        queries:        List of ``{"query": "...", "location": "..."}`` dicts.
        companies:      Override company list (skip YAML load).
        companies_path: Override path to workday_companies.yaml.
        validate:       Probe each tenant before scraping; skip invalid ones.
        config:         Extra config forwarded to each scraper constructor.
        proxy:          HTTP proxy URL.
        rate:           Sustained requests/sec for the shared HTTP client.
        burst:          Rate-limiter burst capacity.
        dry_run:        Log intent without making scrape requests.

    Returns:
        Deduplicated list of ``JobListing`` objects across all companies.
    """
    company_list = companies or load_workday_companies(companies_path)
    if not company_list:
        log.warning("No Workday companies configured — skipping")
        return []

    cfg = {**(config or {}), "dry_run": dry_run}

    log.info(
        "Workday discovery: %d companies × %d queries = %d tasks%s",
        len(company_list),
        len(queries),
        len(company_list) * len(queries),
        "  [DRY RUN]" if dry_run else "",
    )

    async with AsyncHTTPClient(rate=rate, burst=burst, proxy=proxy) as client:

        # Optional: validate all tenants in parallel before scraping
        if validate and not dry_run:
            log.info("─── WORKDAY VALIDATION PHASE  (%d companies) ───", len(company_list))
            company_list = await _validate_companies(client, company_list, cfg)
            if not company_list:
                log.error("No valid Workday companies after validation")
                return []
        else:
            log.info("─── WORKDAY VALIDATION SKIPPED  (%d companies) ───", len(company_list))

        log.info(
            "─── WORKDAY SCRAPING PHASE  %d companies × %d queries = %d tasks ───",
            len(company_list), len(queries), len(company_list) * len(queries),
        )

        tasks = [
            _safe_scrape_workday(
                scraper=WorkdayScraper(client, {**cfg, "company": company}),
                query=q["query"],
                location=q.get("location", ""),
                query_cfg=q,
            )
            for company in company_list
            for q in queries
        ]

        results: list[list[JobListing] | BaseException] = await asyncio.gather(
            *tasks, return_exceptions=True
        )

    all_listings: list[JobListing] = []
    for result in results:
        if isinstance(result, BaseException):
            log.error("Workday task raised: %s", result, exc_info=result)
        else:
            all_listings.extend(result)

    unique = _deduplicate(all_listings)
    log.info(
        "─── WORKDAY COMPLETE  %d unique jobs from %d raw ───",
        len(unique),
        len(all_listings),
    )
    return unique


async def _validate_companies(
    client: AsyncHTTPClient,
    companies: list[dict],
    config: dict,
) -> list[dict]:
    """Probe all companies concurrently; return only those that respond."""
    log.info("Validating %d Workday tenants...", len(companies))

    async def _probe(company: dict) -> tuple[dict, bool]:
        scraper = WorkdayScraper(client, {**config, "company": company})
        ok = await scraper.validate_company()
        # Use to_company_dict() so any auto-corrected wd_host propagates to scraping
        return scraper.to_company_dict(), ok

    results = await asyncio.gather(*[_probe(c) for c in companies], return_exceptions=True)

    valid: list[dict] = []
    invalid: list[str] = []
    for item in results:
        if isinstance(item, BaseException):
            log.debug("Validation probe raised: %s", item)
            continue
        company, ok = item  # type: ignore[misc]
        if ok:
            valid.append(company)
        else:
            invalid.append(company.get("name", company.get("tenant", "?")))

    if invalid:
        log.warning("  Skipping %d invalid tenants: %s", len(invalid), ", ".join(invalid))

    valid_names = [c.get("name", c.get("tenant", "?")) for c in valid]
    log.info(
        "  Validation complete: %d/%d tenants valid",
        len(valid), len(companies),
    )
    if valid_names:
        # Log in chunks of 10 so the line stays readable
        for i in range(0, len(valid_names), 10):
            log.info("  Valid: %s", ", ".join(valid_names[i:i + 10]))
    return valid


async def _safe_scrape_workday(
    scraper: WorkdayScraper,
    query: str,
    location: str,
    query_cfg: dict,
) -> list[JobListing]:
    """Isolated Workday scrape — one company crash never affects others."""
    kwargs = {k: v for k, v in query_cfg.items() if k not in ("query", "location")}
    try:
        results = await scraper.scrape(query, location, **kwargs)
        metrics = scraper.get_metrics()
        log.info(
            "  %-25s  query='%s'  →  %d jobs  (errors=%d)",
            metrics["company"], query, len(results), metrics["errors"],
        )
        return results
    except Exception as exc:
        log.error(
            "  Workday scraper [%s] '%s' failed: %s",
            scraper._name, query, exc, exc_info=True,
        )
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_sources(sources: Optional[list[str]]) -> list[str]:
    """Validate requested sources against the registry."""
    requested = sources or list(REGISTRY.keys())
    unknown = [s for s in requested if s not in REGISTRY]
    if unknown:
        log.warning("Unknown scraper sources (skipping): %s", unknown)
    return [s for s in requested if s in REGISTRY]


async def _safe_scrape(
    scraper: BaseScraper,
    query: str,
    location: str,
    query_cfg: dict,
) -> list[JobListing]:
    """Run a single scraper task with full error isolation.

    A crash in one source never cancels the others.
    """
    # Forward extra per-query config keys (max_pages, days_old, remote_only, …)
    kwargs = {k: v for k, v in query_cfg.items() if k not in ("query", "location")}

    try:
        results = await scraper.scrape(query, location, **kwargs)
        log.info(
            "[%s] '%s' @ '%s' → %d jobs",
            scraper.source,
            query,
            location,
            len(results),
        )
        return results
    except Exception as exc:
        log.error(
            "[%s] '%s' @ '%s' failed: %s",
            scraper.source,
            query,
            location,
            exc,
            exc_info=True,
        )
        return []


def _deduplicate(listings: list[JobListing]) -> list[JobListing]:
    """Remove duplicate listings by normalised URL, preserving discovery order."""
    seen: set[str] = set()
    unique: list[JobListing] = []
    for job in listings:
        key = job.url.rstrip("/").lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(job)
    return unique
