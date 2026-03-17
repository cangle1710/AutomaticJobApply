"""Abstract base scraper + unified JobListing schema.

Every job board scraper inherits from ``BaseScraper`` and must implement:
  - ``source``     : str class attribute (e.g. "indeed")
  - ``fetch_jobs``: async generator that yields one raw dict per posting
  - ``parse_job`` : maps a raw dict to the normalized field set

The public entry point is ``scrape()``, which chains fetch → parse → normalize
and isolates per-record failures so one bad listing never aborts the run.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, AsyncIterator, Optional

if TYPE_CHECKING:
    from .http_client import AsyncHTTPClient


# ---------------------------------------------------------------------------
# Unified schema
# ---------------------------------------------------------------------------


@dataclass
class JobListing:
    """Canonical job record produced by every scraper.

    All string fields default to empty string (never None) so downstream
    code can always do ``listing.title.lower()`` without guard clauses.
    """

    title: str
    company: str
    location: str
    description: str
    url: str
    source: str

    # Optional enrichments — None means "not provided by this source"
    date_posted: Optional[datetime] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: str = "USD"
    salary_interval: str = ""       # "year" | "month" | "hour" | ""
    job_type: Optional[str] = None  # "fulltime" | "parttime" | "contract" | …
    remote: bool = False

    # Full raw payload for debugging / downstream enrichment
    raw: dict = field(default_factory=dict, repr=False)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class BaseScraper(ABC):
    """Abstract base class for all job board scrapers.

    Subclasses must:
      1. Set ``source`` as a class-level string (e.g. ``source = "indeed"``).
      2. Implement ``fetch_jobs()`` — async generator, one raw dict per job.
      3. Implement ``parse_job()`` — maps a raw dict to normalized fields.

    ``normalize()`` has a default implementation that covers the full
    ``JobListing`` schema; override it only for custom coercion logic.
    """

    source: str  # set at class level in each subclass

    def __init__(self, client: "AsyncHTTPClient", config: dict) -> None:
        self.client = client
        self.config = config
        self.logger = logging.getLogger(f"applypilot.discovery.{self.source}")

    @abstractmethod
    async def fetch_jobs(
        self, query: str, location: str, **kwargs: object
    ) -> AsyncIterator[dict]:
        """Yield raw job dicts from the source.

        Implementations own pagination internally and should yield one dict
        per job posting.  Raise on unrecoverable errors; log-and-skip
        individual bad records so the generator keeps running.
        """
        ...

    @abstractmethod
    def parse_job(self, raw: dict) -> dict:
        """Map a raw source dict to the normalized field set.

        Must return a dict with at minimum: title, company, location,
        description, url.

        Optional keys: date_posted, salary_min, salary_max, salary_currency,
        salary_interval, job_type, remote, raw.
        """
        ...

    def normalize(self, parsed: dict) -> JobListing:
        """Convert parsed fields into a typed ``JobListing``.

        Override only if you need custom coercion beyond the default mapping.
        """
        return JobListing(
            title=parsed.get("title") or "",
            company=parsed.get("company") or "",
            location=parsed.get("location") or "",
            description=parsed.get("description") or "",
            url=parsed.get("url") or "",
            source=self.source,
            date_posted=parsed.get("date_posted"),
            salary_min=parsed.get("salary_min"),
            salary_max=parsed.get("salary_max"),
            salary_currency=parsed.get("salary_currency") or "USD",
            salary_interval=parsed.get("salary_interval") or "",
            job_type=parsed.get("job_type"),
            remote=bool(parsed.get("remote", False)),
            raw=parsed.get("raw") or {},
        )

    async def scrape(
        self, query: str, location: str, **kwargs: object
    ) -> list[JobListing]:
        """Full pipeline: fetch → parse → normalize.

        Per-job parse/normalize errors are logged and skipped; the caller
        receives all successfully processed listings.
        """
        results: list[JobListing] = []

        async for raw in self.fetch_jobs(query, location, **kwargs):
            try:
                parsed = self.parse_job(raw)
                listing = self.normalize(parsed)
                if listing.url:  # discard records with no URL — they can't be applied to
                    results.append(listing)
            except Exception as exc:
                self.logger.warning(
                    "parse_job failed — skipping record: %s",
                    exc,
                    extra={"raw_keys": list(raw.keys()) if isinstance(raw, dict) else None},
                )

        return results
