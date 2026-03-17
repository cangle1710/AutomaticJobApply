"""Job discovery layer.

Legacy entry points (jobspy, workday, smartextract) are preserved unchanged.
New async multi-source scraping system lives in:
  - base.py          — JobListing schema + BaseScraper ABC
  - http_client.py   — AsyncHTTPClient, RateLimiter, PlaywrightWrapper
  - orchestrator.py  — run_scrapers() async entry point
  - scrapers/        — per-source implementations
"""

from .base import BaseScraper, JobListing
from .orchestrator import REGISTRY, run_scrapers, run_workday_scrapers, load_workday_companies

__all__ = [
    "BaseScraper",
    "JobListing",
    "run_scrapers",
    "run_workday_scrapers",
    "load_workday_companies",
    "REGISTRY",
]
