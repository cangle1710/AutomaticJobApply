"""Unit tests for applypilot.discovery.base — JobListing and BaseScraper."""

import asyncio
from typing import AsyncIterator
from unittest.mock import MagicMock

import pytest

from applypilot.discovery.base import JobListing, BaseScraper


# ── JobListing ────────────────────────────────────────────────────────────


class TestJobListing:
    def _minimal(self, **overrides):
        defaults = {
            "title": "Eng",
            "company": "Co",
            "location": "NYC",
            "description": "desc",
            "url": "http://x.com",
            "source": "test",
        }
        defaults.update(overrides)
        return JobListing(**defaults)

    def test_required_fields(self):
        listing = self._minimal(title="Software Engineer", company="Acme")
        assert listing.title == "Software Engineer"
        assert listing.company == "Acme"

    def test_optional_fields_default_none(self):
        listing = self._minimal()
        assert listing.date_posted is None
        assert listing.salary_min is None
        assert listing.salary_max is None
        assert listing.job_type is None

    def test_remote_defaults_false(self):
        assert self._minimal().remote is False

    def test_salary_currency_defaults_usd(self):
        assert self._minimal().salary_currency == "USD"

    def test_raw_defaults_empty_dict(self):
        assert self._minimal().raw == {}

    def test_salary_range(self):
        listing = self._minimal(
            salary_min=100_000.0, salary_max=150_000.0, salary_interval="year"
        )
        assert listing.salary_min == 100_000.0
        assert listing.salary_max == 150_000.0
        assert listing.salary_interval == "year"

    def test_raw_not_in_repr(self):
        listing = self._minimal(raw={"huge": "payload"})
        assert "huge" not in repr(listing)


# ── Concrete scraper for testing ─────────────────────────────────────────


class _StubScraper(BaseScraper):
    """Minimal concrete scraper for exercising BaseScraper behaviour."""

    source = "stub"

    def __init__(self, items=None):
        # Bypass BaseScraper.__init__ which expects client/config
        self.client = MagicMock()
        self.config = {}
        self.logger = MagicMock()
        self._items = items or []

    async def fetch_jobs(self, query: str, location: str, **kwargs) -> AsyncIterator[dict]:
        for item in self._items:
            yield item

    def parse_job(self, raw: dict) -> dict:
        return raw


# ── BaseScraper ───────────────────────────────────────────────────────────


class TestBaseScraper:
    def test_normalize_maps_fields(self):
        scraper = _StubScraper()
        listing = scraper.normalize({
            "title": "Engineer",
            "company": "Acme",
            "location": "Remote",
            "description": "desc",
            "url": "http://example.com",
        })
        assert isinstance(listing, JobListing)
        assert listing.title == "Engineer"
        assert listing.company == "Acme"
        assert listing.source == "stub"

    def test_normalize_handles_missing_fields(self):
        listing = _StubScraper().normalize({})
        assert listing.title == ""
        assert listing.company == ""
        assert listing.url == ""

    def test_normalize_coerces_remote_to_bool(self):
        listing = _StubScraper().normalize({"remote": 1})
        assert listing.remote is True

        listing2 = _StubScraper().normalize({"remote": 0})
        assert listing2.remote is False

    def test_scrape_returns_listings(self):
        items = [
            {"title": "Dev", "company": "Corp", "url": "http://a.com"},
            {"title": "Senior", "company": "Corp", "url": "http://b.com"},
        ]
        results = asyncio.run(_StubScraper(items).scrape("dev", "NYC"))
        assert len(results) == 2
        assert all(isinstance(r, JobListing) for r in results)

    def test_scrape_discards_empty_url(self):
        items = [
            {"title": "Valid", "url": "http://a.com"},
            {"title": "No URL", "url": ""},
        ]
        results = asyncio.run(_StubScraper(items).scrape("dev", ""))
        assert len(results) == 1
        assert results[0].url == "http://a.com"

    def test_scrape_continues_on_parse_error(self):
        class _ErrorScraper(_StubScraper):
            _call_count = 0

            def parse_job(self, raw):
                self._call_count += 1
                if self._call_count == 1:
                    raise ValueError("Intentional error")
                return raw

        scraper = _ErrorScraper([
            {"url": "http://a.com"},   # will raise
            {"title": "Good", "url": "http://b.com"},  # will succeed
        ])
        results = asyncio.run(scraper.scrape("dev", ""))
        assert len(results) == 1
        assert results[0].url == "http://b.com"

    def test_source_set_on_listing(self):
        results = asyncio.run(
            _StubScraper([{"title": "Dev", "url": "http://x.com"}]).scrape("dev", "")
        )
        assert results[0].source == "stub"

    def test_scrape_empty_source(self):
        results = asyncio.run(_StubScraper([]).scrape("dev", ""))
        assert results == []

    def test_normalize_preserves_salary_currency(self):
        listing = _StubScraper().normalize({"salary_currency": "GBP"})
        assert listing.salary_currency == "GBP"

    def test_normalize_none_salary_currency_defaults_usd(self):
        listing = _StubScraper().normalize({"salary_currency": None})
        assert listing.salary_currency == "USD"
