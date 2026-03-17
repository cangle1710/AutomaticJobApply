"""Indeed native scraper — full implementation.

Strategy
--------
1. GET the HTML search page.  Indeed embeds all job-card data as a JSON blob
   inside a ``<script>`` tag keyed on ``mosaic-provider-jobcards``.  We
   extract that JSON with a regex; this avoids touching the DOM at all and
   survives layout changes in the surrounding HTML.

2. If the JSON extraction fails (blocked response, blob moved), we fall back
   to parsing ``<div data-jk=…>`` job cards directly from the HTML.

3. Pagination uses the ``start`` offset parameter.  We stop when a page
   returns no new job keys (de-duplication guard) or when ``max_pages`` is
   reached.

Rate limiting and retries are handled transparently by ``AsyncHTTPClient``.
Description snippets are available from the search page; full descriptions
require a separate detail-page fetch (handled by the enrichment stage).
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from ..base import BaseScraper
from ..http_client import AsyncHTTPClient

_RESULTS_PER_PAGE = 15


class IndeedScraper(BaseScraper):
    """Full native scraper for Indeed job search (no third-party wrappers)."""

    source = "indeed"

    _SEARCH_URL = "https://www.indeed.com/jobs"
    _DETAIL_URL = "https://www.indeed.com/viewjob"

    # Matches the JSON blob Indeed injects for job card data.
    # The blob sits between two ``window.mosaic`` assignments.
    _MOSAIC_RE = re.compile(
        r'window\.mosaic\.providerData\["mosaic-provider-jobcards"\]\s*=\s*'
        r"(\{.*?\});\s*window\.mosaic",
        re.DOTALL,
    )

    def __init__(self, client: AsyncHTTPClient, config: dict) -> None:
        super().__init__(client, config)
        self._country = config.get("country_indeed", "usa").lower()

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------

    async def fetch_jobs(  # type: ignore[override]
        self,
        query: str,
        location: str,
        *,
        max_pages: int = 5,
        days_old: int = 7,
        remote_only: bool = False,
        **_: object,
    ) -> AsyncIterator[dict]:
        """Paginate through Indeed search results, yielding one raw dict per job.

        Args:
            query:       Job search query string.
            location:    Location string (city, state, "remote", etc.).
            max_pages:   Maximum pages to fetch (15 results each).
            days_old:    Only return jobs posted within this many days.
            remote_only: Apply Indeed's built-in remote filter.
        """
        seen_keys: set[str] = set()

        for page in range(max_pages):
            start = page * _RESULTS_PER_PAGE
            params: dict[str, object] = {
                "q": query,
                "l": location,
                "limit": _RESULTS_PER_PAGE,
                "start": start,
                "fromage": days_old,
                "sort": "date",
            }
            if remote_only:
                # Indeed's internal GUID for the "Remote" work type filter
                params["remotejob"] = "032b3046-06a3-4876-8dfd-474eb5e7ed11"

            url = f"{self._SEARCH_URL}?{urlencode(params)}"

            self.logger.info(
                "► INDEED  query=%-30s  location=%-20s  page=%d/%d  days_old=%d%s",
                f"'{query}'", f"'{location}'", page + 1, max_pages, days_old,
                "  [remote]" if remote_only else "",
            )
            try:
                resp = await self.client.get(
                    url, headers={"Referer": "https://www.indeed.com/"}
                )
            except Exception as exc:
                self.logger.error("Page %d fetch failed: %s", page + 1, exc)
                break

            jobs = self._extract_jobs(resp.text)
            if not jobs:
                self.logger.info(
                    "No jobs on page %d — stopping pagination", page + 1
                )
                break

            new_on_page = 0
            for job in jobs:
                key = (
                    job.get("jobkey")
                    or job.get("jobKey")
                    or job.get("url", "")
                )
                if key and key in seen_keys:
                    continue
                if key:
                    seen_keys.add(key)
                new_on_page += 1
                yield job

            self.logger.info(
                "Page %d: %d raw / %d new", page + 1, len(jobs), new_on_page
            )
            if new_on_page == 0:
                break  # all results already seen — nothing new on subsequent pages

    def parse_job(self, raw: dict) -> dict:
        """Map an Indeed raw dict to normalized fields."""
        job_key = raw.get("jobkey") or raw.get("jobKey", "")
        url = raw.get("url") or (
            f"{self._DETAIL_URL}?jk={job_key}" if job_key else ""
        )

        location: str = (
            raw.get("location")
            or raw.get("jobLocationCity")
            or raw.get("formattedLocation")
            or ""
        )
        remote = (
            "remote" in location.lower()
            or bool(raw.get("remoteLocation"))
            or bool(raw.get("isRemote"))
        )

        # Salary — Indeed nests this differently across API versions
        salary = raw.get("extractedSalary") or raw.get("salarySnippet") or {}
        salary_min: Optional[float] = None
        salary_max: Optional[float] = None
        salary_currency = "USD"
        salary_interval = ""
        if isinstance(salary, dict):
            salary_min = _to_float(salary.get("min") or salary.get("salaryMin"))
            salary_max = _to_float(salary.get("max") or salary.get("salaryMax"))
            salary_currency = salary.get("currency") or "USD"
            salary_interval = salary.get("type") or salary.get("interval") or ""

        # Description: snippet from search page; full text from enrichment stage
        description: str = (
            raw.get("snippet")
            or raw.get("jobDescription")
            or raw.get("description")
            or ""
        )
        if "<" in description:
            description = BeautifulSoup(description, "html.parser").get_text(
                " ", strip=True
            )

        return {
            "title": raw.get("title") or raw.get("jobTitle") or "",
            "company": raw.get("company") or raw.get("companyName") or "",
            "location": location,
            "description": description,
            "url": url,
            "date_posted": _parse_relative_date(
                raw.get("date_text")
                or raw.get("pubDate")
                or raw.get("formattedRelativeTime")
                or raw.get("dateActuallyPosted")
                or ""
            ),
            "salary_min": salary_min,
            "salary_max": salary_max,
            "salary_currency": salary_currency,
            "salary_interval": salary_interval,
            "job_type": raw.get("jobType") or raw.get("employmentType"),
            "remote": remote,
            "raw": raw,
        }

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _extract_jobs(self, html: str) -> list[dict]:
        """Try JSON extraction first, fall back to HTML card parsing."""
        jobs = self._extract_from_mosaic_json(html)
        if jobs:
            return jobs
        self.logger.debug(
            "Mosaic JSON extraction failed — falling back to HTML card parsing"
        )
        return self._parse_html_cards(html)

    def _extract_from_mosaic_json(self, html: str) -> list[dict]:
        """Extract job data from Indeed's embedded ``mosaic-provider-jobcards`` blob."""
        match = self._MOSAIC_RE.search(html)
        if not match:
            return []

        try:
            data: dict = json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            self.logger.debug("Mosaic JSON parse error: %s", exc)
            return []

        # Indeed nests the results list at different paths across versions
        for path in (
            ["metaData", "mosaicProviderJobCardsModel", "results"],
            ["results"],
            ["data", "results"],
        ):
            node: object = data
            for key in path:
                node = node.get(key) if isinstance(node, dict) else None  # type: ignore[union-attr]
                if node is None:
                    break
            if isinstance(node, list):
                return node  # type: ignore[return-value]

        return []

    def _parse_html_cards(self, html: str) -> list[dict]:
        """Fallback: extract job cards directly from rendered HTML."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict] = []

        # Try multiple card selectors in priority order
        cards = []
        for selector in ("div.job_seen_beacon", "div[data-jk]", "li[data-jk]"):
            cards = soup.select(selector)
            if cards:
                break

        for card in cards:
            job_key = card.get("data-jk", "")

            title_el = card.select_one(
                "h2.jobTitle span[title], h2.jobTitle a, [data-testid='jobTitle']"
            )
            company_el = card.select_one(
                "[data-testid='company-name'], .companyName, [class*='companyName']"
            )
            location_el = card.select_one(
                "[data-testid='text-location'], .companyLocation, [class*='companyLocation']"
            )
            date_el = card.select_one(
                "[data-testid='myJobsStateDate'], .date, [class*='date']"
            )
            snippet_el = card.select_one(
                ".job-snippet, [class*='snippet'], ul[class*='css']"
            )

            if not title_el:
                continue

            jobs.append(
                {
                    "jobkey": job_key,
                    "title": title_el.get_text(strip=True),
                    "company": company_el.get_text(strip=True) if company_el else "",
                    "location": location_el.get_text(strip=True) if location_el else "",
                    "date_text": date_el.get_text(strip=True) if date_el else "",
                    "snippet": snippet_el.get_text(" ", strip=True) if snippet_el else "",
                    "url": f"{self._DETAIL_URL}?jk={job_key}" if job_key else "",
                }
            )

        return jobs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_relative_date(date_str: str) -> Optional[datetime]:
    """Convert Indeed's relative date strings to UTC datetimes.

    Handles: "Just posted", "Today", "Yesterday", "3 days ago",
    "30+ days ago", ISO strings, and Unix timestamps.
    """
    if not date_str:
        return None

    s = date_str.lower().strip()
    now = datetime.now(timezone.utc)

    if any(k in s for k in ("just posted", "today", "active today")):
        return now
    if "yesterday" in s:
        return now - timedelta(days=1)

    m = re.search(r"(\d+)\+?\s*day", s)
    if m:
        return now - timedelta(days=int(m.group(1)))

    try:
        return datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        pass

    try:
        return datetime.fromtimestamp(float(date_str), tz=timezone.utc)
    except (ValueError, TypeError):
        pass

    return None


def _to_float(value: object) -> Optional[float]:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
