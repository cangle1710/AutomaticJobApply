"""LinkedIn job scraper — skeleton implementation.

Approach
--------
LinkedIn exposes a *guest* (unauthenticated) job search endpoint that returns
rendered HTML cards.  It covers discovery but description text is truncated.
Full descriptions require a logged-in session.

Guest search endpoint::

    GET https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search
        ?keywords=<query>
        &location=<location>
        &start=<offset>
        &f_TPR=r<seconds>   # time range: r86400 = last 24 h
        &f_WT=2             # work type: 2 = remote

Anti-scraping notes
-------------------
LinkedIn aggressively blocks automated access after a few pages.  Production
use should layer in:
  - Authenticated session cookies (``li_at``, ``JSESSIONID``) from a real
    account.  Store in config, inject via ``AsyncHTTPClient.extra_headers``.
  - Rotating residential proxies (pass ``proxy`` to ``AsyncHTTPClient``).
  - Human-like delays: add ``await asyncio.sleep(random.uniform(1, 3))``
    between page fetches inside ``fetch_jobs``.
  - ``PlaywrightWrapper`` for pages that serve a CAPTCHA challenge.

TODO checklist
--------------
[ ] Implement ``_parse_guest_html`` — selectors documented below.
[ ] Add cookie-based auth for detail page fetching.
[ ] Wire up ``PlaywrightWrapper`` fallback for challenge pages.
[ ] Parse ``date_posted`` from ``<time datetime="…">`` ISO attribute.
[ ] Map LinkedIn job type codes to normalized strings.
"""

from __future__ import annotations

import asyncio
import random
from typing import AsyncIterator
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from ..base import BaseScraper
from ..http_client import AsyncHTTPClient

_RESULTS_PER_PAGE = 25


class LinkedInScraper(BaseScraper):
    """LinkedIn job scraper (guest endpoint wired; detail-page fetch TODO)."""

    source = "linkedin"

    _GUEST_SEARCH_URL = (
        "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    )

    def __init__(self, client: AsyncHTTPClient, config: dict) -> None:
        super().__init__(client, config)
        # Optional: inject session cookies for authenticated requests
        self._cookies: dict[str, str] = config.get("linkedin_cookies", {})

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------

    async def fetch_jobs(  # type: ignore[override]
        self,
        query: str,
        location: str,
        *,
        max_pages: int = 4,
        days_old: int = 7,
        remote_only: bool = False,
        **_: object,
    ) -> AsyncIterator[dict]:
        """Paginate LinkedIn guest job search, yielding one raw dict per card.

        Args:
            query:       Job search keywords.
            location:    Location string.
            max_pages:   Page limit (LinkedIn blocks aggressively past ~4).
            days_old:    Time range filter in days.
            remote_only: Apply LinkedIn's remote work-type filter (f_WT=2).
        """
        for page in range(max_pages):
            start = page * _RESULTS_PER_PAGE
            params: dict[str, object] = {
                "keywords": query,
                "location": location,
                "start": start,
                "f_TPR": f"r{days_old * 86_400}",
            }
            if remote_only:
                params["f_WT"] = "2"

            url = f"{self._GUEST_SEARCH_URL}?{urlencode(params)}"

            try:
                resp = await self.client.get(
                    url,
                    headers={
                        "Referer": "https://www.linkedin.com/",
                        "X-Requested-With": "XMLHttpRequest",
                    },
                    cookies=self._cookies,
                )
            except Exception as exc:
                self.logger.error("Page %d fetch failed: %s", page + 1, exc)
                break

            cards = self._parse_guest_html(resp.text)
            if not cards:
                self.logger.info(
                    "No cards on page %d — stopping pagination", page + 1
                )
                break

            self.logger.info("Page %d: %d cards", page + 1, len(cards))
            for card in cards:
                yield card

            # Polite delay between pages to reduce block probability
            await asyncio.sleep(random.uniform(1.5, 3.0))

    def parse_job(self, raw: dict) -> dict:
        """Map a LinkedIn HTML card dict to normalized fields."""
        return {
            "title": raw.get("title", ""),
            "company": raw.get("company", ""),
            "location": raw.get("location", ""),
            "description": raw.get("description", ""),
            "url": raw.get("url", ""),
            "date_posted": raw.get("date_posted"),
            "remote": raw.get("remote", False),
            "raw": raw,
        }

    # ------------------------------------------------------------------
    # Private helpers (stubs)
    # ------------------------------------------------------------------

    def _parse_guest_html(self, html: str) -> list[dict]:
        """Parse LinkedIn guest search result HTML into raw dicts.

        LinkedIn returns a fragment of ``<li>`` elements.  Each card uses:
          - ``h3.base-search-card__title``          → title
          - ``h4.base-search-card__subtitle``       → company
          - ``span.job-search-card__location``      → location
          - ``time[datetime]``                      → date_posted (ISO attr)
          - ``a.base-card__full-link[href]``        → url  (strip query params)
          - ``span.job-search-card__salary-info``   → salary text (optional)

        TODO: implement this method.
        """
        # --- implementation stub ---
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict] = []

        for card in soup.select("li.jobs-search__results-list > li, li.result-card"):
            title_el = card.select_one(
                "h3.base-search-card__title, h3[class*='title']"
            )
            company_el = card.select_one(
                "h4.base-search-card__subtitle, h4[class*='subtitle']"
            )
            location_el = card.select_one(
                "span.job-search-card__location, span[class*='location']"
            )
            time_el = card.select_one("time[datetime]")
            link_el = card.select_one("a.base-card__full-link, a[class*='full-link']")

            if not title_el:
                continue

            url = link_el["href"].split("?")[0] if link_el and link_el.get("href") else ""
            location_text = location_el.get_text(strip=True) if location_el else ""

            jobs.append(
                {
                    "title": title_el.get_text(strip=True),
                    "company": company_el.get_text(strip=True) if company_el else "",
                    "location": location_text,
                    "remote": "remote" in location_text.lower(),
                    "date_posted": time_el.get("datetime") if time_el else None,
                    "url": url,
                    "description": "",  # requires separate detail page fetch
                }
            )

        # TODO: add authenticated detail-page fetch to populate description
        return jobs
