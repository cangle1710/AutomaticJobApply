"""Hiring Cafe job scraper — skeleton implementation.

Approach
--------
Hiring Cafe (https://hiring.cafe) is backed by Algolia search.  The index
name and application ID are stable; the public search-only API key rotates
occasionally but is always discoverable from the page source.

Algolia query endpoint::

    POST https://{APP_ID}-dsn.algolia.net/1/indexes/{INDEX}/query
    Headers:
        x-algolia-application-id: {APP_ID}
        x-algolia-api-key:        {PUBLIC_KEY}
    Body (JSON):
        {
          "query":        "software engineer",
          "hitsPerPage":  50,
          "page":         0,
          "filters":      "remote:true"    # optional
        }

Known Algolia hit fields (inspect hiring.cafe network tab to verify current schema)::

    objectID, title, company_name, location, description,
    url / job_url, created_at (Unix epoch), is_remote,
    salary_min, salary_max, employment_type

TODO checklist
--------------
[ ] Implement ``_resolve_algolia_key`` — fetch hiring.cafe HTML and regex the key.
[ ] Wire up pagination loop in ``fetch_jobs``.
[ ] Map ``created_at`` (epoch) to ``date_posted`` in ``parse_job``.
[ ] Handle Algolia's ``nbPages`` response field to avoid over-paginating.
[ ] Add location-based Algolia filter once field name is confirmed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import AsyncIterator, Optional

from ..base import BaseScraper
from ..http_client import AsyncHTTPClient

_ALGOLIA_APP_ID = "QBZL2BUEF4"
_ALGOLIA_INDEX = "jobs"
_HITS_PER_PAGE = 50


class HiringCafeScraper(BaseScraper):
    """Hiring Cafe scraper via Algolia search API (key resolution TODO)."""

    source = "hiring_cafe"

    _ALGOLIA_URL = (
        f"https://{_ALGOLIA_APP_ID}-dsn.algolia.net"
        f"/1/indexes/{_ALGOLIA_INDEX}/query"
    )
    _HOMEPAGE = "https://hiring.cafe"

    def __init__(self, client: AsyncHTTPClient, config: dict) -> None:
        super().__init__(client, config)
        # Allow key override via config for environments where page-scraping
        # the key is undesirable (e.g. CI pipelines).
        self._api_key: Optional[str] = config.get("hiring_cafe_algolia_key")

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------

    async def fetch_jobs(  # type: ignore[override]
        self,
        query: str,
        location: str,
        *,
        max_pages: int = 5,
        remote_only: bool = False,
        **_: object,
    ) -> AsyncIterator[dict]:
        """Paginate Hiring Cafe via Algolia, yielding one hit dict per job.

        Args:
            query:       Job search query.
            location:    Location string (used as Algolia filter once field confirmed).
            max_pages:   Maximum Algolia pages to fetch.
            remote_only: Filter to remote jobs only.
        """
        # Resolve the live Algolia API key if not pre-configured
        if not self._api_key:
            try:
                self._api_key = await self._resolve_algolia_key()
            except NotImplementedError:
                self.logger.warning(
                    "HiringCafeScraper: Algolia key not configured and "
                    "_resolve_algolia_key not yet implemented — skipping"
                )
                return

        headers = {
            "x-algolia-application-id": _ALGOLIA_APP_ID,
            "x-algolia-api-key": self._api_key,
            "Content-Type": "application/json",
        }

        for page in range(max_pages):
            payload: dict[str, object] = {
                "query": query,
                "hitsPerPage": _HITS_PER_PAGE,
                "page": page,
            }

            # TODO: confirm Algolia field name for remote filter
            if remote_only:
                payload["filters"] = "is_remote:true"

            # TODO: add location filter once field name is confirmed
            # if location:
            #     payload["filters"] = f"location:{location}"

            try:
                resp = await self.client.post(
                    self._ALGOLIA_URL, json=payload, headers=headers
                )
            except Exception as exc:
                self.logger.error("Algolia page %d fetch failed: %s", page, exc)
                break

            data = resp.json()
            hits: list[dict] = data.get("hits", [])
            total_pages: int = data.get("nbPages", 0)

            if not hits:
                self.logger.info("No hits on page %d — stopping pagination", page)
                break

            self.logger.info("Page %d/%d: %d hits", page + 1, total_pages, len(hits))
            for hit in hits:
                yield hit

            if page + 1 >= total_pages:
                break

    def parse_job(self, raw: dict) -> dict:
        """Map a Hiring Cafe Algolia hit to normalized fields."""
        # date_posted: Algolia stores Unix epoch in created_at
        date_posted: Optional[datetime] = None
        if ts := raw.get("created_at"):
            try:
                date_posted = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except (TypeError, ValueError):
                pass

        location: str = raw.get("location") or ""
        remote = bool(raw.get("is_remote", False)) or "remote" in location.lower()

        return {
            "title": raw.get("title") or "",
            "company": raw.get("company_name") or raw.get("company") or "",
            "location": location,
            "description": raw.get("description") or "",
            "url": raw.get("url") or raw.get("job_url") or "",
            "date_posted": date_posted,
            "salary_min": _to_float(raw.get("salary_min")),
            "salary_max": _to_float(raw.get("salary_max")),
            "job_type": raw.get("employment_type"),
            "remote": remote,
            "raw": raw,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _resolve_algolia_key(self) -> str:
        """Scrape the Hiring Cafe homepage to extract the live Algolia public key.

        The key is embedded in one of the ``<script>`` tags as:
            ``apiKey":"<32-char-alphanum>"``

        TODO: implement this method.
            1. ``resp = await self.client.get(self._HOMEPAGE)``
            2. Regex: ``r'apiKey["\s:]+([a-zA-Z0-9]{32})'``
            3. Cache result on ``self._api_key`` so we only fetch once per run.
        """
        raise NotImplementedError(
            "Algolia key resolution not yet implemented.  "
            "Set 'hiring_cafe_algolia_key' in your scraper config, "
            "or implement _resolve_algolia_key."
        )


def _to_float(value: object) -> Optional[float]:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
