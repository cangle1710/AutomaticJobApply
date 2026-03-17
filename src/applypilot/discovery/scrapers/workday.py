"""Universal async Workday scraper.

Scrapes any Workday-powered career portal via the internal CXS JSON API.
One ``WorkdayScraper`` instance maps to exactly one (tenant, site_id, wd_host)
triplet — i.e. one company.  The orchestrator creates one per company and
runs them concurrently via ``run_workday_scrapers()``.

API contract
------------
Search::

    POST https://{tenant}.{wd_host}.myworkdayjobs.com
         /wday/cxs/{tenant}/{site_id}/jobs
    Body: {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": "..."}
    Response: {"total": N, "jobPostings": [{title, locationsText, postedOn, externalPath}]}

Detail (optional)::

    POST https://{tenant}.{wd_host}.myworkdayjobs.com
         /wday/cxs/{tenant}/{site_id}/jobs/{externalPath}
    Body: {}
    Response: {"jobPostingInfo": {jobDescription, externalUrl, timeType, remoteType, ...}}

Events
------
Register a handler to receive ``"job.discovered"`` events::

    from applypilot.discovery.scrapers.workday import on_event

    @on_event
    def handle(name: str, payload: dict) -> None:
        print(name, payload["title"], payload["company"])

Dry-run mode
------------
Pass ``dry_run=True`` in the scraper config to log intent without yielding results::

    scraper = WorkdayScraper(client, {"company": {...}, "dry_run": True})
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import AsyncIterator, Callable, Optional

from ..base import BaseScraper, JobListing
from ..http_client import AsyncHTTPClient

log = logging.getLogger("applypilot.discovery.workday")

_PAGE_SIZE = 20
_MAX_PAGES = 50          # hard cap: 1 000 results per company per query
_VALIDATE_TIMEOUT = 10   # seconds for the probe request

# Ordered by likelihood — most Workday tenants are on wd5 or wd1
_WD_HOSTS = ("wd5", "wd1", "wd2", "wd3", "wd4", "wd6", "wd12", "wd103")

# Limit concurrent Playwright browser launches during parallel validation
# (each launch is ~300 MB RAM; 4 concurrent = ~1.2 GB)
_PW_SEM: Optional[asyncio.Semaphore] = None


def _get_pw_sem() -> asyncio.Semaphore:
    global _PW_SEM
    if _PW_SEM is None:
        _PW_SEM = asyncio.Semaphore(4)
    return _PW_SEM


# ---------------------------------------------------------------------------
# Event system — lightweight hook registry, zero new dependencies
# ---------------------------------------------------------------------------

_HOOKS: list[Callable[[str, dict], None]] = []


def on_event(fn: Callable[[str, dict], None]) -> Callable[[str, dict], None]:
    """Decorator / function to register a ``job.discovered`` event handler.

    Example::

        @on_event
        def my_handler(name: str, payload: dict) -> None:
            print(name, payload)
    """
    _HOOKS.append(fn)
    return fn


def _emit(name: str, payload: dict) -> None:
    """Fire an event to all registered handlers.  Never raises."""
    log.debug("EVENT  %-20s  company=%s  title=%s",
              name, payload.get("company"), payload.get("title"))
    for hook in _HOOKS:
        try:
            hook(name, payload)
        except Exception as exc:
            log.debug("Event hook %s raised: %s", getattr(hook, "__name__", hook), exc)


# ---------------------------------------------------------------------------
# WorkdayScraper
# ---------------------------------------------------------------------------


class WorkdayScraper(BaseScraper):
    """Async scraper for a single Workday-powered company career portal.

    Required config keys (nested under ``config["company"]``):
        tenant   (str)  e.g. "netflix"
        site_id  (str)  e.g. "Netflix"
        wd_host  (str)  e.g. "wd1" — the numeric Workday host suffix
        name     (str)  Human-readable company name (optional, defaults to tenant)

    Optional top-level config keys:
        dry_run   (bool)  Log intent without scraping.  Default False.
        fetch_detail (bool)  Fetch full job description per posting.  Default False
                             (description left empty for enrichment stage).
    """

    source = "workday"

    def __init__(self, client: AsyncHTTPClient, config: dict) -> None:
        super().__init__(client, config)
        company = config.get("company", {})
        self._tenant: str = company.get("tenant", "")
        self._site_id: str = company.get("site_id", "")
        self._wd_host: str = company.get("wd_host", "wd5")
        self._name: str = company.get("name") or self._tenant
        self._dry_run: bool = bool(config.get("dry_run", False))
        self._fetch_detail: bool = bool(config.get("fetch_detail", False))

        self._base_url = (
            f"https://{self._tenant}.{self._wd_host}.myworkdayjobs.com"
        )
        self._search_url = (
            f"{self._base_url}/wday/cxs/{self._tenant}/{self._site_id}/jobs"
        )

        # Logger scoped to this company so tenant shows up in log lines
        self.logger = logging.getLogger(
            f"applypilot.discovery.workday.{self._tenant}"
        )

        # Per-run metrics
        self._metrics: dict[str, int] = {
            "fetched": 0, "parsed": 0, "errors": 0, "skipped_location": 0
        }

    # ------------------------------------------------------------------
    # Session warm-up (CSRF fix)
    # ------------------------------------------------------------------

    async def _warm_session(self, base_url: Optional[str] = None) -> None:
        """GET the domain root to establish session cookies.

        Uses the bare domain (no path) so we always get a 200 regardless of
        whether site_id is correct.  The /{site_id} path can return 500 when
        the site_id is wrong, which prevents cookies from being set.
        """
        try:
            await self.client.get(
                (base_url or self._base_url) + "/",
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
            )
        except Exception:
            pass  # Best-effort; proceed even if warm-up fails

    async def _playwright_warm_session(self, base_url: str) -> bool:
        """Use a real Chromium browser to obtain a PLAY_SESSION cookie.

        httpx requests return 406 from Cloudflare's bot detection on many Workday
        tenants, so no PLAY_SESSION is ever set and subsequent POSTs get 422 CSRF
        errors.  Playwright passes Cloudflare's JS/TLS fingerprint checks and
        retrieves a genuine Workday session, which is then injected into the
        shared httpx cookie jar so API POSTs work.

        Uses a module-level semaphore to cap concurrent browser launches at 4.
        """
        from ..http_client import PlaywrightWrapper

        async with _get_pw_sem():
            try:
                async with PlaywrightWrapper() as pw:
                    async with pw.new_context() as ctx:
                        page = await ctx.new_page()
                        # Try the careers listing page; many return 500 for
                        # unknown site_ids but still set PLAY_SESSION. Fall
                        # back to the domain root if that raises.
                        for url in (f"{base_url}/{self._site_id}", base_url + "/"):
                            try:
                                await page.goto(
                                    url,
                                    wait_until="domcontentloaded",
                                    timeout=20_000,
                                )
                                break
                            except Exception:
                                continue
                        await page.wait_for_timeout(500)

                        cookies = await ctx.cookies()
                        if self.client._client:
                            for c in cookies:
                                self.client._client.cookies.set(
                                    c["name"], c["value"],
                                    domain=c.get("domain", ""),
                                )
                        play_session = any(c["name"] == "PLAY_SESSION" for c in cookies)
                        self.logger.debug(
                            "  Playwright warm: %s — %d cookies, PLAY_SESSION=%s",
                            self._name, len(cookies), play_session,
                        )
                        return play_session
            except Exception as exc:
                self.logger.debug(
                    "  Playwright warm-up failed for %s: %s", self._name, exc
                )
                return False

    async def _discover_host(self) -> bool:
        """Try common wd_hosts when the configured one causes a connection error.

        Called only when validate_company() gets a network-level failure (no
        HTTP status), meaning the domain itself doesn't exist.  Updates
        self._wd_host / self._base_url / self._search_url if a working host
        is found.

        Returns True if a valid host was discovered.
        """
        for host in _WD_HOSTS:
            if host == self._wd_host:
                continue  # already tried this one

            base = f"https://{self._tenant}.{host}.myworkdayjobs.com"
            search = f"{base}/wday/cxs/{self._tenant}/{self._site_id}/jobs"

            await self._warm_session(base_url=base)
            try:
                resp = await self.client.post(
                    search,
                    json={"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": "engineer"},
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Origin": base,
                        "Referer": f"{base}/{self._site_id}",
                    },
                    timeout=_VALIDATE_TIMEOUT,
                )
                data = resp.json()
                if "total" in data or "jobPostings" in data:
                    self.logger.info(
                        "  [ok] %s  (host corrected %s → %s)",
                        self._name, self._wd_host, host,
                    )
                    self._wd_host = host
                    self._base_url = base
                    self._search_url = search
                    return True
            except Exception:
                continue

        return False

    def to_company_dict(self) -> dict:
        """Return the current company config, including any auto-discovered values.

        Call this after validate_company() to get the (possibly corrected)
        wd_host that should be used for scraping.
        """
        return {
            "name": self._name,
            "tenant": self._tenant,
            "site_id": self._site_id,
            "wd_host": self._wd_host,
        }

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------

    async def fetch_jobs(  # type: ignore[override]
        self,
        query: str,
        location: str,
        *,
        max_pages: int = _MAX_PAGES,
        **_: object,
    ) -> AsyncIterator[dict]:
        """Paginate the Workday CXS API and yield one raw posting dict per job.

        Args:
            query:     Search term forwarded as ``searchText`` in the API body.
            location:  Unused by the Workday API directly — location filtering
                       is applied downstream by the pipeline's location rules.
            max_pages: Hard page cap per company per query run.
        """
        if self._dry_run:
            self.logger.info(
                "[DRY RUN] Would scrape %-25s  query='%s'", self._name, query
            )
            return

        if not self._tenant or not self._site_id:
            self.logger.error("Missing tenant or site_id — skipping %s", self._name)
            return

        await self._warm_session()

        self.logger.info(
            "► WORKDAY  company=%-25s  query='%s'  host=%s",
            self._name, query, self._wd_host,
        )

        total: Optional[int] = None
        seen_paths: set[str] = set()

        for page in range(max_pages):
            offset = page * _PAGE_SIZE
            payload = {
                "appliedFacets": {},
                "limit": _PAGE_SIZE,
                "offset": offset,
                "searchText": query,
            }

            try:
                resp = await self.client.post(
                    self._search_url,
                    json=payload,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Referer": self._base_url + "/",
                    },
                )
                data: dict = resp.json()
            except Exception as exc:
                self._metrics["errors"] += 1
                self.logger.error(
                    "  %s: API error at offset %d: %s",
                    self._name, offset, exc, exc_info=True,
                )
                break

            if total is None:
                total = int(data.get("total", 0))
                self.logger.info(
                    "  %s: %d total results on Workday", self._name, total
                )
                if total == 0:
                    break

            postings: list[dict] = data.get("jobPostings", [])
            if not postings:
                self.logger.debug(
                    "  %s: empty page at offset %d — stopping", self._name, offset
                )
                break

            self.logger.debug(
                "  %s: page %d (offset %d) — %d postings",
                self._name, page + 1, offset, len(postings),
            )

            new_on_page = 0
            for posting in postings:
                path = posting.get("externalPath", "")
                if path in seen_paths:
                    continue
                if path:
                    seen_paths.add(path)

                # Attach routing metadata so parse_job can build URLs
                posting["_tenant"] = self._tenant
                posting["_site_id"] = self._site_id
                posting["_wd_host"] = self._wd_host
                posting["_base_url"] = self._base_url
                posting["_company_name"] = self._name

                if self._fetch_detail and path:
                    posting = await self._enrich_with_detail(posting, path)

                self._metrics["fetched"] += 1
                new_on_page += 1
                yield posting

            self.logger.debug(
                "  %s: page %d complete — %d new", self._name, page + 1, new_on_page
            )

            if offset + _PAGE_SIZE >= (total or 0):
                break

        self.logger.info(
            "  %s: done — %d fetched | %d errors",
            self._name, self._metrics["fetched"], self._metrics["errors"],
        )

    def parse_job(self, raw: dict) -> dict:
        """Map a Workday raw posting to the normalized field set."""
        base_url = raw.get("_base_url", self._base_url)
        site_id = raw.get("_site_id", self._site_id)
        external_path = raw.get("externalPath", "")

        # Job URL: base_url / site_id / externalPath  (path already has leading /)
        url = f"{base_url}/{site_id}{external_path}" if external_path else ""

        location: str = raw.get("locationsText") or ""
        remote = (
            "remote" in location.lower()
            or str(raw.get("_remote_type", "")).lower() == "remote"
        )

        return {
            "title": raw.get("title") or "",
            "company": raw.get("_company_name") or self._name,
            "location": location,
            "description": raw.get("_description") or "",   # populated by detail fetch
            "url": url,
            "date_posted": _parse_workday_date(raw.get("postedOn") or ""),
            "job_type": raw.get("_time_type"),
            "remote": remote,
            "raw": raw,
        }

    def normalize(self, parsed: dict) -> JobListing:
        listing = super().normalize(parsed)
        if listing.url:
            _emit("job.discovered", {
                "title": listing.title,
                "company": listing.company,
                "location": listing.location,
                "url": listing.url,
                "source": self.source,
            })
            self._metrics["parsed"] += 1
        return listing

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    async def validate_company(self) -> bool:
        """Probe the Workday API with an empty search to verify the tenant exists.

        Returns True if the tenant responds with a valid JSON payload.
        Logs a warning and returns False on any failure (404, timeout, bad JSON).

        Use this before a full scrape to skip invalid entries in the YAML::

            if not await scraper.validate_company():
                return []
        """
        await self._warm_session()
        try:
            resp = await self.client.post(
                self._search_url,
                json={"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": "engineer"},
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Origin": self._base_url,
                    "Referer": f"{self._base_url}/{self._site_id}",
                },
                timeout=_VALIDATE_TIMEOUT,
            )
            data = resp.json()
            is_valid = "total" in data or "jobPostings" in data
            if is_valid:
                self.logger.info("  [ok] %s", self._name)
            else:
                self.logger.debug(
                    "  [skip] %s — unexpected response keys: %s",
                    self._name, list(data.keys()),
                )
            return is_valid
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)

            # 422 = CSRF failure — likely no PLAY_SESSION because Cloudflare blocked
            # the httpx warm-up.  Try a real browser to get a session, then retry.
            if status == 422:
                has_session = bool(
                    self.client._client
                    and self.client._client.cookies.get("PLAY_SESSION")
                )
                if not has_session:
                    self.logger.debug(
                        "  422 without PLAY_SESSION — trying Playwright warm-up for %s",
                        self._name,
                    )
                    await self._playwright_warm_session(self._base_url)
                    try:
                        resp = await self.client.post(
                            self._search_url,
                            json={"appliedFacets": {}, "limit": 1, "offset": 0,
                                  "searchText": "engineer"},
                            headers={
                                "Accept": "application/json",
                                "Content-Type": "application/json",
                                "Origin": self._base_url,
                                "Referer": f"{self._base_url}/{self._site_id}",
                            },
                            timeout=_VALIDATE_TIMEOUT,
                        )
                        data = resp.json()
                        if "total" in data or "jobPostings" in data:
                            self.logger.info(
                                "  [ok] %s  (Playwright warm-up)", self._name
                            )
                            return True
                    except Exception:
                        pass

            # For any failure (HTTP or network), try all known wd_hosts before
            # giving up — fixes tenants where the YAML has the wrong wd_host.
            if await self._discover_host():
                return True

            self.logger.debug(
                "  [skip] %s  (%s)",
                self._name, f"HTTP {status}" if status else type(exc).__name__,
            )
            return False

    # ------------------------------------------------------------------
    # Optional detail fetch
    # ------------------------------------------------------------------

    async def _enrich_with_detail(self, posting: dict, external_path: str) -> dict:
        """Fetch full job description from the detail endpoint.

        Only called when ``config["fetch_detail"] = True``.
        Failure is non-fatal — posting is returned as-is.
        """
        detail_url = (
            f"{self._base_url}/wday/cxs/{self._tenant}/{self._site_id}"
            f"{external_path}"
        )
        try:
            resp = await self.client.post(
                detail_url,
                json={},
                headers={"Accept": "application/json", "Content-Type": "application/json"},
            )
            info: dict = resp.json().get("jobPostingInfo", {})
            posting["_description"] = _strip_html(info.get("jobDescription") or "")
            posting["_time_type"] = info.get("timeType")
            posting["_remote_type"] = info.get("remoteType") or ""
            self.logger.debug(
                "  detail fetched: %-50s @ %s",
                posting.get("title", "?")[:50], self._name,
            )
        except Exception as exc:
            self._metrics["errors"] += 1
            self.logger.debug(
                "  detail fetch failed for %s / %s: %s",
                self._name, external_path, exc,
            )
        return posting

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def get_metrics(self) -> dict:
        """Return a snapshot of per-run counters."""
        return {
            "company": self._name,
            "tenant": self._tenant,
            **self._metrics,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_workday_date(date_str: str) -> Optional[datetime]:
    """Convert Workday's date strings to UTC datetimes.

    Workday uses ISO 8601 (``2024-01-15T00:00:00.000Z``) and also
    relative strings like ``"Posted 3 Days Ago"``.
    """
    if not date_str:
        return None

    # ISO 8601
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        pass

    # "Posted 30+ Days Ago" / "Posted Today"
    s = date_str.lower()
    now = datetime.now(timezone.utc)
    if "today" in s or "just" in s:
        return now
    m = re.search(r"(\d+)\+?\s*day", s)
    if m:
        from datetime import timedelta
        return now - timedelta(days=int(m.group(1)))

    return None


def _strip_html(html: str) -> str:
    """Remove HTML tags, collapse whitespace."""
    if not html:
        return ""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _url_hash(url: str) -> str:
    """Stable 16-char hex fingerprint of a URL for deduplication."""
    return hashlib.md5(url.strip().lower().encode()).hexdigest()[:16]
