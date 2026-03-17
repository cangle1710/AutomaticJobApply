# ApplyPilot — CLAUDE.md

## Project Overview
- 6-stage autonomous job application pipeline: discover → enrich → score → tailor → cover → pdf → apply
- SQLite is the conveyor belt between stages; every stage reads and writes the `jobs` table
- Async multi-source scraping layer sits under `discovery/`; legacy sync scrapers (jobspy, workday) remain untouched

---

## Architecture Rules (STRICT)

- **Never bypass the DB conveyor belt.** Stages communicate only through SQLite, never by passing objects in memory across stage boundaries.
- **Each stage must be independently runnable.** `applypilot run score` must work without having run `tailor` first.
- `pipeline.py` is the only place that imports and sequences stages. Never import one stage from another.
- `config.py` is the single source of all paths, env vars, and defaults. Never hardcode paths or env var names anywhere else.
- `database.py` owns the schema. Never run raw `CREATE TABLE` or `ALTER TABLE` outside of `database.py`.
- Thread-local SQLite connections (`get_connection()`) must be used in all threaded code. Never share a connection across threads.
- Do not add new top-level packages under `src/applypilot/` without explicit instruction.

---

## Service Boundaries

- `discovery/` — job sourcing only. No scoring, no LLM calls for fit, no resume logic.
- `enrichment/` — full description + apply URL extraction only. No scoring.
- `scoring/` — scorer, tailor, cover letter, validator, pdf. No scraping.
- `apply/` — form submission only. Reads job records; never writes scores or resumes.
- `wizard/` — setup only. Never called from pipeline stages.
- The new `discovery/scrapers/` async layer must not import from `scoring/`, `enrichment/`, or `apply/`.

---

## Scraper Guidelines (VERY IMPORTANT)

- Every scraper **must** extend `BaseScraper` from `discovery/base.py`.
- Implement exactly three methods: `fetch_jobs()` (async generator), `parse_job()`, and `normalize()` (optional override).
- `fetch_jobs()` **must** be an `AsyncIterator[dict]`, not a list. Memory must stay flat.
- `parse_job()` **must** return a plain `dict` — never a `JobListing` directly.
- `normalize()` must produce a `JobListing`. Never construct `JobListing` outside of `normalize()`.
- Register new scrapers only in `orchestrator.REGISTRY`. Never instantiate scrapers directly from pipeline code.
- Never import a scraper class directly from pipeline.py or cli.py — always go through `run_scrapers()`.
- Rate limiting lives in `AsyncHTTPClient`. Never add `time.sleep()` or `asyncio.sleep()` in scraper business logic except for polite inter-page delays.
- Retry logic lives in `AsyncHTTPClient._request()`. Never add retry loops inside a scraper.
- A scraper crashing must not kill other scrapers. `_safe_scrape()` in orchestrator handles this — do not re-raise in scraper methods.
- Selectors and regex patterns **will** break. Write extraction in layers: structured data first, CSS selectors second, regex last.

---

## LLM Usage Rules

- Never call the LLM client directly from a scraper or database layer.
- All LLM calls go through `llm.py`. Never instantiate `openai`, `google.generativeai`, or any SDK client outside of `llm.py`.
- Prompts must be defined as module-level constants (e.g. `SCORE_PROMPT`). Never build prompts inline with f-strings in the call site.
- Always validate LLM output before writing to DB. Use `scoring/validator.py`.
- Never trust LLM JSON without field validation. Always check required fields exist and have correct types.
- Retry on malformed LLM output (max 5 attempts, already in tailor/cover logic). Do not increase this limit.
- Never fabricate resume content. `FABRICATION_WATCHLIST` in `validator.py` is the guard — do not bypass it.

---

## Playwright / Automation Rules

- `PlaywrightWrapper` in `http_client.py` is the only place to launch a browser for scraping.
- `apply/chrome.py` manages browsers for form submission. These are two separate concerns — never merge them.
- Always use `async with pw.new_context()` — never reuse contexts across jobs.
- `wait_until="domcontentloaded"` is the default. Only use `"networkidle"` if the target page requires it and you can justify it.
- Never call `page.waitForTimeout()` with values over 5000ms.
- Always close pages and contexts in `finally` blocks. `PlaywrightWrapper.new_context()` is a context manager — use it.
- Never hardcode CDP ports. Use `BASE_CDP_PORT` from `apply/chrome.py`.

---

## Data Contracts

- `JobListing` fields are: `title`, `company`, `location`, `description`, `url`, `source`, `date_posted`, `salary_min`, `salary_max`, `salary_currency`, `salary_interval`, `job_type`, `remote`, `raw`.
- **Never remove or rename a field from `JobListing`.** Downstream code depends on all fields.
- All string fields on `JobListing` default to `""`, never `None`. Code can always call `.lower()` without a guard.
- The `jobs` table columns are defined in `database.py:init_db()`. Adding a column requires a migration there — nowhere else.
- `url` is the deduplication key. It must be non-empty on every stored job. Discard records with no URL.
- `strategy` column must be set on every INSERT: `"jobspy"`, `"workday"`, `"smartextract"`, or `"native_scraper"`.
- Never change the `store_jobspy_results()` signature — it is called from multiple places.

---

## Error Handling & Logging

- Use `logging.getLogger(__name__)` in every module. Never use `print()` for operational output.
- In scrapers, log at `WARNING` for skipped records, `ERROR` for failed pages, `INFO` for page-level stats.
- Never swallow exceptions silently. At minimum: `log.error("...", exc_info=True)`.
- `exc_info=True` is required on all `log.error()` calls that catch an exception.
- Use structured `extra={}` kwargs on log calls where context (job URL, source, page number) aids debugging.
- HTTP 403/404 are permanent failures — do not retry. 408/429/5xx are transient — let `AsyncHTTPClient` retry.
- Never raise inside `BaseScraper.scrape()` for individual record failures — log and continue.

---

## Anti-Patterns

- **Do NOT** rewrite entire files. Make targeted edits only.
- **Do NOT** introduce new dependencies (frameworks, ORMs, HTTP clients, task queues) without explicit instruction.
- **Do NOT** use `pd.DataFrame` in the new async scraper layer. That's a jobspy legacy pattern.
- **Do NOT** use `time.sleep()` in async code. Use `asyncio.sleep()`.
- **Do NOT** call `asyncio.run()` inside an already-running event loop. Use `await` instead.
- **Do NOT** store secrets or API keys in code. All keys come from `.env` via `config.load_env()`.
- **Do NOT** add `Optional` fields to `JobListing` without a default. All new fields need defaults.
- **Do NOT** write migrations as ad-hoc SQL in module init. Migrations belong in `database.py:init_db()`.
- **Do NOT** import `pipeline.py` from anywhere. It is a top-level orchestrator only.
- **Do NOT** add `verbose` print statements or progress bars inside library code. Use `logging`.
- **Do NOT** write a new scraper that inherits directly from another scraper. Always inherit from `BaseScraper`.

---

## Current Focus

1. **Indeed scraper** — primary native scraper, mosaic JSON extraction + HTML fallback both implemented. Validate against live Indeed pages and fix selectors if broken.
2. **LinkedIn scraper** — guest endpoint wired. Priority next step: implement `_parse_guest_html()` and add authenticated cookie support.
3. **HiringCafe scraper** — Algolia pagination wired. Blocked on `_resolve_algolia_key()`. Either implement page-source extraction or accept key via `config["hiring_cafe_algolia_key"]`.
4. **Bridge to pipeline** — `_store_listings()` helper and `_run_discover()` integration block are documented in CLAUDE.md (see Option 2 in session history). Wire these in once Indeed is validated end-to-end.
5. **Do not touch** `jobspy.py`, `workday.py`, `smartextract.py`, or any `scoring/` module until the above is complete.
