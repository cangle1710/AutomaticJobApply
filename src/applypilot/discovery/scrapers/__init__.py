"""Per-source scraper implementations.

Each module exposes one class that inherits from ``BaseScraper``:
  - IndeedScraper     — full native implementation
  - LinkedInScraper   — skeleton (guest search wired, detail page TODO)
  - HiringCafeScraper — skeleton (Algolia API wired, key resolution TODO)

To add a new source:
  1. Create ``scrapers/<source_name>.py`` with a class that extends BaseScraper.
  2. Register it in ``orchestrator.REGISTRY``.
  That's it — the orchestrator picks it up automatically.
"""

from .hiring_cafe import HiringCafeScraper
from .indeed import IndeedScraper
from .linkedin import LinkedInScraper
from .workday import WorkdayScraper

__all__ = ["IndeedScraper", "LinkedInScraper", "HiringCafeScraper", "WorkdayScraper"]
