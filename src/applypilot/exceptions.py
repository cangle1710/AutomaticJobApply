"""ApplyPilot custom exception hierarchy.

All application-specific exceptions inherit from ``ApplyPilotError``, enabling
callers to catch the entire hierarchy with a single clause when needed, or to
catch specific sub-types for targeted handling.

Hierarchy
---------
ApplyPilotError
├── ConfigError          — missing / invalid configuration
├── DatabaseError        — SQLite operation failure
├── LLMError             — any LLM request or response failure
│   ├── LLMRateLimitError  — provider 429 / 503 rate limit
│   └── LLMTimeoutError    — request timed out after all retries
├── ValidationError      — LLM output failed content checks
├── ScrapeError          — non-recoverable scraping failure
└── EnrichmentError      — detail enrichment failure
"""


class ApplyPilotError(Exception):
    """Base class for all ApplyPilot exceptions."""


class ConfigError(ApplyPilotError):
    """Configuration is missing, invalid, or insufficient for the requested operation."""


class DatabaseError(ApplyPilotError):
    """A SQLite database operation failed."""


class LLMError(ApplyPilotError):
    """An LLM request or response failed."""


class LLMRateLimitError(LLMError):
    """The LLM provider returned a rate-limit (429) or overloaded (503) response."""


class LLMTimeoutError(LLMError):
    """The LLM request timed out after all retries were exhausted."""


class ValidationError(ApplyPilotError):
    """LLM output failed content validation checks.

    Attributes:
        errors:   Hard-failure messages that triggered the exception.
        warnings: Soft-failure messages that were logged but did not block.
    """

    def __init__(
        self,
        message: str,
        errors: list[str],
        warnings: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.errors: list[str] = errors
        self.warnings: list[str] = warnings or []

    def __str__(self) -> str:
        detail = "; ".join(self.errors)
        return f"{super().__str__()} — errors: [{detail}]"


class ScrapeError(ApplyPilotError):
    """A scraping operation failed in a non-recoverable way."""


class EnrichmentError(ApplyPilotError):
    """A detail-enrichment operation failed."""
