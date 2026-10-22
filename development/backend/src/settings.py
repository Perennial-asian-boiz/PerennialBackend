"""Validated process settings. Secrets are resolved only at startup."""

import os
from datetime import date, time, timedelta
from pathlib import Path
from typing import Literal

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.db.config import DatabaseConfigError, parse_database_url
from src.services.consensus_watchlist.paths import ENV_PATH


class SettingsError(ValueError):
    """A fixed startup error that never contains a configured value."""


class QualityThreshold(BaseModel):
    min_count_ratio: float = Field(default=0.5, ge=0, le=1)
    max_count_ratio: float = Field(default=4.0, ge=1)
    null_share_margin: float = Field(default=0.05, ge=0, le=1)
    allow_equal_as_of: bool = False
    null_fields: tuple[str, ...] = ()


def _quality_defaults() -> dict[str, QualityThreshold]:
    return {
        "congress_trades": QualityThreshold(null_fields=("disclosure_date",)),
        "ark_holdings": QualityThreshold(null_fields=("share_price",)),
        "insider_trades": QualityThreshold(null_fields=("shares", "value")),
        "short_interest": QualityThreshold(
            allow_equal_as_of=True,
            null_fields=("short_interest_shares", "average_daily_volume"),
        ),
    }


class SourcePolicy(BaseModel):
    """M2 degraded publishing policy for one source (roadmap decision D7).

    max_age bounds any run a publication may pin; max_reuse_age bounds a run
    reused from the previous publication after today's collection failed.
    Every source is required: optional sources are not supported (M2 Q2).
    """

    required: Literal[True] = True
    max_reuse_age: timedelta
    max_age: timedelta

    @model_validator(mode="after")
    def reuse_within_max_age(self) -> "SourcePolicy":
        if not timedelta(0) < self.max_reuse_age <= self.max_age:
            raise ValueError("max_reuse_age must be positive and at most max_age")
        return self


def _source_policy_defaults() -> dict[str, SourcePolicy]:
    days = timedelta(days=1)
    return {
        "congress_trades": SourcePolicy(max_reuse_age=3 * days, max_age=3 * days),
        "ark_holdings": SourcePolicy(max_reuse_age=3 * days, max_age=3 * days),
        "insider_trades": SourcePolicy(max_reuse_age=3 * days, max_age=3 * days),
        "short_interest": SourcePolicy(max_reuse_age=15 * days, max_age=45 * days),
    }


MARKET_CAP_MAX_REUSE_AGE = timedelta(days=3)  # D7
PUBLICATION_DEADLINE = time(10, 30)  # D6, in PUBLICATION_TIMEZONE
PUBLICATION_TIMEZONE = "America/Los_Angeles"
# NYSE full-day closures (M2 Q6). Weekends are never trading days. Extend yearly.
TRADING_HOLIDAYS: tuple[date, ...] = tuple(
    date.fromisoformat(d)
    for d in (
        "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
        "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
        "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31",
        "2027-06-18", "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
    )
)  # fmt: skip


class ProviderSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None, env_ignore_empty=True, extra="ignore", case_sensitive=False
    )

    fmp_api_key: SecretStr | None = None
    finnhub_api_key: SecretStr | None = None

    def require_provider_keys(self, provider: Literal["fmp", "finnhub"]) -> None:
        key = self.fmp_api_key if provider == "fmp" else self.finnhub_api_key
        if key is None or key.get_secret_value().lower().startswith("your_"):
            name = "FMP_API_KEY" if provider == "fmp" else "FINNHUB_API_KEY"
            raise SettingsError(f"{name} is required for live collection")


class Settings(ProviderSettings):
    database_url: SecretStr
    fmp_max_pages: int = Field(default=50, ge=1, le=1000)
    quality_thresholds: dict[str, QualityThreshold] = Field(
        default_factory=_quality_defaults
    )
    source_policies: dict[str, SourcePolicy] = Field(
        default_factory=_source_policy_defaults
    )
    market_cap_max_reuse_age: timedelta = Field(
        default=MARKET_CAP_MAX_REUSE_AGE, gt=timedelta(0)
    )
    trading_holidays: tuple[date, ...] = TRADING_HOLIDAYS

    @field_validator("source_policies")
    @classmethod
    def complete_source_policies(
        cls, value: dict[str, SourcePolicy]
    ) -> dict[str, SourcePolicy]:
        defaults = _source_policy_defaults()
        if set(value) - set(defaults):
            raise ValueError("unknown source")
        return {**defaults, **value}

    @model_validator(mode="after")
    def complete_quality_thresholds(self) -> "Settings":
        defaults = _quality_defaults()
        if set(self.quality_thresholds) - set(defaults):
            raise ValueError("QUALITY_THRESHOLDS contains an unknown source")
        self.quality_thresholds = {
            source: defaults[source].model_copy(
                update=self.quality_thresholds[source].model_dump(exclude_unset=True)
            )
            if source in self.quality_thresholds
            else defaults[source]
            for source in defaults
        }
        return self


def load_provider_settings(
    *, local_dev: bool | None = None, env_path: Path | None = None
) -> ProviderSettings:
    """Compatibility settings for the JSON-only consensus command."""
    if local_dev is None:
        local_dev = os.environ.get("PERENNIAL_ENV") == "local"
    try:
        return ProviderSettings(  # type: ignore[call-arg]
            _env_file=(ENV_PATH if env_path is None else env_path)
            if local_dev
            else None
        )
    except ValidationError as exc:
        names = sorted({str(error["loc"][0]).upper() for error in exc.errors()})
        raise SettingsError(f"invalid or missing setting: {', '.join(names)}") from None


def load_settings(
    *, local_dev: bool | None = None, env_path: Path | None = None
) -> Settings:
    """Read environment once; opt into .env only for an explicit local process."""
    if local_dev is None:
        local_dev = os.environ.get("PERENNIAL_ENV") == "local"
    try:
        settings = Settings(  # type: ignore[call-arg]  # pydantic-settings runtime kwarg
            _env_file=(ENV_PATH if env_path is None else env_path)
            if local_dev
            else None
        )
    except ValidationError as exc:
        errors = exc.errors()
        if (
            len(errors) == 1
            and errors[0]["loc"] == ("database_url",)
            and errors[0]["type"] == "missing"
        ):
            raise SettingsError("DATABASE_URL is not set") from None
        names = sorted({str(error["loc"][0]).upper() for error in errors})
        raise SettingsError(f"invalid or missing setting: {', '.join(names)}") from None
    try:
        parse_database_url(settings.database_url.get_secret_value())
    except DatabaseConfigError:
        raise SettingsError("DATABASE_URL is invalid") from None
    return settings
