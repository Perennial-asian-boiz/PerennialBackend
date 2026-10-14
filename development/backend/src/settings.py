"""Validated process settings. Secrets are resolved only at startup."""

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, ValidationError, model_validator
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


def _quality_defaults() -> dict[str, QualityThreshold]:
    return {
        "congress_trades": QualityThreshold(),
        "ark_holdings": QualityThreshold(),
        "insider_trades": QualityThreshold(),
        "short_interest": QualityThreshold(allow_equal_as_of=True),
    }


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
