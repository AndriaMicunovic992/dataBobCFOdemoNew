from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    DATABASE_URL: str = "postgresql+asyncpg://databobiq:databobiq@localhost:5432/databobiq"
    DATABASE_URL_SYNC: str = "postgresql://databobiq:databobiq@localhost:5432/databobiq"

    # Generic key — falls back to role-specific keys when those are absent
    ANTHROPIC_API_KEY: str = ""
    # Separate API keys for the two different AI roles.
    # ANTHROPIC_API_KEY_CHAT  → scenario creation, general chat (services/chat.py)
    # ANTHROPIC_API_KEY_AGENT → data-understanding agent / schema detection (services/schema_agent.py)
    ANTHROPIC_API_KEY_CHAT: str = ""
    ANTHROPIC_API_KEY_AGENT: str = ""

    UPLOAD_DIR: str = "./uploads"
    CORS_ORIGINS: str = "http://localhost:5173"

    @model_validator(mode="after")
    def _normalise(self) -> "Settings":
        # Railway provides DATABASE_URL as postgresql:// (no +asyncpg) — derive both URLs
        if self.DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in self.DATABASE_URL:
            self.DATABASE_URL_SYNC = self.DATABASE_URL
            self.DATABASE_URL = self.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

        # Fall back to generic ANTHROPIC_API_KEY when role-specific keys are absent
        if not self.ANTHROPIC_API_KEY_CHAT:
            self.ANTHROPIC_API_KEY_CHAT = self.ANTHROPIC_API_KEY
        if not self.ANTHROPIC_API_KEY_AGENT:
            self.ANTHROPIC_API_KEY_AGENT = self.ANTHROPIC_API_KEY

        return self

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]


settings = Settings()
