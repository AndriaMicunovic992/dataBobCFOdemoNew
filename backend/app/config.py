from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    DATABASE_URL: str = "postgresql+asyncpg://databobiq:databobiq@localhost:5432/databobiq"
    DATABASE_URL_SYNC: str = "postgresql://databobiq:databobiq@localhost:5432/databobiq"

    # Separate API keys for the two different AI roles.
    # ANTHROPIC_API_KEY_CHAT  → scenario creation, general chat (services/chat.py)
    # ANTHROPIC_API_KEY_AGENT → data-understanding agent / schema detection (services/schema_agent.py)
    ANTHROPIC_API_KEY_CHAT: str = ""
    ANTHROPIC_API_KEY_AGENT: str = ""

    UPLOAD_DIR: str = "./uploads"
    CORS_ORIGINS: str = "http://localhost:5173"

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]


settings = Settings()
