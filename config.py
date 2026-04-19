import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    BOT_TOKEN: str
    QUIZ_TIMEOUT: int = 20  # Seconds the poll stays open

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

config = Settings()