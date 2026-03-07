import secrets
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Allegro
    allegro_client_id: str
    allegro_client_secret: str
    allegro_redirect_uri: str
    allegro_api_url: str = "https://api.allegro.pl"
    allegro_auth_url: str = "https://allegro.pl/auth/oauth"

    # Supabase
    supabase_url: str
    supabase_service_key: str

    # Security
    encryption_key: str = ""

    # App
    environment: str = "development"
    port: int = 8000
    frontend_url: str = "http://localhost:3000"

    # Sniper
    snipe_offset_ms: int = 100

    # ScraperAPI (residential proxy for allegro.pl scraping)
    scraper_api_key: str = ""

    def model_post_init(self, __context) -> None:
        if not self.encryption_key:
            from cryptography.fernet import Fernet
            import os
            key = Fernet.generate_key().decode()
            # Warn loudly — ephemeral key won't survive restarts
            print(
                "WARNING: ENCRYPTION_KEY not set. Generated ephemeral key — "
                "set it permanently in .env or stored tokens will break on restart."
            )
            object.__setattr__(self, "encryption_key", key)


settings = Settings()
