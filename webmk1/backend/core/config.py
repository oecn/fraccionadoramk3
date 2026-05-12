from functools import lru_cache
from os import getenv

from dotenv import load_dotenv


load_dotenv()


class Settings:
    def __init__(self):
        db_url = getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL no está definida. Crea el archivo .env basándote en .env.example")
        self.database_url: str = db_url


@lru_cache
def get_settings() -> Settings:
    return Settings()

