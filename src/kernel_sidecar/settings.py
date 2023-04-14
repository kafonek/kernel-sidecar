import functools

from pydantic import BaseSettings


class Settings(BaseSettings):
    pprint_logs: bool = False


# cached get_settings
@functools.lru_cache()
def get_settings():
    return Settings()
