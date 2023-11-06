import functools
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    pprint_logs: bool = False
    model_config = SettingsConfigDict(env_prefix="kernel_sidecar_")


# cached get_settings
@functools.lru_cache()
def get_settings():
    return Settings()
