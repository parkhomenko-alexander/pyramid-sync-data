import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    CELERY_BROKER_URL: str
    CELERY_RESULT_BACKEND: str

    API_BASE_URL: str
    API_USER: str
    API_PAS: str
    API_SOAP_BASE_URL: str
    API_SOAP_USER: str
    API_SOAP_PAS: str
    API_CALLS_DELAY: float = 0
    API_CALLS_DELAY_TIMEOUT_ERROR: float = 0
    APP_DATA_DIR: str
 
    DB_URI: str
    DB_ECHO: bool = False

    APPLICATION_PREFIX_BEHIND_PROXY: str
    APPLICATION_LOGGER_PATH: str
    APPLICATION_LOGGER_FILENAME: str
    APPLICATION_LOGGER_FORMAT: str
    APPLICATION_LOGGER_ROTATION: str
    APPLICATION_LOGGER_COMPRESSION: str
    APPLICATION_LOGGER_SERIALIZE: bool = True

    POSTGRES_DB: str
    POSTGRES_PASSWORD: str
    POSTGRES_USER: str

    ENERGY_SCHEDULE_TIME_DELTA: int = 1
      
    model_config = SettingsConfigDict(env_file=".env")

config = Config() # type: ignore