from celery import Celery
from celery.schedules import crontab

from config import config

from loguru import logger

logger.info(["Init celery", config.CELERY_BROKER_URL])

celery_app = Celery(
    __name__,
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND,
    include=[
        "app.celery.tasks.building.tasks",
        "app.celery.tasks.data.tasks",
        "app.celery.tasks.tag.tasks",
    ],
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
)

celery_app.conf.beat_schedule = {
    "sync_history_data_every_2_hours": {
        "task": "app.celery.tasks.data.tasks.schedule_sync_history_data",
        "schedule": crontab(minute="*/40"),
        "kwargs": {"tag_title": "EnergyActiveForward30Min", "time_partition": "1month",}
    },
    "sync_pipe_absolute_pressure_per_day": {
        "task": "app.celery.tasks.data.tasks.schedule_sync_history_data",
        "schedule": crontab(hour=17, minute=0),
        "kwargs": {"tag_title": "AverageAbsolutePressurePerDay", "time_partition": "1month",}
    },
    "sync_pipe_temperature_per_day": {
        "task": "app.celery.tasks.data.tasks.schedule_sync_history_data",
        "schedule": crontab(hour=17, minute=0),
        "kwargs": {"tag_title": "AverageTemperaturePerDay", "time_partition": "1month",}
    },
    "sync_pipe_volume_per_day": {
        "task": "app.celery.tasks.data.tasks.schedule_sync_history_data",
        "schedule": crontab(hour=17, minute=0),
        "kwargs": {"tag_title": "VolumePerDayProfile", "time_partition": "1month",}
    },
    "sync_pipe_mass_per_day": {
        "task": "app.celery.tasks.data.tasks.schedule_sync_history_data",
        "schedule": crontab(hour=17, minute=0),
        "kwargs": {"tag_title": "MassPerDay", "time_partition": "1month",}
    },
    "sync_power_from_tags": {
        "task": "app.celery.tasks.data.tasks.schedule_sync_history_data",
        "schedule": crontab(minute="*/3"),
        "kwargs": {"tag_title": "ActivePowerSummary", "time_partition": "5m",}
    },
}