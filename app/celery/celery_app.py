from celery import Celery
from celery.schedules import crontab

from config import config

celery_app = Celery(
    __name__,
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND,
    include=[
        "tasks.building.tasks",
        "tasks.data.tasks",
        "tasks.tag.tasks",
    ],
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
)

celery_app.conf.beat_schedule = {
    "sync_history_data_every_2_hours": {
        "task": "schedule_sync_history_data",
        "schedule": crontab(minute="0", hour="*/2"),
        "kwargs": {"tag_title": "EnergyActiveForward30Min", "hours_delta": config.ENERGY_SCHEDULE_TIME_DELTA, "time_partition": "30m", "meter_points": []}
    }
}
