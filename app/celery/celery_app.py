from celery import Celery

from config import config

celery_app = Celery(
    __name__,
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND,
    include=[
        "tasks.building.tasks",
        "tasks.tag.tasks",
        "tasks.data.tasks",
    ],
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
)

celery_app.conf.beat_schedule = {
#    "sync_current_archive_issues": {
#         "task": "tasks.issues_tasks.orchestrator.sync_issues_current_archive_chord_job",
#         "schedule": crontab(minute="0", hour='*/3'),
#         "kwargs": {"delay": 3, "service_external_ids": [3, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19], "archive_borders": {"start": 1, "end": 30}},
#     },
}
