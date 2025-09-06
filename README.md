uv run celery -A app.celery.celery_app:celery_app beat --loglevel=info 
uv run celery -A app.celery.celery_app:celery_app worker --loglevel=info --pool=solo

uv run celery -A app.celery.celery_app call app.celery.tasks.tag.tasks.upload_tags 
celery -A celery_app call tasks.data.tasks.sync_history_data_with_filters
celery -A celery_app call tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "EnergyActiveForward30Min","hours_delta": 2, "time_partition": " 30m", "meter_points": []}'
celery -A celery_app call tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "EnergyActiveForward30Min", "days_delta": 10, "hours_delta": 0, "time_partition": "1month", "meter_points": [3569]}'
celery -A celery_app call tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "EnergyActiveForward30Min", "days_delta": 760, "hours_delta": 0, "time_partition": "1month", "meter_points": []}'
celery -A celery_app call tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "EnergyActiveForward30Min", "time_range": ["2024-10-24T00:00:00", "2024-10-30T00:08:00"], "time_partition": "1month",}'
celery -A celery_app call tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "EnergyActiveForward30Min", "time_range": ["2024-08-01T00:00:00", "2024-08-30T00:08:00"], "time_partition": "1month",}'
celery -A celery_app call tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "EnergyActiveForward30Min", "time_range": ["2024-09-25T00:00:00", "2024-09-30T23:08:00"], "time_partition": "1month"}'
celery -A celery_app call tasks.data.tasks.sync_meter_points
celery -A celery_app call tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "EnergyActiveForward30Min", "time_range": ["2022-08-01T00:00:00", "2024-10-30T00:08:00"], "time_partition": "1month", "meter_points": [9887, 9893, 9899, 9905, 9911, 9917, 9923, 9929, 9935, 9941, 9947, 9953, 9959, 9965, 9971, 9977, 9983, 9989, 9995, 10001, 10007, 10013, 10019, 10025, 10031, 10037, 10043, 10049, 10055, 10061, 10067, 10073, 10079, 10085, 10091, 10097, 10103, 10109, 10115, 10121, 10127, 50191, 50192, 50193, 50194, 50195, 50197, 50198, 50199, 50200, 50212, 54030, 54097, 54105, 54119, 54124, 54133, 54144, 54183, 54213, 54221, 54229, 54237, 54244, 54250, 54254]}'
celery -A celery_app call tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "EnergyActiveForward30Min", "time_range": ["2024-09-01T00:00:00", "2024-09-10T20:08:00"], "time_partition": "1month", "meter_points": [54270]}'

uv run celery -A app.celery.celery_app call app.celery.tasks.data.tasks.schedule_sync_history_data --kwargs='{"tag_title": "VolumeMaxPerDay", "time_range": ["2025-06-01T00:00:00", "2025-09-30T20:08:00"], "time_partition": "1month"}'


celery -A celery_app call tasks.data.tasks.load_electro --kwargs='{"file_name": "warm"}'
celery -A celery_app call tasks.data.tasks.load_electro --kwargs='{"file_name": "electro_30_04_25"}'
