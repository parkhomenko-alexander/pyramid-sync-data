from fastapi.openapi.models import Example

consumer_groups_data_request_examples = {
    "simple_example": Example(
        summary="Простой запрос",
        # description="Данные по группам 1 и 2 за сутки",
        value=[
            {
                "id": 1,
                "start": "2025-01-01T00:00:00",
                "end": "2025-02-02T00:00:00",
                "mode": "1d"
            },
            {
                "id": 4,
                "start": "2025-01-01T00:00:00",
                "end": "2025-04-02T00:00:00",
                "mode": "3d"
            },
            {
                "id": 3,
                "start": "2025-01-01T00:00:00",
                "end": "2025-01-02T00:00:00",
                "mode": "7d"
            },
        ],
    ),
}

consumer_groups_data_requestlist_id_examples = {
    "simple_example": Example(
        summary="Простой запрос",
        value=[
            {
                "id": [1],
                "start": "2025-01-01T00:00:00",
                "end": "2025-02-02T00:00:00",
            },
            {
                "id": [4, 3],
                "start": "2025-01-01T00:00:00",
                "end": "2025-04-02T00:00:00",
                "mode": "3d"
            }
        ]
    ),
}