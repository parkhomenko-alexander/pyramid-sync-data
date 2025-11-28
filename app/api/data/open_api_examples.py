from fastapi.openapi.models import Example

cg_examples = {
    "simple_range": Example(
        summary="Простой запрос по двум группам",
        description="Данные по группам 1 и 2 за сутки",
        value={
            "groups": [1, 2],
            "start": "2025-01-01T00:00:00",
            "end": "2025-01-02T00:00:00",
        },
    ),
    "another_range": Example(
        summary="Другой набор групп",
        value={
            "groups": [1, 3],
            "start": "2025-01-01T00:00:00",
            "end": "2025-01-02T00:00:00",
        },
    ),
}