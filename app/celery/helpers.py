import asyncio
from functools import wraps


def async_to_sync(task_func):
    @wraps(task_func)
    def wrapper(*args, **kwargs):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None

        if loop is None or not loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(task_func(*args, **kwargs))
        else:
            return asyncio.run(task_func(*args, **kwargs))

    return wrapper
