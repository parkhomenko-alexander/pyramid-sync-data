from fastapi import APIRouter, FastAPI

from loguru import logger
from config import config
from app.api import router as root_router

def create_app(config=config):
    app = FastAPI(
        title="Pyramid app",
        # root_path=settings.APPLICATION_PREFIX_BEHIND_PROXY,
        # docs_url="/api/docs",
        # openapi_url="/api/openapi.json"
    )
    
    app.include_router(root_router)

    return app


app = create_app()