from fastapi import APIRouter, FastAPI

from loguru import logger
from config import config
from app.api import router as root_router

def create_app(config=config):
    app = FastAPI(
        title="Pyramid app",
        root_path=config.APPLICATION_PREFIX_BEHIND_PROXY,
        docs_url="/docs",
        openapi_url="/openapi.json"
    )
    
    app.include_router(root_router)

    return app


app = create_app()