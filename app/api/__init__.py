from fastapi import APIRouter

from .data.router import router as data_router

router = APIRouter()
router.include_router(router=data_router, prefix='/data')