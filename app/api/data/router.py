from datetime import datetime
from typing import Annotated, Literal
from fastapi import APIRouter, Depends, HTTPException, Query
from app.api.dependencies import get_data_service
from app.schemas.data_schema import GetDataQueryParams
from app.services.data_service import DataService
from config import config
from loguru import logger


router = APIRouter(
    tags=['Data']
)



@router.get(
    '',
    response_model=list[dict]
)
async def get_data_by_type(
    params: Annotated[GetDataQueryParams, Query()],
    service: DataService = Depends(get_data_service)
):
    if params.type == "b":
        return await service.get_data_buildings(params)
    else:
        return await service.get_data_cg(params)