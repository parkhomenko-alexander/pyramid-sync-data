from datetime import datetime
from typing import Annotated, Literal
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from app.api.dependencies import get_data_service
from app.schemas.data_schema import CGRequest, GetDataQueryParams
from app.services.data_service import DataService
from app.api.data.open_api_examples import cg_examples
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
    

@router.get(
    '/consumer_groups',
    response_model=dict
)
async def get_consumer_groups(
    ds: DataService = Depends(get_data_service)
):
    r = ds.make_consumer_groups_markers()
    return r


@router.get(
    '/consumer_groups/data',
    response_model=dict
)
async def get_consumer_groups_data(
    payload: Annotated[
        CGRequest,
        Query()
    ],
    ds: DataService = Depends(get_data_service),
):
    res = await ds.get_data_for_consumer_groups(payload.groups, payload.start, payload.end)

    return res
