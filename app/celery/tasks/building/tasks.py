import os

import pandas as pd
from loguru import logger

from app.celery.celery_app import celery_app
from app.celery.helpers import async_to_sync
from app.schemas.building_schema import BuildingWithPyramidTitle
from app.services.building_service import BuildingService
from app.utils.unit_of_work import SqlAlchemyUnitOfWork


@celery_app.task
@async_to_sync
async def upload_buildings():
    """
        Upload buildings
    """

    uow = SqlAlchemyUnitOfWork()

    building_service = BuildingService(uow)

    logger.info("Buildings are synchronize")
    dir_path = os.path.dirname(__file__)
    df = pd.read_excel(dir_path+"/files/buildings.xlsx")
    try:
        external_ids = [r["ID здания"] for i, r in df.iterrows()]
        existing_external_ids = await building_service.get_existing_external_ids(external_ids)
        elements_to_insert: list[BuildingWithPyramidTitle] = []
        element_to_update: list[BuildingWithPyramidTitle] = []
        for ind, row in df.iterrows():
            pyramid_title = row["Пирамида"]
            if pd.isnull(pyramid_title):
                pyramid_title = None
            building = BuildingWithPyramidTitle(
                title=row["Навание здания"],
                pyramid_title=pyramid_title,
                external_id=row["ID здания"],
            )
            if row["ID здания"] not in existing_external_ids:
                elements_to_insert.append(building)
            else:
                element_to_update.append(building)

        if elements_to_insert != []:
            await building_service.bulk_insert(elements_to_insert) 
        if element_to_update != []:
            await building_service.bulk_update(element_to_update) 
    except Exception as e:
        logger.exception(f"Error: {e}")
    msg = "Buildings were synchronized"
    logger.info(msg) 
    return msg

