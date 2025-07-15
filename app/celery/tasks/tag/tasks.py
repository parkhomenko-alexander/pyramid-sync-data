import os

import pandas as pd
from loguru import logger

from app.celery.celery_app import celery_app
from app.celery.helpers import async_to_sync
from app.schemas.tag_schema import TagSchema
from app.services.tag_service import TagService
from app.utils.unit_of_work import SqlAlchemyUnitOfWork


@celery_app.task
@async_to_sync
async def upload_tags(): 
    """
        Upload Tags
    """

    uow = SqlAlchemyUnitOfWork()

    # pyramid_api: PyramidAPI = PyramidAPI()
    # pyramid_api.auth()

    tag_service = TagService(uow)

    logger.info("Tags are synchronize")
    dir_path = os.path.dirname(__file__)
    df = pd.read_excel(dir_path+"/files/tags.xlsx")
    try:
        external_ids = [r["title"] for i, r in df.iterrows()]
        existing_external_ids = await tag_service.get_existing_external_ids(external_ids)
        elements_to_insert: list[TagSchema] = []
        element_to_update: list[TagSchema] = []
        for ind, row in df.iterrows():
            title = row["title"]
            descr = row["description"]
            building = TagSchema(
                title=title,
                description=descr,
            )
            if title not in existing_external_ids:
                elements_to_insert.append(building)
            else:
                element_to_update.append(building)

        if elements_to_insert != []:
            await tag_service.bulk_insert(elements_to_insert) 
        if element_to_update != []:
            await tag_service.bulk_update(element_to_update) 
    except Exception as e:
        logger.exception(f"Error: {e}")
    msg = "Tags were synchronized"
    logger.info(msg) 
    return msg
