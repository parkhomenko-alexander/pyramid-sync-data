from loguru import logger
from app.db.models.tag import Tag
from app.schemas.tag_schema import TagGET, TagSchema
from app.services.helper import with_uow
from app.utils.unit_of_work import AbstractUnitOfWork


class TagService():
    def __init__(self, uow: AbstractUnitOfWork):
        self.uow: AbstractUnitOfWork = uow

    @with_uow
    async def bulk_insert(self, elements: list[TagSchema]):
        """
            Tags inserting
        """
        elements_for_inserting = [e.model_dump() for e in elements]
        try:
            await self.uow.tag_repo.bulk_insert(elements_for_inserting)
            await self.uow.commit()
        except Exception as e:
            logger.error(f"Some error occurred: {e}")
            return 1
        
        logger.info(f"Tags between {0}-{len(elements) - 1} were inserted")
        return 0
    
    @with_uow
    async def bulk_update(self, elements: list[TagSchema]):
        """
            Tags update
        """
        elements_for_inserting = [e.model_dump() for e in elements]
        try:
            await self.uow.tag_repo.bulk_update_by_external_ids(elements_for_inserting)
            await self.uow.commit()
        except Exception as e:
            logger.error(f"Some error occurred: {e}")
            return 1
        
        logger.info(f"Tags between {0}-{len(elements) - 1} were updated")
        return 0
    
    @with_uow
    async def get_existing_external_ids(self, external_ids: list[str]) -> set[str]:
        return await self.uow.tag_repo.get_existing_external_ids(external_ids)

    @with_uow
    async def get_filtered(self, title) -> TagGET | None:
        tag: Tag | None = await self.uow.tag_repo.find_one(title=title)
        if not tag:
            return None
        return TagGET.model_validate(tag)