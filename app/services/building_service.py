from loguru import logger
from app.schemas.building_schema import BuildingPostSchema, BuildingWithPyramidTitle
from app.services.helper import with_uow
from app.utils.unit_of_work import AbstractUnitOfWork


class BuildingService():
    def __init__(self, uow: AbstractUnitOfWork):
        self.uow: AbstractUnitOfWork = uow

    @with_uow
    async def bulk_insert(self, elements: list[BuildingWithPyramidTitle]):
        """
            Buildings inserting
        """
        elements_for_inserting = [e.model_dump() for e in elements]
        try:
            await self.uow.building_repo.bulk_insert(elements_for_inserting)
            await self.uow.commit()
        except Exception as e:
            logger.error(f"Some error occurred: {e}")
            return 1
        
        logger.info(f"Buildings between {elements[0].external_id}-{elements[-1].external_id} were inserted")
        return 0
    
    @with_uow
    async def bulk_update(self, elements: list[BuildingWithPyramidTitle]):
        """
            Buildings update
        """
        elements_for_inserting = [e.model_dump() for e in elements]
        try:
            await self.uow.building_repo.bulk_update_by_external_ids(elements_for_inserting)
            await self.uow.commit()
        except Exception as e:
            logger.error(f"Some error occurred: {e}")
            return 1
        
        logger.info(f"Buildings between {elements[0].external_id}-{elements[-1].external_id} were updated")
        return 0
    
    @with_uow
    async def get_existing_external_ids(self, external_ids: list[int]) -> set[int]:
        return await self.uow.building_repo.get_existing_external_ids(external_ids)
    
    @staticmethod
    async def get_pyramid_title_external_id_mapping(uow: AbstractUnitOfWork) -> dict[str, int]:
        async with uow:
            buildings = await uow.building_repo.get_all()
            buildings_title_id_mapped: dict[str, int] = {}
            for b in buildings:
                if b.pyramid_title is not None:
                    buildings_title_id_mapped[b.pyramid_title] = b.external_id
            return buildings_title_id_mapped