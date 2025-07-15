from typing import Sequence

from loguru import logger

from app.db.models.device import Device
from app.schemas.device_schema import DeviceFromApi, DeviceGET
from app.services.helper import with_uow
from app.utils.unit_of_work import AbstractUnitOfWork


class DeviceService():
    def __init__(self, uow: AbstractUnitOfWork):
        self.uow: AbstractUnitOfWork = uow

    @with_uow
    async def bulk_insert(self, elements: list[DeviceFromApi]):
        """
            Devices inserting
        """
        elements_for_inserting = [e.model_dump() for e in elements]
        try:
            await self.uow.device_repo.bulk_insert(elements_for_inserting)
            await self.uow.commit()
        except Exception as e:
            logger.error(f"Some error occurred: {e}")
            return 1
        
        logger.info(f"Devices between {0}-{len(elements) - 1} were inserted")
        return 0
    
    @with_uow
    async def bulk_update(self, elements: list[DeviceFromApi]):
        """
            Devices update
        """
        elements_for_inserting = [e.model_dump() for e in elements]
        try:
            await self.uow.device_repo.bulk_update_by_external_ids(elements_for_inserting)
            await self.uow.commit()
        except Exception as e:
            logger.error(f"Some error occurred: {e}")
            return 1
        
        logger.info(f"Devices between {0}-{len(elements) - 1} were updated")
        return 0
    
    @with_uow
    async def get_existing_external_ids(self, external_ids: list[str]) -> set[str]:
        return await self.uow.device_repo.get_existing_external_ids(external_ids)
    
    @with_uow
    async def get_all(self) -> Sequence[DeviceGET]:
        devices = await self.uow.device_repo.get_all()
        return [DeviceGET.model_validate(d) for d in devices]
    
    @with_uow
    async def get_diveces_by_sync_ids(self, sr_numbers: list[int] = []) -> list[DeviceGET]:
        devices: Sequence[Device] = await self.uow.device_repo.get_diveces_by_sync_ids(sr_numbers)
        
        return [DeviceGET.model_validate(d) for d in devices]