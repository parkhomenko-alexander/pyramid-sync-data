
from msilib import sequence
from typing import Sequence
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.device import Device
from app.repositories.abstract_repository import SQLAlchemyRepository


class DeviceRepository(SQLAlchemyRepository[Device]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Device)

    async def get_existing_external_ids(self, external_ids: list[str]) -> set[str]:
        stmt = (
            select(self.model.guid).
            where(self.model.guid.in_(external_ids))
        )

        res = await self.async_session.execute(stmt)

        return set(res.scalars().all())
    

    async def bulk_update_by_external_ids(self, data: list[dict]) -> int:
        for item in data:
            stmt = (
                update(self.model).
                where(self.model.guid == item["guid"]).
                values(**item)
            )
        
            await self.async_session.execute(stmt)
         
        return 0
    
    async def get_diveces_by_serial_numbers(self, sr_numbers: list[str] = []) -> Sequence[Device]:
        stmt = (
            select(self.model).
            where(self.model.serial_number.in_(sr_numbers))
        )
        
        res = await self.async_session.execute(stmt)

        return res.scalars().all()