from typing import Mapping, Sequence

from sqlalchemy import VARCHAR, bindparam, select, text, update
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
    
    async def get_diveces_by_sync_ids(self, sr_numbers: Sequence[int] = []) -> Sequence[Device]:
        stmt = (
            select(self.model).
            where(self.model.sync_id.in_(sr_numbers))
        )
        
        res = await self.async_session.execute(stmt)

        return res.scalars().all()
    
    async def get_building_energy_devices(self) -> Sequence[str]:
        stmt = text(
            """
                SELECT
                    d.full_title
                FROM
                    devices d
                WHERE d.full_title SIMILAR TO 'Полная (нагрузка|мощность) %'
            """
        )

        res = await self.async_session.execute(stmt)
        
        return res.scalars().all()
    
    async def get_by_regexp(self, reg_exp: str) -> Sequence[int]:
        stmt = text(
            """
                SELECT 
                    d.sync_id
                FROM
                    devices d
                WHERE d.full_title ~* :reg_exp
            """
        ).bindparams(
            bindparam("reg_exp", value=reg_exp, type_=VARCHAR),
        )

        res = await self.async_session.execute(stmt)
        return res.scalars().all()