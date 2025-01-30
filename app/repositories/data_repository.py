
from typing import Sequence

from sqlalchemy import and_, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.data import Data
from app.repositories.abstract_repository import SQLAlchemyRepository
from app.schemas.data_schema import DataAddSheme


class DataRepository(SQLAlchemyRepository[Data]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Data)

    async def get_existing_values(self, values: set[DataAddSheme]) -> Sequence[Data]:
        records = [(value.created_at, value.tag_id, value.device_sync_id) for value in values]

        stmt = (
            select(self.model).
            where(
                tuple_(self.model.created_at, self.model.tag_id, self.model.device_sync_id).in_(records)
            )
        )

        res = await self.async_session.execute(stmt)

        return res.scalars().all()


    async def bulk_update_by_external_ids(self, data: list[dict]) -> int:
        for item in data:
            stmt = (
                update(self.model).
                where((self.model.created_at == item["created_at"]) & (self.model.tag_id == item["tag_id"]) & (self.model.device_sync_id == item["device_sync_id"])).
                values(**item)
            )
            
            await self.async_session.execute(stmt)
         
        return 0
    
    async def get_device_data(self, device_sync_id: int, time_range: list[str], tag_id: int):
        stmt = (
            select(
                self.model.value/1000000, self.model.created_at
            ).
            where(
                and_(
                    self.model.created_at.between(time_range[0], time_range[1]),
                    self.model.device_sync_id==device_sync_id,
                    self.model.tag_id == tag_id
                )
            )
        )

        res = await self.async_session.execute(stmt)

        return res 