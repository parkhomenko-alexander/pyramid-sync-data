
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.data import Data
from app.repositories.abstract_repository import SQLAlchemyRepository
from app.schemas.data_schema import DataAddSheme


class DataRepository(SQLAlchemyRepository[Data]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Data)

    async def get_existing_values(self, values: set[DataAddSheme]) -> Sequence[Data]:
        created_at = []
        tag_id = []
        device_sync_id = []

        for value in values:
            created_at.append(value.created_at)
            tag_id.append(value.tag_id)
            device_sync_id.append(value.device_sync_id)

        stmt = (
            select(self.model).
            where(
                self.model.created_at.in_(created_at) &
                self.model.tag_id.in_(tag_id) &
                self.model.device_sync_id.in_(device_sync_id)
            )
        )

        res = await self.async_session.execute(stmt)

        return res.scalars().all()
    

    # async def bulk_update_by_external_ids(self, data: list[dict]) -> int:
        # for item in data:
        #     stmt = (
        #         update(self.model).
        #         where(self.model.guid == item["guid"]).
        #         values(**item)
        #     )
        
        #     await self.async_session.execute(stmt)
         
        # return 0