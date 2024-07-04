from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.building import Building
from app.repositories.abstract_repository import SQLAlchemyRepository


class BuildingRepository(SQLAlchemyRepository[Building]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Building)

    async def get_existing_external_ids(self, external_ids: list[int]) -> set[int]:
        stmt = (
            select(self.model.external_id).
            where(self.model.external_id.in_(external_ids))
        )

        res = await self.async_session.execute(stmt)

        return set(res.scalars().all())
    

    async def bulk_update_by_external_ids(self, data: list[dict]) -> int:
        for item in data:
            stmt = (
                update(self.model).
                where(self.model.external_id == item["external_id"]).
                values(**item)
            )
        
            await self.async_session.execute(stmt)
         
        return 0