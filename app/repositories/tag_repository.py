from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.tag import Tag
from app.repositories.abstract_repository import SQLAlchemyRepository


class TagRepository(SQLAlchemyRepository[Tag]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Tag)

    async def get_existing_external_ids(self, external_ids: list[str]) -> set[str]:
        stmt = (
            select(self.model.title).
            where(self.model.title.in_(external_ids))
        )

        res = await self.async_session.execute(stmt)

        return set(res.scalars().all())
    

    async def bulk_update_by_external_ids(self, data: list[dict]) -> int:
        for item in data:
            stmt = (
                update(self.model).
                where(self.model.title == item["title"]).
                values(**item)
            )
        
            await self.async_session.execute(stmt)
         
        return 0