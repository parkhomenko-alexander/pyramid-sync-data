
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.water import Water
from app.repositories.abstract_repository import SQLAlchemyRepository


class WaterRepository(SQLAlchemyRepository[Water]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Water)