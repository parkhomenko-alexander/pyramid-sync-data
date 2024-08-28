
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.electro import Electro
from app.db.models.warm import Warm
from app.repositories.abstract_repository import SQLAlchemyRepository


class WarmRepository(SQLAlchemyRepository[Warm]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Warm)