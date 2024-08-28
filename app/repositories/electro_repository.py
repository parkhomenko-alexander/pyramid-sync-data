
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.electro import Electro
from app.repositories.abstract_repository import SQLAlchemyRepository


class ElectroRepository(SQLAlchemyRepository[Electro]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Electro)