from abc import ABC, abstractmethod
from typing import Generic, Sequence, Type, TypeVar

from loguru import logger
from sqlalchemy import insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base_model import Base

T = TypeVar('T', bound=Base)


class AbstractRepository(ABC, Generic[T]):

    @abstractmethod
    async def add(self, data: dict) -> int:
        raise NotImplementedError
    
    @abstractmethod
    async def get_all(self) -> Sequence[T]:
        raise NotImplementedError
    
    @abstractmethod
    async def get_all_fitered_or(self, filter_by: list) -> Sequence[T]:
        raise NotImplementedError
    
    @abstractmethod
    async def find_one(self, **filter_by) -> T | None:
        raise NotImplementedError

    @abstractmethod
    async def edit_one(self, id: int, data: dict) -> int:
        raise NotImplementedError
    
    @abstractmethod
    async def bulk_insert(self, data: list[dict]) -> int:
        raise NotImplementedError
    
    # @abstractmethod
    # async def get_count(self) -> int:
    #     raise NotImplementedError
    

class SQLAlchemyRepository(AbstractRepository[T]):
    
    def __init__(self, async_session: AsyncSession, model: Type[T]):
        self.async_session = async_session
        self.model = model

    async def add(self, data: dict):
        stmt = insert(self.model).values(**data).returning(self.model.id)
        res = await self.async_session.execute(stmt)
        
        return res.scalar_one()

    async def get_all(self):
        stmt = select(self.model)
        res = await self.async_session.scalars(stmt)

        return res.all()
    
    async def get_all_fitered_or(self, filter_by: list):
        stmt = select(self.model).filter(
            or_(
                *filter_by
            )
        )

        res = await self.async_session.scalars(stmt)

        return res.all()

    async def find_one(self, **filter_by):
        stmt = select(self.model).filter_by(**filter_by)
        res = await self.async_session.execute(stmt)

        return res.scalar_one_or_none()

    async def edit_one(self, id: int, data: dict):
        stmt = update(self.model).values(**data).filter_by(id=id).returning(self.model.id)
        res = await self.async_session.execute(stmt)
        return res.scalar_one()

    async def bulk_insert(self, data: list[dict]) -> int:
        try:
            stmt = (
                insert(self.model),
                data
            )
            
            res = await self.async_session.execute(*stmt)
            return 0
        except Exception as e:
            logger.error(f"Some error: {e}")
            return 1

