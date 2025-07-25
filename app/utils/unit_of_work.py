from abc import ABC, abstractmethod

from sqlalchemy.ext.asyncio import AsyncSession

from app.db import db
from app.repositories import (BuildingRepository, DataRepository,
                              DeviceRepository, TagRepository)
from app.repositories.electro_repository import ElectroRepository
from app.repositories.warm_repository import WarmRepository
from app.repositories.water_repository import WaterRepository


class AbstractUnitOfWork(ABC):
    building_repo: BuildingRepository
    device_repo: DeviceRepository
    tag_repo: TagRepository
    data_repo: DataRepository
    electro_repo: ElectroRepository
    water_repo: WaterRepository
    warm_repo: WarmRepository
   
    @abstractmethod
    def __init__(self, *args):
        raise NotImplementedError
    
    @abstractmethod
    async def __aenter__(self):
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(self, *args):
        await self.rollback()

    @abstractmethod
    async def commit(self):
        raise NotImplementedError

    @abstractmethod
    async def rollback(self):
        raise NotImplementedError


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    
    def __init__(self):
        self.async_session_factory = db.get_async_sessionmaker()

    async def __aenter__(self):
        self.async_session: AsyncSession = self.async_session_factory()
        
        self.building_repo = BuildingRepository(self.async_session)
        self.device_repo = DeviceRepository(self.async_session)
        self.tag_repo = TagRepository(self.async_session)
        self.data_repo = DataRepository(self.async_session)
        self.electro_repo = ElectroRepository(self.async_session)
        self.water_repo = WaterRepository(self.async_session)
        self.warm_repo = WarmRepository(self.async_session)
 
    async def __aexit__(self, *args):
        await self.rollback()
        await self.async_session.close()
        
    async def commit(self):
        await self.async_session.commit()

    async def rollback(self):
        await self.async_session.rollback()