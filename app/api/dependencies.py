from fastapi import Depends

from app.services.data_service import DataService
from app.utils.unit_of_work import AbstractUnitOfWork, SqlAlchemyUnitOfWork

def get_uow() -> AbstractUnitOfWork:
    return SqlAlchemyUnitOfWork()

def get_data_service(uow: AbstractUnitOfWork = Depends(get_uow)) -> DataService:
    return DataService(uow)
