from typing import Sequence
from loguru import logger

from app.schemas.data_schema import DataAddSheme, GetDataQueryParams
from app.services.helper import with_uow
from app.utils.unit_of_work import AbstractUnitOfWork


class DataService():
    def __init__(self, uow: AbstractUnitOfWork):
        self.uow: AbstractUnitOfWork = uow

    @with_uow
    async def bulk_insert(self, elements: set[DataAddSheme]):
        """
            Data inserting
        """
        elements_for_inserting = [e.model_dump() for e in elements]
        try:
            res = await self.uow.data_repo.bulk_insert(elements_for_inserting)
            if res == 1:
                logger.error(f"Data was not inserted")
                return 1
            await self.uow.commit()
        except Exception as e:
            logger.error(f"Some error occurred: {e}")
            return 1
        element  = next(iter(elements))
        logger.info(f"Data were inserted device_sync_id: {element.device_sync_id}, len: {len(elements)}")
        return 0
    
    @with_uow
    async def bulk_update(self, elements: set[DataAddSheme]):
        """
            Buildings update
        """
        elements_for_inserting = [e.model_dump() for e in elements]
        try:
            await self.uow.data_repo.bulk_update_by_external_ids(elements_for_inserting)
            await self.uow.commit()
        except Exception as e:
            logger.error(f"Some error occurred: {e}")
            return 1
        element  = next(iter(elements))
        logger.info(f"Data were updated device_sync_id: {element.device_sync_id}, len: {len(elements)}")
        return 0
    
    @with_uow
    async def get_existing_values(self, values: set[DataAddSheme]) -> set[DataAddSheme]:
        res = await self.uow.data_repo.get_existing_values(values)
        return {DataAddSheme.model_validate(value_orm) for value_orm in res}
    
    # @staticmethod
    # async def get_pyramid_title_external_id_mapping(uow: AbstractUnitOfWork) -> dict[str, int]:
        # async with uow:
        #     buildings = await uow.building_repo.get_all()
        #     buildings_title_id_mapped: dict[str, int] = {}
        #     for b in buildings:
        #         if b.pyramid_title is not None:
        #             buildings_title_id_mapped[b.pyramid_title] = b.external_id
        #     return buildings_title_id_mapped

    @with_uow
    async def get_data_excel(self, devices: list[int], time_range: list[str], tag_id: int):
        try:
            for device in devices:
                res = await self.uow.data_repo.get_device_data(device, time_range, tag_id)
                logger.info(res)
        except Exception as e:
            logger.exception("")

    @with_uow
    async def get_data_buildings(self, params: GetDataQueryParams) -> list[dict]:
        devices_building: Sequence[str] = await self.uow.device_repo.get_building_energy_devices()
        
        d = {}
        for device in devices_building:
            title = device.lower()
            if 'медицинкского' in title:
                alias = 'M'
            elif 'лабораторного' in title:
                alias = 'L'
            else:
                alias = device.strip().split()[-1]
            d[device] = alias

        res = await self.uow.data_repo.get_data_buildings(params.start, params.end, d)
        parsed_res = []
        for d in res:
            parsed_res.append(
                {
                    "1": d.get("1"),
                    "2": d.get("2"),
                    "3": d.get("3"),
                    "4": d.get("4"),
                    "5": d.get("5"),
                    "6": d.get("6"),
                    "7": d.get("7"),
                    "8": d.get("8"),
                    "9": d.get("9"),
                    "10": d.get("10"),
                    "11": d.get("11"),
                    "А": d.get("А"),   # кириллица
                    "ВDS": d.get("BDS"),  # смешанное
                    "C": d.get("С"),   # ⚡ проблема!
                    "E": d.get("Е"),   # ⚡ проблема!
                    "F": d.get("F"),
                    "G": d.get("G"),
                    "S1": d.get("S1"),
                    "S2": d.get("S2"),
                    "L": d.get("L"),
                    "M": d.get("M"),
                    "timestamp": d["timestamp"].strftime("%-m/%-d/%Y %H:%M")
                }
            )
        return parsed_res
    
    @with_uow
    async def get_data_cg(self, params: GetDataQueryParams) -> list[dict]:
        res = await self.uow.data_repo.get_data_cg(params.start, params.end)
        parsed_res = []
        for d in res:
            parsed_res.append(
                {
                    "Вентиляция": d.get("Вентиляция"),
                    "ИТП": d.get("ИТП"),
                    "Розетки": d.get("Розетки"),
                    "Освещение": d.get("Освещение"),
                    "Пожарка": d.get("Пожарка"),
                    "Фитнес": d.get("Фитнес"),
                    "МедОборудование": d.get("МедОборудование"),
                    "Операционная": d.get("Операционная"),
                    "timestamp": d["timestamp"].strftime("%-m/%-d/%Y %H:%M")
                }
            )
        return parsed_res