from collections import defaultdict
from datetime import datetime, timedelta
from typing import Mapping, Sequence
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

        res = await self.uow.data_repo.get_data_buildings(self.round_to_nearest_30(params.start), self.round_to_nearest_30(params.end), d)
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
        res = await self.uow.data_repo.get_data_cg(self.round_to_nearest_30(params.start), self.round_to_nearest_30(params.end))
        parsed_res = []
        for d in res:
            parsed_res.append(
                {
                    "Вентиляция": d.get("Вентиляция"),
                    "ИТП": d.get("ИТП"),
                    "Розетки": d.get("Розетки"),
                    "Освещение": d.get("Освещение"),
                    "Пожарка": d.get("Пожарка"),
                    "Столовая": d.get("Столовая"),
                    "Фитнес": d.get("Фитнес"),
                    "МедОборудование": d.get("МедОборудование"),
                    "Операционная": d.get("Операционная"),
                    "timestamp": d["timestamp"].strftime("%-m/%-d/%Y %H:%M")
                }
            )
        return parsed_res
    
    def round_to_nearest_30(self, dt: datetime) -> datetime:
        discard = timedelta(
            minutes=dt.minute % 30,
            seconds=dt.second,
            microseconds=dt.microsecond
        )
        return dt - discard

    def make_consumer_groups_markers(self) -> Sequence:
        return [
            {"title": "Корпус 1", "id": 1},
            {"title": "Корпус 2", "id": 2},
            {"title": "Корпус 3", "id": 3},
            {"title": "Корпус 4", "id": 4},
            {"title": "Корпус 5", "id": 5},
            {"title": "Корпус 6", "id": 6},
            {"title": "Корпус 7", "id": 7},
            {"title": "Корпус 8", "id": 8},
            {"title": "Корпус 9", "id": 9},
            {"title": "Корпус 10", "id": 10},
            {"title": "Корпус 11", "id": 11},
            {"title": "Корпус A", "id": 12},
            {"title": "Корпус BDS", "id": 13},
            {"title": "Корпус C", "id": 14},
            {"title": "Корпус E", "id": 15},
            {"title": "Корпус F", "id": 16},
            {"title": "Корпус G", "id": 17},
            {"title": "Корпус S1", "id": 18},
            {"title": "Корпус S2", "id": 19},
            {"title": "Корпус L", "id": 20},
            {"title": "Корпус M", "id": 21},

            {"title": "Корпус 1 - Вентиляция", "id": 22},
            {"title": "Корпус 1 - Итп", "id": 23},
            {"title": "Корпус 1 - Розетки", "id": 24},
            {"title": "Корпус 1 - Освещение", "id": 25},
            {"title": "Корпус 1 - ГПМ, АПС, Пож.вентиляция", "id": 26},

            {"title": "Корпус 2 - Вентиляция", "id": 27},
            {"title": "Корпус 2 - Итп", "id": 28},
            {"title": "Корпус 2 - Розетки", "id": 29},
            {"title": "Корпус 2 - Освещение", "id": 30},
            {"title": "Корпус 2 - ГПМ, АПС, Пож.вентиляция", "id": 31},
        ]
    
    def make_comsumer_group_sync_id(self,) -> Mapping:
        return {
            "Корпус 1": [53484],
            "Корпус 2": [53519],
            "Корпус 3": [53560],
            "Корпус 4": [53559],
            "Корпус 5": [53674],
            "Корпус 6": [53618],
            "Корпус 7": [53801],
            "Корпус 8": [53678],
            "Корпус 9": [53925],
            "Корпус 10": [53757],
            "Корпус 11": [53850],
            "Корпус A": [54270],
            "Корпус BDS": [54030],
            "Корпус C": [54297],
            "Корпус E": [54380],
            "Корпус F": [54511],
            "Корпус G": [54462],
            "Корпус S1": [54448],
            "Корпус S2": [54434],
            "Корпус L": [54149],
            "Корпус M": [53987],

            "Корпус M - Вентиляция": [
                51904,
                52066,
                52072,
                52084,
                52090,
                61329,
            ],
            "Корпус M - Итп": [51934, 51940],
            # "Корпус 1 - Розетки": [],
            # "Корпус 1 - Освещение": [],
            # "Корпус 1 - ГПМ, АПС, Пож.вентиляция": [],

            # "Корпус 2 - Вентиляция": [],
            # "Корпус 2 - Итп": [],
            # "Корпус 2 - Розетки": [],
            # "Корпус 2 - Освещение": [],
            # "Корпус 2 - ГПМ, АПС, Пож.вентиляция": [],
        }
    
    async def get_data_for_consumer_groups(self, markers: list[int] | None, start: datetime, end: datetime) -> Mapping:
        if not(markers and len(markers) > 0):
            raise ValueError("Bad request")
        
        marker_group = {
            e["id"]: e["title"]
            for e in self.make_consumer_groups_markers()
        }
        group_sync_ids = self.make_comsumer_group_sync_id()
        
        selected_groups: set[str] = {
            marker_group[m] for m in markers if m in marker_group
        }

        groups = []
        for group_name in selected_groups:
            for sync_id in group_sync_ids.get(group_name, []):
                groups.append((sync_id, group_name))


        result = {}
        async with self.uow:
            data_repo = self.uow.data_repo
            rows = await data_repo.get_data_for_specific_devices(start, end, groups, tag="EnergyActiveForward30Min")

            for r in rows:
                grp = r["group_name"]
                result.setdefault(grp, []).append({
                    "created": r["created_at"],
                    "value": float(r["value"]),
                })
        return result
    
