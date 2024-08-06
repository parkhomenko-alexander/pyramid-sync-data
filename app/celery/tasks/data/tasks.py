from datetime import datetime, timedelta
from typing import Sequence, Set

import bs4
from loguru import logger
from requests import Response

from app.celery.celery_app import celery_app
from app.celery.helpers import async_to_sync
from app.celery.pyramid_api import APIRoutes, PyramidAPI, SOAPActionsTypes
from app.celery.shared_types import TimePartition, TimeRangeForDataSync
from app.schemas.data_schema import DataAddSheme
from app.schemas.device_schema import DeviceFromApi, DeviceGET
from app.schemas.tag_schema import TagGET
from app.services.building_service import BuildingService
from app.services.data_service import DataService
from app.services.device_service import DeviceService
from app.services.tag_service import TagService
from app.utils.unit_of_work import SqlAlchemyUnitOfWork
from config import config


@celery_app.task
@async_to_sync
async def sync_meter_points():
    """
        Sync metrer points
    """

    uow = SqlAlchemyUnitOfWork()

    pyramid_api: PyramidAPI = PyramidAPI()
    pyramid_api.auth()
    
    device_service: DeviceService = DeviceService(uow)

    soap_request_entities_data: str = pyramid_api.generate_soap_request_data(SOAPActionsTypes.REQUEST_METERPOINTS, source="P25", filters=["IncludeMeterPoints"])
    soap_raw_reponse = pyramid_api.soap_post("/SlaveEntities", soap_request_entities_data)
    if soap_raw_reponse is not None:
        request_id = pyramid_api.get_request_id_from_response(soap_raw_reponse)
    else:
        logger.error("Some error, request id not defined")
        return
    
    soap_request_entities_data: str = pyramid_api.generate_soap_request_data(SOAPActionsTypes.FETCH_METERPOINTS, source="P25", request_id=request_id)
    soap_raw_reponse = pyramid_api.soap_post("/SlaveEntities", soap_request_entities_data)
    try:
        if soap_raw_reponse is not None:
            building_pyra_title_ext_id = await BuildingService.get_pyramid_title_external_id_mapping(uow)
            building_pyra_title_ext_id.update({
                "медицинский центр": 128
            })
            meter_points: list = pyramid_api.get_meter_points_from_response(soap_raw_reponse)

            external_ids = [["ID здания"] for mp in meter_points]

            based_vrus: list[DeviceFromApi] = []
            for meter_point in meter_points:
                name_tag = meter_point.find("Name").text
                sync_id = meter_point.find("SyncId").text

                resp = pyramid_api.get(APIRoutes.GET_INSTANSE_INFO + sync_id)
                if resp is None:
                    logger.warning("Meter points response is none")
                    return
                
                response_meter_data = resp.json()

                instanse = response_meter_data["instance"]
                address = instanse.get("-13379", None)
                
                ignore_units = ["корпус bds", "мпуса", "корпус а (цод)"]

                if not address or "Приморский" not in address:
                    continue
                
                full_title = instanse.get("-2439", None)
                serial_number = full_title.split(", ")[-1]
                building_title = address.split(", ")[3][5:].lower()
                
                if building_title in ignore_units:
                    continue
                
                is_meter_point_havent_building = -100
                building_external_id = building_pyra_title_ext_id.get(building_title, is_meter_point_havent_building)

                if building_external_id == is_meter_point_havent_building: 
                    break
                guid = meter_point.find("Id").text
                based_vrus.append(DeviceFromApi(
                        full_title=name_tag,
                        guid=guid,
                        sync_id=sync_id,
                        serial_number=serial_number,
                        building_external_id=building_external_id,
                    )
                )
            
            external_ids = [vru.guid for vru in based_vrus]
            existing_external_ids = await device_service.get_existing_external_ids(external_ids)
            elements_for_insert: list[DeviceFromApi] = []
            element_for_update: list[DeviceFromApi] = []
            for vru in based_vrus:
                if vru.guid in existing_external_ids:
                    element_for_update.append(vru)
                else:
                    elements_for_insert.append(vru)

            if elements_for_insert != []:
                await device_service.bulk_insert(elements_for_insert) 
            if element_for_update != []:
                await device_service.bulk_update(element_for_update) 
    except Exception as e:
        logger.exception(f"Some exception: {e}")
    
    msg = "Devices were synchronized"
    logger.info(msg) 
    return msg


async def sync_history_data_with_filters(tag_title: str = "", time_range_raw: dict = {"start": "", "end": ""}, time_partition: TimePartition = "30m", meter_points: list[int] = []):
    uow = SqlAlchemyUnitOfWork()

    device_service = DeviceService(uow)
    tag_service = TagService(uow)
    data_service = DataService(uow)

    pyramid_api: PyramidAPI = PyramidAPI()

    try:
        if meter_points == []:
            devices: Sequence[DeviceGET] = await device_service.get_all()
        else:
            devices: Sequence[DeviceGET] = await device_service.get_diveces_by_sync_ids(meter_points)

        tag: TagGET | None = await tag_service.get_filtered(title=tag_title)
        if tag is None:
            logger.error(f"Tag /{tag_title}/ not found")
            return 1
        
        time_range: TimeRangeForDataSync = TimeRangeForDataSync(start=time_range_raw["start"], end=time_range_raw["end"])
        time_pairs = pyramid_api.prepare_time_range(time_range, time_partition)
        
        if isinstance(time_pairs, int):
            logger.error(f"Errors with time pairs")
            return 2

        for d in devices:
            logger.info(f"Starting data synchronization for {d.full_title.split()[0]} (guid: {d.guid})")
            for time_range in time_pairs:
                data = pyramid_api.generate_soap_request_data(
                    SOAPActionsTypes.REQUEST_DATA_FOR_METER_POINT_WITH_TAG_AND_TIME,
                    source="P25",
                    time_range=time_range,
                    tag_title=tag.title,
                    meter_point_guid=d.guid
                )

                soap_reponse_with_reqid: Response | None = pyramid_api.soap_post("/SlaveArchives", data)
                if not soap_reponse_with_reqid:
                    logger.error(f"Bad soap response")
                    return 3
                request_id_for_data: str | None  = pyramid_api.get_request_id_from_response(soap_reponse_with_reqid)
                if not request_id_for_data:
                    logger.error(f"Bad soap request id")
                    return 3
                
                data = pyramid_api.generate_soap_request_data(
                    SOAPActionsTypes.FETCH_DATA_FOR_METER_POINT_WITH_TAG_AND_TIME,
                    source="P25",
                    request_id=request_id_for_data,
                )
                soap_reponse_data_with_values: Response | None = pyramid_api.soap_post("/SlaveArchives", data)
                if not soap_reponse_data_with_values:
                    logger.error(f"Bad soap response on the data request")
                    return 3
                response_text = soap_reponse_data_with_values.content.decode("utf-8")
                soup = bs4.BeautifulSoup(response_text, "lxml-xml")
                measured_values: list[bs4.Tag] = soup.find_all("MeasuredValue")
                if measured_values == []:
                    continue
                seriallized_values: Set[DataAddSheme] = set()
                for measured_value in measured_values:
                    val = measured_value.find("Value")
                    created = measured_value.find("ValueDt")
                    if not (val and created):
                        logger.info("Empty data for insertion")
                        continue
                    seriallized_values.add(DataAddSheme(
                        value=float(val.text),
                        created_at=datetime.strptime(created.text, "%Y-%m-%dT%H:%M:%S"),
                        tag_id=tag.id,
                        device_sync_id=d.sync_id
                    ))

                existing_values = await data_service.get_existing_values(seriallized_values)
                values_for_inserting: set[DataAddSheme] = seriallized_values - existing_values
                if not values_for_inserting:
                    logger.info("Empty data for insertion")
                    continue

                await data_service.bulk_insert(values_for_inserting)
            logger.info(f"Data synchronization for device {d.full_title.split()[0]} (guid: {d.guid}) finished")
    except Exception as e:
        logger.exception(f"Some error: {e}")
        return 1
    return 0

@celery_app.task
@async_to_sync
async def schedule_sync_history_data(tag_title: str = "", time_range: tuple[str | None, str| None] = (None, None), time_partition: TimePartition = "30m", meter_points: list[int] = []):
    now = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    start_time, end_time = time_range
    if not (start_time and end_time):
        end_time = now.isoformat()
        start_time = (now - timedelta(hours=config.ENERGY_SCHEDULE_TIME_DELTA)).isoformat()
     
    await sync_history_data_with_filters(
        tag_title,
        {"start": start_time, "end": end_time},
        time_partition,
        meter_points
    )

