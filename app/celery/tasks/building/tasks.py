from dataclasses import dataclass
from datetime import datetime, timedelta
import os
from typing import Sequence, Set, TypedDict
import bs4
from loguru import logger
import numpy as np
from requests import Response
from app.celery.celery_app import celery_app
from app.celery.helpers import async_to_sync
from app.celery.pyramid_api import APIRoutes, PyramidAPI, SOAPActionsTypes, SOAPApiRoutes
from app.celery.types import TimePartition, TimeRangeForDataSync
from app.schemas.building_schema import BuildingWithPyramidTitle
from app.schemas.data_schema import DataAddSheme
from app.schemas.device_schema import DeviceFromApi, DeviceGET
from app.schemas.tag_schema import TagGET, TagSchema
from app.services.building_service import BuildingService
from app.services.data_service import DataService
from app.services.device_service import DeviceService
from app.services.tag_service import TagService
from app.utils.unit_of_work import SqlAlchemyUnitOfWork
import pandas as pd




@celery_app.task
@async_to_sync
async def upload_buildings():
    """
        Upload buildings
    """

    uow = SqlAlchemyUnitOfWork()

    building_service = BuildingService(uow)

    logger.info("Buildings are synchronize")
    dir_path = os.path.dirname(__file__)
    df = pd.read_excel(dir_path+"/files/buildings.xlsx")
    try:
        external_ids = [r["ID здания"] for i, r in df.iterrows()]
        existing_external_ids = await building_service.get_existing_external_ids(external_ids)
        elements_to_insert: list[BuildingWithPyramidTitle] = []
        element_to_update: list[BuildingWithPyramidTitle] = []
        for ind, row in df.iterrows():
            pyramid_title = row["Пирамида"]
            if pd.isnull(pyramid_title):
                pyramid_title = None
            building = BuildingWithPyramidTitle(
                title=row["Навание здания"],
                pyramid_title=pyramid_title,
                external_id=row["ID здания"],
            )
            if row["ID здания"] not in existing_external_ids:
                elements_to_insert.append(building)
            else:
                element_to_update.append(building)

        if elements_to_insert != []:
            await building_service.bulk_insert(elements_to_insert) 
        if element_to_update != []:
            await building_service.bulk_update(element_to_update) 
    except Exception as e:
        logger.exception(f"Error: {e}")
    msg = "Buildings were synchronized"
    logger.info(msg) 
    return msg

@celery_app.task
@async_to_sync
async def upload_tags():
    """
        Upload Tags
    """

    uow = SqlAlchemyUnitOfWork()

    # pyramid_api: PyramidAPI = PyramidAPI()
    # pyramid_api.auth()

    tag_service = TagService(uow)

    logger.info("Tags are synchronize")
    dir_path = os.path.dirname(__file__)
    df = pd.read_excel(dir_path+"/files/tags.xlsx")
    try:
        external_ids = [r["title"] for i, r in df.iterrows()]
        existing_external_ids = await tag_service.get_existing_external_ids(external_ids)
        elements_to_insert: list[TagSchema] = []
        element_to_update: list[TagSchema] = []
        for ind, row in df.iterrows():
            title = row["title"]
            descr = row["description"]
            building = TagSchema(
                title=title,
                description=descr,
            )
            if title not in existing_external_ids:
                elements_to_insert.append(building)
            else:
                element_to_update.append(building)

        if elements_to_insert != []:
            await tag_service.bulk_insert(elements_to_insert) 
        if element_to_update != []:
            await tag_service.bulk_update(element_to_update) 
    except Exception as e:
        logger.exception(f"Error: {e}")
    msg = "Tags were synchronized"
    logger.info(msg) 
    return msg


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


@celery_app.task
@async_to_sync
async def sync_history_data_with_filters(tag_title: str = "", time_range: TimeRangeForDataSync = TimeRangeForDataSync(start = "", end = ""), time_partition: TimePartition = "30m", meter_points: list[str] = []):
    uow = SqlAlchemyUnitOfWork()

    device_service = DeviceService(uow)
    tag_service = TagService(uow)
    data_service = DataService(uow)

    pyramid_api: PyramidAPI = PyramidAPI()

    try:
        if meter_points == []:
            devices: Sequence[DeviceGET] = await device_service.get_all()
        else:
            devices: Sequence[DeviceGET] = await device_service.get_diveces_by_serial_numbers(meter_points)

        tag: TagGET | None = await tag_service.get_filtered(title=tag_title)
        if tag is None:
            logger.error(f"Tag /{tag_title}/ not found")
            return 1
        
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
