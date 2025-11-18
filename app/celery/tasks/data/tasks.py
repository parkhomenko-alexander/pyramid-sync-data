from datetime import date, datetime, timedelta
from logging import raiseExceptions
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
from app.services.load_data_service import LoadDataFromFilesService
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
                "медицинский центр": 128,
                "корпус bds": 121,
                "корпус а (цод)": 120
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
                
                ignore_units = ["мпуса"]

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
async def sync_pipe_entities():
    """
        Sync metrer points
    """

    buildings_map = {
        "Гостиничный корпус №1": 96,
        "Гостиничный корпус №3": 110,
        "Гостиничный корпус №4": 111,
        "Гостиничный корпус №5": 112,
        "Гостиничный корпус №6.2": 113,
        "Гостиничный корпус №7.2": 114,
        "Гостиничный корпус №8.1": 115,
        "Учебный корпус S1 (ФОК С)": 141,
        "Учебный корпус №12 (C,E)": 122,   # C=122, E=124
        "Учебный корпус №20 (B,D,S)": 121,  # B=121, D=123, S=140
        "Учебный корпус №22 (G)": 126
    }

    guid_to_building = {
        "4f1e67b7-22cc-4d00-ab5b-2fc2abed28fc": "Гостиничный корпус №1",
        "4dcc4229-e836-46f7-9909-69214e02ca15": "Гостиничный корпус №1",
        "2a726869-9888-4468-89b8-fc765a91979b": "Гостиничный корпус №3",
        "663ff011-c134-4261-bf41-d2af96941f2a": "Гостиничный корпус №3",
        "b3d19898-7c60-45fc-b45a-adf025856b9a": "Гостиничный корпус №4",
        "93952098-f83d-4f80-b531-104c5ee246de": "Гостиничный корпус №4",
        "d4beeeec-1970-4b3c-a973-02c1779c9967": "Гостиничный корпус №5",
        "685a27e2-8ac2-4b8f-b6ed-991b75d67fd5": "Гостиничный корпус №5",
        "9eb4f0ae-ae24-46ea-99fa-72b5cd6a1540": "Гостиничный корпус №6.2",
        "eac89a23-6f53-49d2-8c1c-927a089d8b3c": "Гостиничный корпус №6.2",
        "e80925cd-ad8a-43d8-9e1a-928d11e02b9f": "Гостиничный корпус №7.2",
        "e32b38ab-68ab-487f-96a6-20bf29375a4a": "Гостиничный корпус №7.2",
        "514897b3-0958-4735-ac0c-6f0b4f8a9a5f": "Гостиничный корпус №8.1",
        "5433d257-f922-41d2-9ce6-a6abd8f4f4be": "Гостиничный корпус №8.1",
        "9d6387be-3289-49c0-8d29-86d1bd692f8a": "Учебный корпус S1 (ФОК С)",
        "9d724a87-6514-4ea7-a996-e69b939aee5e": "Учебный корпус S1 (ФОК С)",
        "ff0a277b-4abe-4e35-a8c9-d858d755f702": "Учебный корпус №12 (C,E)",
        "10b229b7-1a40-45a7-847d-cfa7271d73ad": "Учебный корпус №12 (C,E)",
        "a9cc530a-81d8-46ab-b163-d602797da379": "Учебный корпус №20 (B,D,S)",
        "d6d168f8-9eb1-46bb-b31d-d54362c61297": "Учебный корпус №20 (B,D,S)",
        "b46c7c42-58d7-4560-b610-127455fa9dd3": "Учебный корпус №22 (G)",
        "29439625-0be6-4ac3-8f19-8ac1e30db2dd": "Учебный корпус №22 (G)",
    }


    uow = SqlAlchemyUnitOfWork()

    pyramid_api: PyramidAPI = PyramidAPI()
    pyramid_api.auth()
    
    device_service: DeviceService = DeviceService(uow)

    soap_request_entities_data: str = pyramid_api.generate_soap_request_data(SOAPActionsTypes.REQUEST_METERPOINTS_PIPES, source="P25")
    soap_raw_reponse = pyramid_api.soap_post("/SlaveEntities", soap_request_entities_data)
    if soap_raw_reponse and soap_raw_reponse.status_code != 500:
        request_id = pyramid_api.get_request_id_from_response(soap_raw_reponse)
    else:
        logger.error("Some error, request id not defined")
        return
    
    soap_request_entities_data: str = pyramid_api.generate_soap_request_data(SOAPActionsTypes.FETCH_METERPOINTS, source="P25", request_id=request_id)
    soap_raw_reponse = pyramid_api.soap_post("/SlaveEntities", soap_request_entities_data)
    

    tv7_info_response = pyramid_api.post(
        APIRoutes.GET_INSTANSES_INFO,
        pyramid_api.prepare_data_for_pipe_post_request(),
        # {"Content-Type": "application/json"}
    )
    if not tv7_info_response:
        logger.error("Some error, request id not defined")
        return 
    
    tv7_api_instanses: list = tv7_info_response.json()["data"]
    try:
        if soap_raw_reponse is None:
            raise ValueError("No response data")

        meter_points: list = pyramid_api.get_pipes_from_response(soap_raw_reponse)

        based_vrus: list[DeviceFromApi] = []
        for meter_point in meter_points:
            if not meter_point.contents:
                continue
            sync_id = meter_point.find("SyncId").text
            guid = meter_point.find("Id").text
            name = meter_point.find("Name").text

            serial_number = name.split(" ")[1]
            building_name = guid_to_building.get(guid)
            
            if building_name is None:
                logger.error(f"Some exception: not found building for meter: {guid}")
                raise ValueError("Not found building")
            
            building_external_id = buildings_map[building_name]

            based_vrus.append(DeviceFromApi(
                    full_title=name,
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
        return
    
    msg = "Devices were synchronized"
    logger.info(msg) 
    return msg



async def sync_history_data_with_filters(
        tag_title: str = "",
        time_range_raw: dict = {"start": "", "end": ""},
        time_partition: TimePartition = "30m",
        meter_points: Sequence[int] = []
    ):
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
        logger.info(time_pairs)
        if isinstance(time_pairs, int):
            logger.error(f"Errors with time pairs")
            return 2

        logging_step = 100 if len(devices) > 500 else 10
        for i, d in enumerate(devices):
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
                measured_values: list[bs4.Tag] = soup.find_all("MeasuredValue")  # type: ignore
                if measured_values == []:
                    continue
                seriallized_values: Set[DataAddSheme] = set()
                for measured_value in measured_values:
                    val = measured_value.find("Value")
                    created = measured_value.find("ValueDt")
                    if not (val and created):
                        logger.info("Empty data for insertion")
                        continue
                    dt = datetime.fromisoformat(created.text.strip())
                    dt = dt.replace(microsecond=0)

                    seriallized_values.add(DataAddSheme(
                        value=float(val.text),
                        # created_at=datetime.strptime(created.text, "%Y-%m-%dT%H:%M:%S"),
                        # created_at=datetime.strptime(created.text, "%Y-%m-%dT%H:%M:%S"),
                        created_at=dt,
                        tag_id=tag.id,
                        device_sync_id=d.sync_id
                    ))

                existing_values = await data_service.get_existing_values(seriallized_values)
                values_for_inserting: set[DataAddSheme] = seriallized_values - existing_values
                values_for_updating: set[DataAddSheme] =  seriallized_values - values_for_inserting
                if  i % logging_step == 0:
                    logger.info(f"insert_len = {len(values_for_inserting)}, update_len = {len(values_for_updating)}, ind = {i}")
                if values_for_inserting:
                    await data_service.bulk_insert(values_for_inserting)
                if values_for_updating:
                    await data_service.bulk_update(values_for_updating)
                else:
                    continue
    except Exception as e:
        logger.exception(f"Some error: {e}")
        return 1
    return 0

@celery_app.task
@async_to_sync
async def schedule_sync_history_data(tag_title: str = "", time_range: tuple[str | None, str| None] = (None, None), time_partition: TimePartition = "30m", meter_points: Sequence[int] = []):
    start_time, end_time = time_range
    now = datetime.now()
    logger.info(f"Sync data for tag: {tag_title}")
    
    if not (start_time and end_time):
        start_time = (date.today() - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")
        end_time = (date.today() + timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%S")
    
    if tag_title == "ActivePowerSummary":
        end_time = now.strftime("%Y-%m-%dT%H:%M:%S")
        start_time = (now - timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%S")

    uow = SqlAlchemyUnitOfWork()

    device_service = DeviceService(uow)

    if tag_title in ["AverageAbsolutePressurePerDay", "VolumePerDayProfile", "AverageTemperaturePerDay", "MassPerDayProfile"] \
        and meter_points == []:
        sync_ids = await device_service.get_devices_sync_ids_by_regexp("ТВ[-]?7")

        meter_points = sync_ids 

    await sync_history_data_with_filters(
        tag_title,
        {"start": start_time, "end": end_time},
        time_partition,
        meter_points
    )

@celery_app.task
@async_to_sync
async def load_electro(file_name: str):
    uow = SqlAlchemyUnitOfWork()
    electro_service: LoadDataFromFilesService = LoadDataFromFilesService(uow)

    await electro_service.insert_from_file(file_name)
 