from datetime import date, datetime, timedelta
from enum import Enum
from time import sleep
from typing import Any, Literal

import bs4
import urllib3
from dateutil.relativedelta import relativedelta
from loguru import logger
from requests import ConnectionError, Response, Session, Timeout

from app.celery.shared_types import TimePartition, TimeRangeForDataSync
from config import config


class APIRoutes:
    LOGIN = "/auth/login"
    GET_INSTANSE_INFO = "/rdinstance/getinstanceinfo/?instanceId="
    GET_INSTANSES_INFO = "/rdinstance/getinstances/"

SOAPApiRoutes = Literal["/SlaveEntities", "/SlaveArchives"]


class SOAPActionsTypes(Enum):
    REQUEST_METERPOINTS = "request_meterpoints"
    REQUEST_METERPOINTS_PIPES = "request_meterpoints_pipes"
    FETCH_METERPOINTS = "fetch_meterpoints"
    REQUEST_DATA_FOR_METER_POINT_WITH_TAG_AND_TIME = "request_data_with_filter"
    FETCH_DATA_FOR_METER_POINT_WITH_TAG_AND_TIME = "fetch_data_with_filter"

class PyramidAPI():

    def __init__(
            self,
            base_url: str = config.API_BASE_URL,
            base_soap_url: str = config.API_SOAP_BASE_URL,
            api_user: str = config.API_USER,
            api_pas: str = config.API_PAS,
            soap_user: str = config.API_SOAP_USER, 
            soap_pas: str = config.API_SOAP_PAS, 
            timeout: int = 10
        ):

        self.session: Session = Session()
        self.base_url = base_url
        self.base_soap_url = base_soap_url
        self.api_user = api_user
        self.api_pas = api_pas
        self.soap_user = soap_user
        self.soap_pas = soap_pas
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Authorization": "",
        }

       
        self.timeout = timeout
        urllib3.disable_warnings()
        
    def get(self, route: str, params: dict[str, Any] = {}) -> Response | None:
        flag = True
        response = None

        while flag:
            try:
                response = self.session.get(self.base_url + route, params=params, timeout=self.timeout, headers=self.headers, verify=False)
                st_code = response.status_code 
                
                if st_code == 401:
                    logger.error(f"Some error: status code is {st_code}, text: {response.text}")
                    continue
                elif st_code == 404:
                    logger.error(f"Some error: status code is {st_code}, text: {response.text}")
                    return None
                flag = False
            except Timeout as e:
                logger.exception("Time out error. Pyramid may be shut down", e)
                sleep(config.API_CALLS_DELAY_TIMEOUT_ERROR)
                logger.exception("Next try")
                return None
            except ConnectionError as e:
                logger.exception("Connecttion error. Pyuramid is shutdown or vpn enabled. Pyramid may be shut down", e)
                sleep(config.API_CALLS_DELAY_TIMEOUT_ERROR)
                logger.exception("Next try")
            except Exception as e:
                    logger.exception("Some error: ", e)
                    logger.exception(e)
                    sleep(config.API_CALLS_DELAY)
                    continue
        return response    

    def post(self, route: str, json: dict[str, Any] = {}, headers: dict[str, Any] = {}) -> Response | None:        
        flag = True
        response = None
        req_headers = self.headers | headers
        while flag:
            try:
                response = self.session.post(self.base_url + route, json=json, timeout=self.timeout, headers=req_headers, verify=False)
                st_code = response.status_code
                
                if st_code == 401:
                    logger.error(f"Some error: status code is {st_code}, text: {response.text}")
                    continue
                elif st_code == 404:
                    logger.error(f"Some error: status code is {st_code}, text: {response.text}")
                    return None
                flag = False
            except Timeout as e:
                logger.exception("Time out error. Pyramid may be shut down", e)
                sleep(config.API_CALLS_DELAY_TIMEOUT_ERROR)
                logger.exception("Next try")
                return None
            except ConnectionError as e:
                logger.exception("Connecttion error. Pyuramid is shutdown or vpn enabled. Pyramid may be shut down", e)
                sleep(config.API_CALLS_DELAY_TIMEOUT_ERROR)
                logger.exception("Next try")
            except Exception as e:
                    logger.exception("Some error: ", e)
                    logger.exception(e)
                    sleep(config.API_CALLS_DELAY)
                    continue
        return response
    
    def auth(self) -> int:
        flag = True
        while flag:
            try:
                auth_response = self.session.post(
                    url=self.base_url+APIRoutes.LOGIN,
                    headers=self.headers,
                    json={
                        "username": self.api_user,
                        "password": self.api_pas,
                        "tokens": None,
                    },
                    verify=False,
                    timeout=self.timeout
                )

                st_code = auth_response.status_code

                if st_code != 200:
                    logger.info(f"Status code of authorization is {st_code}")
                    return 1
                else:
                    flag = False
                    auth_response_json = auth_response.json()
                    self.headers.update({
                        "Authorization":f"Bearer {auth_response_json['tokens']['accessToken']}"
                    })

            except Exception as e:
                print("Some error: ", e)
                sleep(config.API_CALLS_DELAY)
                continue
        return 0

    def generate_soap_request_data(self, type: SOAPActionsTypes, **kwargs) -> str:
        source = kwargs["source"]
        body = ""
        if type == SOAPActionsTypes.REQUEST_METERPOINTS:
            filters = "\n".join([f"<{f}/>" for f in kwargs["filters"]])

            body = f"""
                <soapenv:Body>
                    <ns:RequestEntities>
                        <ns:message>
                            <ns:Header>
                                <ns:Source>{source}</ns:Source>
                            </ns:Header>
                            <ns:EntityTypeKind>MeterPoint</ns:EntityTypeKind>
                            <ns:Filters>
                                {filters}
                            </ns:Filters>
                        </ns:message>
                    </ns:RequestEntities>
                </soapenv:Body>
            """
        if type == SOAPActionsTypes.FETCH_METERPOINTS:
            request_id = kwargs["request_id"]

            body = f"""
                <soapenv:Body>
                    <ns:FetchEntities>
                        <ns:message>
                            <ns:Header>
                            <ns:Source>{source}</ns:Source>
                            </ns:Header>
                            <ns:RequestId>{request_id}</ns:RequestId>
                        </ns:message>
                    </ns:FetchEntities>
                </soapenv:Body>
            """
        if type == SOAPActionsTypes.REQUEST_DATA_FOR_METER_POINT_WITH_TAG_AND_TIME:
            time_range: TimeRangeForDataSync = kwargs["time_range"]
            tag_title = kwargs["tag_title"]
            meter_point_guid = kwargs["meter_point_guid"]

            body = f"""
                <soapenv:Body>
                    <ns:RequestValuesAndEvents>
                        <ns:message>
                            <ns:Header>
                                <ns:Source>{source}</ns:Source>
                            </ns:Header>
                            <ns:MeterPointIds>
                                <arr:string>{meter_point_guid}</arr:string>
                            </ns:MeterPointIds>
                            <ns:Parameters>
                                <ns:EndDate>{time_range.end}</ns:EndDate>
                                <ns:StartDate>{time_range.start}</ns:StartDate>
                                <ns:ValueTypeNames>
                                </ns:ValueTypeNames>
                                <ns:ValueTypes>
                                    <ns:MeasuredValueTypeEnum>{tag_title}</ns:MeasuredValueTypeEnum>
                                </ns:ValueTypes>
                            </ns:Parameters>
                        </ns:message>
                    </ns:RequestValuesAndEvents>
                </soapenv:Body>
            """

        if type == SOAPActionsTypes.REQUEST_METERPOINTS_PIPES:
            body = f"""
                <soapenv:Body>
                    <ns:RequestEntities>
                    <ns:message>
                        <ns:Header>
                            <ns:Source>{source}</ns:Source>
                        </ns:Header>
                        <ns:EntityTypeKind>MeterPoint</ns:EntityTypeKind>
                        <ns:Filters>
                            <ns:Filter xsi:type="ns:MeterPointFilter">
                                <ns:IncludePipes>true</ns:IncludePipes>
                                <ns:IncludeMeterPoints>false</ns:IncludeMeterPoints>
                                <ns:IncludePipeGroups>false</ns:IncludePipeGroups>
                            </ns:Filter>
                        </ns:Filters>
                    </ns:message>
                    </ns:RequestEntities>
                </soapenv:Body>
            """

        if type == SOAPActionsTypes.FETCH_DATA_FOR_METER_POINT_WITH_TAG_AND_TIME:
            request_id = kwargs["request_id"]

            body = f"""
                <soapenv:Body>
                    <ns:FetchValuesAndEvents>
                        <ns:message>
                            <ns:Header>
                            <ns:Source>{source}</ns:Source>
                            </ns:Header>
                            <ns:RequestId>{request_id}</ns:RequestId>
                        </ns:message>
                    </ns:FetchValuesAndEvents>
                </soapenv:Body>
            """
            

        main_part = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:ns="http://www.sicon.ru/Integration/Pyramid/2019/08"
                xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            >
            <soapenv:Header />
                {body}
            </soapenv:Envelope>
        """

        return main_part

    def soap_post(self, route: SOAPApiRoutes, data: str = "") -> Response | None:
        flag = True
        response = None
        while flag:
            try:
                response = self.session.post(
                    self.base_soap_url + route,
                    data=data,
                    headers={
                        "Content-Type": "text/xml",
                    },
                    auth=(self.soap_user, self.soap_pas),
                    timeout=self.timeout
                )
                st_code = response.status_code 
                
                if st_code == 404:
                    logger.error(f"Some error: status code is {st_code}, text: {response.text}")
                    return None
                flag = False
            except Timeout as e:
                logger.error("Time out error. Pyramid may be shut down", e)
                sleep(config.API_CALLS_DELAY_TIMEOUT_ERROR)
                logger.error("Next try")
                return None
            except ConnectionError as e:
                logger.error("Connecttion error. Pyramid is shutdown or vpn enabled. Pyramid may be shut down", e)
                sleep(config.API_CALLS_DELAY_TIMEOUT_ERROR)
                logger.error("Next try")
                return None
            except Exception as e:
                    logger.error("Some error: ", e)
                    sleep(config.API_CALLS_DELAY)
                    continue
        return response 

    def get_request_id_from_response(self, response: Response) -> str | None:
        soup = bs4.BeautifulSoup(response.text, "lxml-xml")
        req_id_block = soup.find("RequestId")
        if not req_id_block:
            return None
        return req_id_block.text
    
    def get_meter_points_from_response(self, response: Response) -> list:
        response_text = response.content.decode("utf-8")
        soup = bs4.BeautifulSoup(response_text, "lxml-xml")
        meter_points = soup.find_all("MeterPoint")

        return meter_points
    
    def prepare_data_for_pipe_post_request(self) -> dict:
        return {
            "classId": -30331,
            "userCustomized": False,
            "options": {
                "take": "50",
                "sort": "[{\"selector\":\"id\",\"desc\":false}]"
            }
        }

    def get_pipes_from_response(self, response: Response) -> list:
        response_text = response.content.decode("utf-8")
        soup = bs4.BeautifulSoup(response_text, "lxml-xml")
        meter_points = soup.find_all("RelationEntities", attrs={"i:type": "PipeRelationEntities"})

        return meter_points
    
    def prepare_time_range(self, time_range: TimeRangeForDataSync = TimeRangeForDataSync(start = "", end = ""), time_partition: TimePartition = "30m") -> list[TimeRangeForDataSync] | int:
        try:
            if time_range.start == "" or time_range.end == "":
                logger.error(f"Incorrect time borders must be str in format '%Year-%month-%day %H:%M:%S'")
                return 2

            start_date = datetime.strptime(time_range.start, "%Y-%m-%dT%H:%M:%S")
            end_date = datetime.strptime(time_range.end, "%Y-%m-%dT%H:%M:%S")

            if end_date < start_date:
                logger.error(f"End date should be more start date")
                return 2

            partition_delta: timedelta | relativedelta = timedelta()
            match time_partition:
                case "5m":
                    partition_delta = timedelta(minutes=5)
                case "30m":
                    partition_delta = timedelta(minutes=30)
                case "2h":
                    partition_delta = timedelta(hours=2)
                case "1day":
                    partition_delta = timedelta(days=1)
                case "1month":
                    return [TimeRangeForDataSync(start=start_date.isoformat(), end=end_date.isoformat())]

                case _:
                    raise ValueError(f"Unsupported time partition: {time_partition}")
                
            current_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            time_pairs: list[TimeRangeForDataSync] = []

            if isinstance(partition_delta, timedelta):
                current_start = start_date.replace(second=0, microsecond=0)
            else:
                current_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            while current_start < end_date:
                current_end = current_start + partition_delta - timedelta(seconds=1)
                if current_end > end_date:
                    current_end = end_date
                time_pairs.append(TimeRangeForDataSync(start=current_start.isoformat(), end=current_end.isoformat()))
                current_start = current_end + timedelta(seconds=1)
            
            if time_pairs[0]:
                last_time_pair = time_pairs[-1]
                last_end_date = datetime.strptime(last_time_pair.end, "%Y-%m-%dT%H:%M:%S") + timedelta(hours=3)
                time_pairs[-1].end = last_end_date.isoformat()
                
            return time_pairs
        except Exception as e:
            logger.exception(f"Some error: {e}")
            return 1