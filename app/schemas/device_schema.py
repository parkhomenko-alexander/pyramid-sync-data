from pydantic import Field

from app.schemas.base_schema import GeneralSchema


class DeviceFromApi(GeneralSchema):
    full_title: str = Field(validation_alias="full_title")
    guid: str  = Field(validation_alias="guid")
    sync_id: int = Field(validation_alias="sync_id")
    serial_number: str = Field(validation_alias="serial_number")
    building_external_id: int = Field(validation_alias="building_external_id")


class DeviceGET(GeneralSchema):
    id: int = Field(validation_alias="id")
    full_title: str = Field(validation_alias="full_title")
    guid: str  = Field(validation_alias="guid")
    sync_id: int = Field(validation_alias="sync_id")
    serial_number: str = Field(validation_alias="serial_number")
    building_external_id: int = Field(validation_alias="building_external_id")