
from pydantic import Field

from app.schemas.base_schema import GeneralSchema


class BuildingPostSchema(GeneralSchema):
    title: str
    external_id: int

class BuildingWithPyramidTitle(GeneralSchema):
    title: str
    pyramid_title: str | None = Field(validation_alias="pyramid_title")
    external_id: int = Field(validation_alias="external_id")