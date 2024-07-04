from pydantic import Field

from app.schemas.base_schema import GeneralSchema


class TagSchema(GeneralSchema):
    title: str = Field(validation_alias="title")
    description: str = Field(validation_alias="description")

    
class TagGET(GeneralSchema):
    id: int = Field(validation_alias="id")
    title: str = Field(validation_alias="title")
    description: str = Field(validation_alias="description")