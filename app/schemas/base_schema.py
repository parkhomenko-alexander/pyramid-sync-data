from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class GeneralSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True, alias_generator=to_camel, extra="ignore")

    