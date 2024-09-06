from datetime import datetime

from pydantic import Field

from app.schemas.base_schema import GeneralSchema


class DataExternalIdMinix(GeneralSchema):
    created_at: datetime  = Field(validation_alias="created_at")
    tag_id: int = Field(validation_alias="tag_id")
    device_sync_id: int = Field(validation_alias="device_sync_id")

    def __eq__(self, other):
        if isinstance(other, DataExternalIdMinix):
            return (self.created_at == other.created_at and 
                    self.tag_id == other.tag_id and 
                    self.device_sync_id == other.device_sync_id)
        return False
    

class DataAddSheme(DataExternalIdMinix):
    value: float = Field(validation_alias="value")

    def __hash__(self):
        return hash((self.created_at, self.tag_id, self.device_sync_id))
    


