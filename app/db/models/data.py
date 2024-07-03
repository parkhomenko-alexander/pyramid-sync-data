from typing import TYPE_CHECKING
from sqlalchemy import DateTime, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, relationship, mapped_column
from app.db import Base

if TYPE_CHECKING:
    # from app.db.models.issue import Issue
    pass

class Data(Base):
    __tablename__ = "data"

    value: Mapped[float]
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=False), primary_key=True)
    
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id"), primary_key=True)
    device_sync_id: Mapped[int] = mapped_column(ForeignKey("devices.sync_id"), primary_key=True)