from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base_model import Base, IDMixin


class Device(Base, IDMixin):
    __tablename__ = "devices"

    full_title: Mapped[str]
    guid: Mapped[str] = mapped_column(unique=True)
    sync_id: Mapped[int] = mapped_column(unique=True, nullable=False)
    serial_number: Mapped[str]
    
    building_external_id: Mapped[int] = mapped_column(ForeignKey("buildings.external_id"))

    __table_args__ = (
        UniqueConstraint('sync_id', name='uq_sync_id'),
    )