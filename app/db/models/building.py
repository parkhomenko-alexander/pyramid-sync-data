import datetime
from typing import TYPE_CHECKING

from sqlalchemy.orm import Mapped

from app.db.base_model import Base, IDMixin
from app.db.mixins.external_id_mixin import ExternalIdMixin

if TYPE_CHECKING:
    pass

class Building(ExternalIdMixin, IDMixin, Base):
    __tablename__ = "buildings"

    title: Mapped[str]
    pyramid_title: Mapped[str | None]

