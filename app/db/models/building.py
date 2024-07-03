from typing import TYPE_CHECKING
from sqlalchemy import ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, relationship, mapped_column
from app.db import Base
from app.db.base_model import IDMixin
from app.db.mixins.external_id_mixin import ExternalIdMixin

if TYPE_CHECKING:
    pass

class Building(ExternalIdMixin, IDMixin, Base):
    __tablename__ = "buildings"

    title: Mapped[str]
    pyramid_title: Mapped[str | None]