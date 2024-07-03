from sqlalchemy import ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, relationship, mapped_column
from app.db import Base
from app.db.base_model import IDMixin


class Tag(IDMixin, Base):
    __tablename__ = "tags"

    title: Mapped[str]
    description: Mapped[str]
