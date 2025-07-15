from sqlalchemy.orm import Mapped

from app.db.base_model import Base, IDMixin


class Tag(IDMixin, Base):
    __tablename__ = "tags"

    title: Mapped[str]
    description: Mapped[str]
