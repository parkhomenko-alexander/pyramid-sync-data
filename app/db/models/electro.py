from datetime import datetime

from sqlalchemy.orm import Mapped

from app.db.base_model import Base, IDMixin


class Electro(Base, IDMixin):
    __tablename__ = "electro"

    electro_unit: Mapped[str]
    created_at: Mapped[datetime]

    plan: Mapped[float]
    fact: Mapped[float]

    plane_coast: Mapped[float]
    fact_coast: Mapped[float]

    delta_values: Mapped[float]
    delta_coast: Mapped[float]

