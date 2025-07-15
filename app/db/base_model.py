from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()

class IDMixin:
    id: Mapped[int] = mapped_column(primary_key=True)