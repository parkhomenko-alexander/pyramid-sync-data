from datetime import datetime, tzinfo
from typing import TypeVar
from typing_extensions import Annotated
from pytz import timezone
from sqlalchemy import DateTime, PrimaryKeyConstraint, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, registry, declarative_base

Base = declarative_base()

class IDMixin:
    id: Mapped[int] = mapped_column(primary_key=True)