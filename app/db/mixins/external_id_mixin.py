from sqlalchemy.orm import Mapped, mapped_column


class ExternalIdMixin:
    external_id: Mapped[int] = mapped_column(unique=True)