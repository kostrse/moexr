from typing import final


@final
class AutoPagination:
    """Sentinel indicating automatic pagination resolution from the endpoint registry."""

    def __repr__(self) -> str:
        return "auto"


auto = AutoPagination()
