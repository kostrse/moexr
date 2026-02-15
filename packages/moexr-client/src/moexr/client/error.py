class MoexClientError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class PaginationError(MoexClientError):
    pass
