class DomainError(Exception):
    def __init__(self, message: str, *, code: str = 'domain_error') -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class ValidationException(DomainError):  # noqa: N818
    def __init__(self, message: str) -> None:
        super().__init__(message, code='validation_error')


class DependencyOfflineException(DomainError):  # noqa: N818
    def __init__(self, message: str) -> None:
        super().__init__(message, code='dependency_offline')
