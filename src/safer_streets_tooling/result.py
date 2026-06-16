from abc import ABC, abstractmethod
from typing import cast, final


class Result[T](ABC):
    def __init__(self, *, value: T | None = None, error: Exception | None = None):
        self.value = value
        self.error = error

    @final
    def unwrap(self) -> T:
        if self.error is not None:
            raise self.error
        return cast(T, self.value)

    @final
    def is_err(self) -> bool:
        return self.error is not None

    @final
    def is_ok(self) -> bool:
        return self.error is None

    @abstractmethod
    def __repr__(self) -> str:
        pass

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Result):
            print("not a result")
            return False
        if self.error:
            # we don't know if/how __eq__ is implemented for Exception, so we compare reprs
            return repr(self.error) == repr(other.error)
        return self.value == other.value


class Ok[T](Result[T]):
    def __init__(self, value: T) -> None:
        super().__init__(value=value)

    def __repr__(self) -> str:
        return f"Ok({self.value})"


class Err[T](Result[T]):
    def __init__(self, error: Exception) -> None:
        super().__init__(error=error)

    def __repr__(self) -> str:
        return f"Err({self.error})"
