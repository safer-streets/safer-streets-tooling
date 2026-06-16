from abc import abstractmethod
from inspect import getfullargspec
from typing import final

from safer_streets_tooling.result import Err, Result


class AsyncNode[T_in, T_out]:
    """
    Functor interface for a node in the pipeline.
    """

    def __init__(self, *runtime_dependency_ids: str) -> None:
        method_spec = getfullargspec(self.execute)
        # check method signature
        if method_spec.varargs is not None:
            raise ValueError(
                f"{self.__class__.__name__}.execute method cannot have positional args. "
                "Only keyword arguments are allowed."
            )
        # check if method has runtime dependencies
        if method_spec.varkw is None and runtime_dependency_ids:
            raise ValueError(f"{self.__class__.__name__}.execute method with runtime dependencies must take **kwargs")
        self._dependency_ids = tuple(method_spec.kwonlyargs) + runtime_dependency_ids

    @abstractmethod
    async def execute(self, **kwargs: Result[T_in]) -> Result[T_out]:
        """Override this in subclasses. Only keyword arguments are allowed, and they should
        be the names of the dependencies of the node."""
        pass

    @final
    async def __call__(self, **kwargs: Result[T_in]) -> Result[T_out]:
        """Exception-safe wrapper around `execute`."""
        try:
            return await self.execute(**kwargs)
        except Exception as e:  # noqa: BLE001
            return Err[T_out](e)

    @final
    @property
    def dependency_ids(self) -> tuple[str, ...]:
        return self._dependency_ids
