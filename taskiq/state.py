from collections import UserDict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    _Base = UserDict[str, Any]
else:
    _Base = UserDict


class TaskiqState(_Base):
    """
    State class.

    This class is used to store useful variables
    for later use.
    """

    def __init__(self) -> None:
        self.__dict__["data"] = {}

    def __getattr__(self, name: str) -> Any:
        try:
            return self.__dict__["data"][name]
        except KeyError as exc:
            cls_name = self.__class__.__name__
            raise AttributeError(
                f"'{cls_name}' object has no attribute '{name}'",
            ) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    def __delattr__(self, name: str) -> None:
        try:
            del self[name]
        except KeyError as exc:
            cls_name = self.__class__.__name__
            raise AttributeError(
                f"'{cls_name}' object has no attribute '{name}'",
            ) from exc

    def __str__(self) -> str:
        return "TaskiqState(%s)" % super().__str__()
