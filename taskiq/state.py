from typing import Any


class TaskiqState(dict[str, Any]):
    """
    State class.

    This class is used to store useful variables
    for later use.
    """

    def __getattr__(self, name: str) -> Any:
        try:
            return super().__getitem__(name)  # noqa: WPS613
        except KeyError:
            cls_name = self.__class__.__name__
            raise AttributeError(f"'{cls_name}' object has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setitem__(name, value)  # noqa: WPS613

    def __delattr__(self, name: str) -> None:  # noqa: WPS603
        return super().__delitem__(name)  # noqa: WPS613
