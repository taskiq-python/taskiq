import logging
from typing import TypeVar

from typing_extensions import ParamSpec

logger = logging.getLogger("taskiq")

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")
