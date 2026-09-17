from collections.abc import Hashable
from functools import lru_cache
from typing import Any, TypeVar

import pydantic

T = TypeVar("T", bound=Hashable)


@lru_cache
def create_type_adapter(annot: type[T]) -> pydantic.TypeAdapter[T]:
    return pydantic.TypeAdapter(annot)


def parse_obj_as(annot: type[T], obj: Any) -> T:
    return create_type_adapter(annot).validate_python(obj)
