# flake8: noqa
from typing import Any, TypeVar

import pydantic

pydantic_version = tuple([int(part) for part in pydantic.__version__.split(".")])


if pydantic_version >= (2, 0):
    T = TypeVar('T')

    def parse_obj_as(annot: T, obj: Any) -> T:
        return pydantic.TypeAdapter(annot).validate_python(obj)

    def model_validate_json(model_class, message):  # type: ignore
        return model_class.model_validate_json(message)

    def model_dump_json(instance):  # type: ignore
        return instance.model_dump_json()

    def model_copy(instance, *args, **kwargs):
        return instance.model_copy(*args, **kwargs)
else:
    parse_obj_as = pydantic.parse_obj_as  # type: ignore

    def model_validate_json(model_class, message):
        return model_class.parse_raw(message)

    def model_dump_json(instance):
        return instance.json()

    def model_copy(instance, *args, **kwargs):
        return instance.copy(*args, **kwargs)
