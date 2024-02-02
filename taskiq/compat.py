# flake8: noqa
from functools import lru_cache
from typing import Any, Dict, Optional, Type, TypeVar, Union

import pydantic
from importlib_metadata import version
from packaging.version import Version, parse

PYDANTIC_VER = parse(version("pydantic"))

Model = TypeVar("Model", bound="pydantic.BaseModel")
IS_PYDANTIC2 = PYDANTIC_VER >= Version("2.0")

if IS_PYDANTIC2:
    T = TypeVar("T")

    @lru_cache(maxsize=None)
    def create_type_adapter(annot: T) -> pydantic.TypeAdapter:
        return pydantic.TypeAdapter(annot)

    def parse_obj_as(annot: T, obj: Any) -> T:
        return create_type_adapter(annot).validate_python(obj)

    def model_validate(
        model_class: Type[Model],
        message: Dict[str, Any],
    ) -> Model:
        return model_class.model_validate(message)

    def model_dump(instance: Model) -> Dict[str, Any]:
        return instance.model_dump()

    def model_validate_json(
        model_class: Type[Model],
        message: Union[str, bytes, bytearray],
    ) -> Model:
        return model_class.model_validate_json(message)

    def model_dump_json(instance: Model) -> str:
        return instance.model_dump_json()

    def model_copy(
        instance: Model,
        update: Optional[Dict[str, Any]] = None,
        deep: bool = False,
    ) -> Model:
        return instance.model_copy(update=update, deep=deep)

    validate_call = pydantic.validate_call

else:
    parse_obj_as = pydantic.parse_obj_as  # type: ignore

    def model_validate(
        model_class: Type[Model],
        message: Dict[str, Any],
    ) -> Model:
        return model_class.parse_obj(message)

    def model_dump(instance: Model) -> Dict[str, Any]:
        return instance.dict()

    def model_validate_json(
        model_class: Type[Model],
        message: Union[str, bytes, bytearray],
    ) -> Model:
        return model_class.parse_raw(message)

    def model_dump_json(instance: Model) -> str:
        return instance.json()

    def model_copy(
        instance: Model,
        update: Optional[Dict[str, Any]] = None,
        deep: bool = False,
    ) -> Model:
        return instance.copy(update=update, deep=deep)

    validate_call = pydantic.validate_arguments  # type: ignore
