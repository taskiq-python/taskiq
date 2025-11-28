from collections.abc import Hashable
from functools import lru_cache
from importlib.metadata import version
from typing import Any, TypeVar

import pydantic
from packaging.version import Version, parse

PYDANTIC_VER = parse(version("pydantic"))

Model = TypeVar("Model", bound="pydantic.BaseModel")
IS_PYDANTIC2 = Version("2.0") <= PYDANTIC_VER

if IS_PYDANTIC2:
    T = TypeVar("T", bound=Hashable)

    @lru_cache
    def create_type_adapter(annot: type[T]) -> pydantic.TypeAdapter[T]:
        return pydantic.TypeAdapter(annot)

    def parse_obj_as(annot: type[T], obj: Any) -> T:
        return create_type_adapter(annot).validate_python(obj)

    def model_validate(
        model_class: type[Model],
        message: dict[str, Any],
    ) -> Model:
        return model_class.model_validate(message)

    def model_dump(instance: Model) -> dict[str, Any]:
        return instance.model_dump(mode="json")

    def model_validate_json(
        model_class: type[Model],
        message: str | bytes | bytearray,
    ) -> Model:
        return model_class.model_validate_json(message)

    def model_dump_json(instance: Model) -> str:
        return instance.model_dump_json()

    def model_copy(
        instance: Model,
        update: dict[str, Any] | None = None,
        deep: bool = False,
    ) -> Model:
        return instance.model_copy(update=update, deep=deep)

    validate_call = pydantic.validate_call

else:
    parse_obj_as = pydantic.parse_obj_as  # type: ignore

    def model_validate(
        model_class: type[Model],
        message: dict[str, Any],
    ) -> Model:
        return model_class.parse_obj(message)

    def model_dump(instance: Model) -> dict[str, Any]:
        return instance.dict()

    def model_validate_json(
        model_class: type[Model],
        message: str | bytes | bytearray,
    ) -> Model:
        return model_class.parse_raw(message)  # type: ignore[arg-type]

    def model_dump_json(instance: Model) -> str:
        return instance.json()

    def model_copy(
        instance: Model,
        update: dict[str, Any] | None = None,
        deep: bool = False,
    ) -> Model:
        return instance.copy(update=update, deep=deep)

    validate_call = pydantic.validate_arguments  # type: ignore
