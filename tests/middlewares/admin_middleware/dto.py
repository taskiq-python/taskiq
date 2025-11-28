from dataclasses import dataclass

import pydantic
from typing_extensions import TypedDict


@dataclass(frozen=True, slots=True)
class DataclassNestedDTO:
    id: int
    name: str


@dataclass(frozen=True, slots=True)
class DataclassDTO:
    nested: DataclassNestedDTO
    recipients: list[str]
    subject: str
    attachments: list[str] | None = None
    text: str | None = None
    html: str | None = None


class PydanticDTO(pydantic.BaseModel):
    number: int
    text: str
    flag: bool
    list: list[float]
    dictionary: dict[str, str] | None = None


class TypedDictDTO(TypedDict):
    id: int
    name: str
    active: bool
    scores: list[int]
    metadata: dict[str, str] | None
