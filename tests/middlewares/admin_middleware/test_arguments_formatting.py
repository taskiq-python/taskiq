import pytest
from polyfactory.factories import BaseFactory, DataclassFactory, TypedDictFactory
from polyfactory.factories.pydantic_factory import ModelFactory

from taskiq.brokers.inmemory_broker import InMemoryBroker
from tests.middlewares.admin_middleware.dto import (
    DataclassDTO,
    PydanticDTO,
    TypedDictDTO,
)


class DataclassDTOFactory(DataclassFactory[DataclassDTO]):
    __model__ = DataclassDTO


class TypedDictDTOFactory(TypedDictFactory[TypedDictDTO]):
    __model__ = TypedDictDTO


class PydanticDTOFactory(ModelFactory[PydanticDTO]):
    __model__ = PydanticDTO


class TestArgumentsFormattingInAdminMiddleware:
    @pytest.mark.parametrize(
        "dto_factory, task_name",
        [
            pytest.param(DataclassDTOFactory, "task_with_dataclass", id="dataclass"),
            pytest.param(TypedDictDTOFactory, "task_with_typed_dict", id="typeddict"),
            pytest.param(PydanticDTOFactory, "task_with_pydantic_model", id="pydantic"),
        ],
    )
    async def test_when_task_dto_passed__then_middleware_successfully_send_request(
        self,
        broker_with_admin_middleware: InMemoryBroker,
        dto_factory: type[BaseFactory],  # type: ignore[type-arg]
        task_name: str,
    ) -> None:
        # given
        task_arguments = dto_factory.build()
        task = broker_with_admin_middleware.find_task(task_name)
        assert task is not None, f"Task {task_name} should be registered in the broker"

        # when
        kicked_task = await task.kiq(task_arguments)
        await broker_with_admin_middleware.wait_all()

        # then
        result = await kicked_task.get_result()
        # we just expect no errors during post_send/pre_execute/post_execute
        assert result.error is None
