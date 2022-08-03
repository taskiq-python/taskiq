from typing import TYPE_CHECKING, Any, Callable, Coroutine, Dict, Generic, overload

from taskiq.kicker import AsyncKicker
from taskiq.task import AsyncTaskiqTask
from taskiq.types_helpers import T_, FuncParams_, ReturnType_

if TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker


class AsyncTaskiqDecoratedTask(Generic[FuncParams_, ReturnType_]):
    """
    Class for all task functions.

    When function is decorated
    with the `task` decorator, it
    will return an instance of this class.

    This class parametrized with original function's
    arguments types and a return type.

    This class has kiq method which is used
    to kick tasks out of this thread and send them to
    current broker.
    """

    def __init__(
        self,
        broker: "AsyncBroker",
        task_name: str,
        original_func: Callable[FuncParams_, ReturnType_],
        labels: Dict[str, Any],
    ) -> None:
        self.broker = broker
        self.task_name = task_name
        self.original_func = original_func
        self.labels = labels

    # Docs for this method are ommited in order to help
    # your IDE resolve correct docs for it.
    def __call__(  # noqa: D102
        self,
        *args: FuncParams_.args,
        **kwargs: FuncParams_.kwargs,
    ) -> ReturnType_:
        return self.original_func(*args, **kwargs)

    @overload
    async def kiq(  # noqa: D102
        self: "AsyncTaskiqDecoratedTask[FuncParams_, Coroutine[Any, Any, T_]]",
        *args: FuncParams_.args,
        **kwargs: FuncParams_.kwargs,
    ) -> AsyncTaskiqTask[T_]:
        ...

    @overload
    async def kiq(  # noqa: D102
        self: "AsyncTaskiqDecoratedTask[FuncParams_, ReturnType_]",
        *args: FuncParams_.args,
        **kwargs: FuncParams_.kwargs,
    ) -> AsyncTaskiqTask[ReturnType_]:
        ...

    async def kiq(
        self,
        *args: FuncParams_.args,
        **kwargs: FuncParams_.kwargs,
    ) -> Any:
        """
        This method sends function call over the network.

        It gets current broker and calls it's kick method,
        returning what it returns.

        :param args: function's arguments.
        :param kwargs: function's key word arguments.

        :returns: taskiq task.
        """
        return await self.kicker().kiq(*args, **kwargs)

    def kicker(self) -> AsyncKicker[FuncParams_, ReturnType_]:
        """
        This function returns kicker object.

        Kicker is a object that can modyfy kiq request
        before sendig it.

        :return: AsyncKicker instance.
        """
        return AsyncKicker(
            task_name=self.task_name,
            broker=self.broker,
            labels=self.labels,
        )

    def __repr__(self) -> str:
        return f"AsyncTaskiqDecoratedTask({self.task_name})"
