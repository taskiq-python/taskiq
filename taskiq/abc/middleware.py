from typing import TYPE_CHECKING, Any, Coroutine, Dict, Union

if TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker
    from taskiq.message import TaskiqMessage
    from taskiq.result import TaskiqResult


class TaskiqMiddleware:
    """Base class for middlewares."""

    def __init__(self) -> None:
        self.broker: "AsyncBroker" = None  # type: ignore

    def set_broker(self, broker: "AsyncBroker") -> None:
        """
        Sets broker to middleware.

        :param broker: broker to set.
        """
        self.broker = broker

    def pre_send(
        self,
        message: "TaskiqMessage",
        labels: Dict[str, Any],
    ) -> "Union[TaskiqMessage, Coroutine[Any, Any, TaskiqMessage]]":
        """
        Hook that executes before sending the task to worker.

        This is a client-side hook, that executes right before
        the message is sent to broker.

        :param message: message to send.
        :param labels: task's labels.
        :return: modified message.
        """
        return message

    def post_send(
        self,
        message: "TaskiqMessage",
        labels: Dict[str, Any],
    ) -> "Union[None, Coroutine[Any, Any, None]]":
        """
        This hook is executed right after the task is sent.

        This is a client-side hook. It executes right
        after the messages is kicked in broker.

        :param message: kicked message.
        :param labels: labels for a message.
        """

    def pre_execute(
        self,
        message: "TaskiqMessage",
        labels: Dict[str, Any],
    ) -> "Union[TaskiqMessage, Coroutine[Any, Any, TaskiqMessage]]":
        """
        This hook is called before executing task.

        This is a worker-side hook, wich means it
        executes in the worker process.

        :param message: incoming parsed taskiq message.
        :param labels: task's labels without user-supplied lables.
        :return: modified message.
        """
        return message

    def post_execute(
        self,
        result: "TaskiqResult[Any]",
        labels: Dict[str, Any],
    ) -> "Union[None, Coroutine[Any, Any, None]]":
        """
        This hook executes after task is complete.

        This is a worker-side hook. It's called
        in worker process.

        :param result: result of execution for current task.
        :param labels: task's labels. Without user-supplied labels.
        """
