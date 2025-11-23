from typing import TYPE_CHECKING

from taskiq.abc.formatter import TaskiqFormatter
from taskiq.compat import model_dump, model_validate
from taskiq.message import BrokerMessage, TaskiqMessage

if TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker


class ProxyFormatter(TaskiqFormatter):
    """Default taskiq formatter."""

    def __init__(self, broker: "AsyncBroker") -> None:
        self.broker = broker

    def dumps(self, message: TaskiqMessage) -> BrokerMessage:
        """
        Dumps taskiq message to some broker message format.

        :param message: message to send.
        :return: Dumped message.
        """
        return BrokerMessage(
            task_id=message.task_id,
            task_name=message.task_name,
            queue=message.queue,
            message=self.broker.serializer.dumpb(model_dump(message)),
            labels=message.labels,
        )

    def loads(self, message: bytes) -> TaskiqMessage:
        """
        Loads json from message.

        :param message: broker's message.
        :return: parsed taskiq message.
        """
        return model_validate(TaskiqMessage, self.broker.serializer.loadb(message))
