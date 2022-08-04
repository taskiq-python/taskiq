from typing import Any, Dict

from taskiq.abc.formatter import TaskiqFormatter
from taskiq.message import BrokerMessage, TaskiqMessage


class JSONFormatter(TaskiqFormatter):
    """Default taskiq formatter."""

    def dumps(self, message: TaskiqMessage, labels: Dict[str, Any]) -> BrokerMessage:
        """
        Dumps taskiq message to some broker message format.

        :param message: message to send.
        :param labels: message's labels.
        :return: Dumped message.
        """
        return BrokerMessage(
            task_id=message.task_id,
            task_name=message.task_name,
            message=message.json(),
            headers={
                "content_type": "application/json",
            },
        )

    def loads(self, message: BrokerMessage) -> TaskiqMessage:
        """
        Loads json from message.

        :param message: broker's message.
        :return: parsed taskiq message.
        """
        return TaskiqMessage.parse_raw(message.message)
