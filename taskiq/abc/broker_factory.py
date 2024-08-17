from abc import ABC, abstractmethod

from taskiq.abc.broker import AsyncBroker


class BrokerFactory(ABC):
    """BrokerFactory class."""

    @abstractmethod
    def get_broker(self) -> AsyncBroker:
        """
        Get broker instance.

        :return: AsyncBroker instance
        """
