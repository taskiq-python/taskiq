from typing import Awaitable, Callable, Union

from pydantic import BaseModel


class AckableMessage(BaseModel):
    """
    Message that can be acknowledged.

    If your broker support message acknowledgement,
    please return this type of message, so we'll be
    able to mark this message as acknowledged after
    the function will be executed.

    It adds more reliability to brokers and system
    as a whole.
    """

    data: bytes
    ack: Callable[[], Union[None, Awaitable[None]]]
