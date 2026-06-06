from collections.abc import Iterable, Mapping
from dataclasses import asdict, is_dataclass
from typing import Any

from pydantic import BaseModel

from taskiq.compat import model_dump
from taskiq.labels import parse_label, prepare_label


class TaskiqMessage(BaseModel):
    """
    Message abstractions.

    This an internal class used
    by brokers. Every remote call
    receive such messages.
    """

    task_id: str
    task_name: str
    labels: dict[str, Any]
    labels_types: dict[str, int] | None = None
    args: list[Any]
    kwargs: dict[str, Any]

    def parse_labels(self) -> None:
        """
        Parse labels.

        :return: None
        """
        if self.labels_types is None:
            return

        for label, label_type in self.labels_types.items():
            if label in self.labels:
                self.labels[label] = parse_label(self.labels[label], label_type)


def _prepare_message_arg(arg: Any) -> Any:
    """
    Prepare invocation argument for message payloads.

    :param arg: argument to prepare.
    :return: serializable argument representation.
    """
    if isinstance(arg, BaseModel):
        arg = model_dump(arg)
    if is_dataclass(arg):
        if isinstance(arg, type):
            raise ValueError(
                f"Cannot serialize types. The {arg} is not serializable.",
            )
        arg = asdict(arg)
    return arg


def _build_taskiq_message(
    task_id: str,
    task_name: str,
    labels: Mapping[str, Any],
    args: Iterable[Any],
    kwargs: Mapping[str, Any],
) -> TaskiqMessage:
    """
    Build a taskiq message using the common invocation contract.

    :param task_id: task id.
    :param task_name: task name.
    :param labels: task invocation labels.
    :param args: task positional arguments.
    :param kwargs: task keyword arguments.
    :return: prepared taskiq message.
    """
    prepared_labels: dict[str, Any] = {}
    labels_types: dict[str, int] = {}
    for label_name, label_value in labels.items():
        prepared_labels[label_name], labels_types[label_name] = prepare_label(
            label_value,
        )

    return TaskiqMessage(
        task_id=task_id,
        task_name=task_name,
        labels=prepared_labels,
        labels_types=labels_types,
        args=[_prepare_message_arg(arg) for arg in args],
        kwargs={
            kwarg_name: _prepare_message_arg(kwarg_value)
            for kwarg_name, kwarg_value in kwargs.items()
        },
    )


class BrokerMessage(BaseModel):
    """Format of messages for brokers."""

    task_id: str
    task_name: str
    message: bytes
    labels: dict[str, Any]
