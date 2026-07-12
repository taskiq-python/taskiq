"""Internal validation for resources owned by broker lifecycle coordinators."""

from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker


def validate_unshared_broker_resources(
    brokers: Iterable["AsyncBroker"],
    *,
    error_type: type[ValueError] = ValueError,
) -> None:
    """Reject middleware and result backends owned by multiple brokers."""
    middleware_owners: dict[int, str] = {}
    backend_owners: dict[int, str] = {}
    for broker in brokers:
        for middleware in broker.middlewares:
            previous_owner = middleware_owners.setdefault(
                id(middleware),
                broker.broker_name,
            )
            if previous_owner != broker.broker_name:
                raise error_type(
                    "One middleware instance cannot be lifecycle-owned by "
                    f"brokers {previous_owner!r} and {broker.broker_name!r}.",
                )

        previous_owner = backend_owners.setdefault(
            id(broker.result_backend),
            broker.broker_name,
        )
        if previous_owner != broker.broker_name:
            raise error_type(
                "One result backend instance cannot be lifecycle-owned by "
                f"brokers {previous_owner!r} and {broker.broker_name!r}.",
            )
