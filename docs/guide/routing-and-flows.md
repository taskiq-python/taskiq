---
title: Routing and flows
order: 10
---

# Routing and flows

::: warning Experimental branch
This page describes the routing and flow contract on the
`experiment/separate_broker` branch. The old task declaration API remains
compatible. Broker-specific packages may keep using their existing send/listen
implementation until they opt into flow-aware dispatch and listen-plan support.
:::

Taskiq keeps the old task declaration API:

```python
@broker.task(task_name="billing.charge")
async def charge(user_id: int) -> None:
    ...
```

The broker is still the transport adapter, but routing decisions can be moved to
a `TaskiqRouter`. This is useful when one application owns several brokers or
when a task package declares tasks before the final application knows which
broker must handle them.

The important responsibility split is:

- Task declaration describes a Python callable, task name, labels and direct
  call behavior.
- Kicker prepares a task invocation: task id, labels, args, kwargs and optional
  route overrides.
- Router owns outbound routing policy: task invocation to broker and flow.
- Router also owns inbound listen-plan records for flow-aware brokers.
- Broker owns transport lifecycle: startup, shutdown, send, listen, ack/nack,
  serializer/formatter, middleware and result backend integration.
- Worker still executes by `task_name`; flow does not select the Python task.

## Router

A router owns broker registration, task registration and routing rules.
One router can own several brokers. A broker has a single router owner; pass the
router when creating the broker instead of registering the same broker in
several routers.

```python
from taskiq import Flow, InMemoryBroker, TaskiqRouter

router = TaskiqRouter()

default_broker = InMemoryBroker(router=router, broker_name="default")
priority_broker = InMemoryBroker(router=router, broker_name="priority")

priority_route = router.route_task(
    "billing.charge",
    broker=priority_broker,
    flow=Flow("billing.priority"),
)
```

If a broker is created without an explicit router, Taskiq creates a default
router for that broker. Use an explicit shared router when one worker or app
needs several brokers to follow the same routing policy.

Pass broker objects to routing APIs. If broker names come from configuration,
resolve them explicitly first:

```python
broker = router.get_broker("priority")
route = router.route_task("billing.charge", broker=broker)
```

This keeps routing errors local and avoids hidden string references.

## Flow

`Flow` is a transport-neutral delivery address. It is intentionally small.
The routing identity of a flow is its logical name:

```python
from taskiq import Flow, FlowIdentity, get_flow_identity

flow = Flow("billing.priority").with_options(priority=10)

assert flow.identity == FlowIdentity("billing.priority")
assert get_flow_identity(flow) == flow.identity
```

Broker packages can expose their own flow objects when they need
transport-specific options:

```python
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RabbitQueue:
    name: str
    durable: bool = True

    def broker_options(self) -> Mapping[str, object]:
        return {"durable": self.durable}
```

The core protocol requires `name` and `broker_options()`. Broker-specific
settings stay in the flow object and are returned through `broker_options()`;
they are not encoded into router task names or message labels.

Flow identity and declaration options are separate:

- Same broker, same flow name and same `broker_options()` means the listen plan
  can deduplicate the flow and merge task names.
- Same broker and same flow name with different `broker_options()` is a
  conflict, so Taskiq raises `ValueError` instead of silently choosing one
  queue/topic declaration.
- Different brokers may use the same flow name independently.

## Sending with a route

`router.route_task()` returns a `TaskiqRoute`. You can pass that object to a
kicker when you want the route to be explicit at the call site.

```python
route = router.resolve_route("billing.charge")
task = await charge.kicker().with_route(route).kiq(42)
```

For one-off overrides, resolve a route with the target broker and flow:

```python
bulk_route = router.resolve_route(
    "billing.charge",
    broker=default_broker,
    flow=Flow("billing.bulk"),
)

task = await charge.kicker().with_route(bulk_route).kiq(42)
```

`with_broker(...)` remains available for shared-task style invocation. When a
kicker has both a route and a later broker override, the broker override clears
the explicit route. Taskiq then resolves the task for the override broker: it
uses that broker's task route flow when one exists, otherwise the broker
`default_flow`.

`prepare(...)` creates a prepared invocation for later use. Prepared invocations
keep a snapshot of the route that was resolved at prepare time, so later changes
to the mutable kicker object or router route table do not change that prepared
send.

Flow selection is resolved in this order:

| Source | Effect |
| --- | --- |
| Explicit route | `.with_route(route)` uses that route's broker and flow. A later `.with_flow(flow)` updates the route flow, while a later `.with_broker(broker)` clears the explicit route. |
| Explicit broker | `.with_broker(broker)` sends through that broker. If `.with_flow(flow)` is also set, that flow wins; otherwise Taskiq uses the same-broker task route flow, then the broker `default_flow`. |
| Explicit flow | `.with_flow(flow)` without a broker override replaces the flow of the resolved route or broker default route. |
| Router task route | `router.route_task(task, broker=..., flow=...)` is the default outbound route for that task. |
| Broker default flow | `InMemoryBroker(..., default_flow=...)` and other brokers with `default_flow` provide the fallback flow for that broker. |

## Subscriptions

Routers also keep inbound flow subscriptions. Routing and subscribing are
separate operations: `route_task()` selects the outbound broker/flow for a task
invocation, while `subscribe()` adds a flow to the broker listen plan.

```python
billing_flow = Flow("billing.priority")

route = router.route_task(
    "billing.charge",
    broker=priority_broker,
    flow=billing_flow,
)

router.subscribe(priority_broker, billing_flow, "billing.charge")

subscriptions = router.get_subscriptions(priority_broker)
flows = priority_broker.get_subscribed_flows()
```

`route_task(...)` does not subscribe by default. This is intentional: outbound
routing answers "where should this invocation be sent?", while subscribing
answers "which flows should this broker listen to?".

The deprecated `route_task(..., subscribe=True)` shim still performs this
subscription when a flow is resolved, but new code should call `subscribe()`
directly.

`TaskiqSubscription.task_names` is diagnostic listen-plan metadata. It records
which task names caused the broker to listen to a flow, but it does not make the
flow an inbound task router. Workers still execute messages by
`TaskiqMessage.task_name`.

Existing brokers can keep implementing `listen()` as before. New flow-aware
brokers may use `get_subscribed_flows()` to subscribe to queues, topics,
subjects or streams while the routing rules stay in the router.

`get_subscribed_flows()` returns the broker `default_flow` plus explicit
subscriptions. It deduplicates by flow identity and checks broker options for
conflicts.

Router routes and subscriptions are mutable setup-time configuration. They are
not thread-safe runtime coordination primitives; configure them before starting
concurrent worker or client activity.

## Scheduler and requeue

`ScheduledTask` remains a transport-neutral invocation payload. It stores task
name, labels, args, kwargs, optional task id and schedule timing fields; it does
not store broker objects, broker names, route objects, flows or
transport-specific flow options.

When a scheduled task is ready, `TaskiqScheduler` resolves the task route in the
scheduler process through the scheduler broker's router. Route changes made
before `on_ready(...)` runs affect that scheduled dispatch. If no router route
exists for the scheduled task, Taskiq keeps the old behavior and sends through
the scheduler broker.

`with_broker(...)`, `with_route(...)` and `with_flow(...)` on a kicker are not
persisted schedule route metadata. They can affect `CreatedSchedule.kiq()`,
which is an immediate queued invocation helper, but they do not change the
stored schedule payload.

`Context.requeue()` is current-broker sticky. It sends the same task message
through the broker currently executing the task, even if the task's default
route now points to another broker. Requeue flow selection is explicit
low-level override, same-broker task route flow, current broker `default_flow`,
then no flow. Taskiq does not currently preserve the inbound source flow because
flow provenance is not stored in `TaskiqMessage` or `Context`.

## Shared task declarations

Libraries can declare tasks without importing an application broker:

```python
from taskiq import task_builder


@task_builder("billing.calculate_total", domain="billing")
async def calculate_total(price: int, quantity: int) -> int:
    return price * quantity
```

The final application binds the task definition to its router and broker:

```python
billing_flow = Flow("billing.tasks")

registered = router.register_task(
    calculate_total,
    broker=billing_broker,
    flow=billing_flow,
)

router.subscribe(
    billing_broker,
    billing_flow,
    registered,
)

result = await registered.kiq(19, 3)
```

Task name and labels belong to the shared declaration. Register a
`TaskDefinition` without `task_name` or label overrides; Taskiq rejects those
overrides instead of silently ignoring them.

The unbound task can still run locally:

```python
total = await calculate_total.call(19, 3)
```

Shared task declarations can also provide a custom task class for one task:

```python
from typing import Any

from taskiq import AsyncTaskiqDecoratedTask, task_builder


class TracingTask(AsyncTaskiqDecoratedTask[Any, Any]):
    def tracing_name(self) -> str:
        return self.task_name


@task_builder("billing.traced_charge", base_cls=TracingTask)
async def traced_charge(user_id: int) -> None:
    ...
```

When the final application registers this definition, Taskiq creates the bound
task using `TracingTask`. If `base_cls` is not provided, Taskiq uses the native
decorated task class.

Custom task classes do not bypass broker middleware. Send and execute lifecycle
hooks are still owned by the selected broker and worker path.

For low-level integrations, `TaskDefinition.message(...)` builds a
`TaskiqMessage` without binding the task. It uses the same argument and label
preparation contract as a normal `.kicker().prepare(...)` invocation.

## Compatibility and migration notes

The routing/flow branch keeps the old broker-first task API working:

```python
@broker.task(task_name="billing.charge")
async def charge(user_id: int) -> None:
    ...

await charge.kiq(42)
await charge.kicker().with_labels(source="api").kiq(42)
```

Migration guidance for application code:

- Keep using `@broker.task(...)` when one broker owns task declaration and send
  path. No router code is required for this case.
- Use one shared `TaskiqRouter` when several brokers must share task registry
  and routing policy.
- Pass broker objects to routing APIs. If configuration stores broker names,
  resolve them with `router.get_broker(name)` before calling `route_task(...)`.
- Replace `route_task(..., subscribe=True)` usage with explicit
  `router.subscribe(...)`.
- Do not use labels as a transport routing schema. Labels remain task metadata.
- Register a `TaskDefinition` without `task_name` or label overrides; plain
  callable registration still supports those overrides.

Migration guidance for broker packages:

- Existing brokers that implement only `kick(message)` and `listen()` remain
  valid. The default `kick_to_flow(message, flow=None)` calls `kick(message)`.
- Flow-aware brokers can override `kick_to_flow(...)` and map `flow.name` to a
  queue, topic, subject or stream.
- Flow-aware listeners can call `get_subscribed_flows()` during listen setup to
  add router-owned subscriptions while keeping old default listen behavior when
  there are no explicit subscriptions.
- Broker-specific declaration settings belong in broker-specific flow objects
  and `broker_options()`, not in task names, labels or scheduler payloads.
- Ack/nack behavior, serializer/formatter behavior, retry, middleware hooks and
  result backend ownership remain broker responsibilities.

Scheduler and requeue compatibility:

- `ScheduledTask` does not store routes, broker names, flow objects or
  broker-specific flow options in this iteration.
- Scheduled dispatch resolves the route late in the scheduler process through
  the scheduler broker's router.
- `Context.requeue()` sends through the broker currently executing the task. It
  does not re-resolve a task route across brokers.
- Taskiq does not preserve inbound source-flow provenance yet because that data
  is not stored in `TaskiqMessage` or `Context`.

## Examples

See executable examples for the two main usage shapes:

- [Multiple brokers and explicit routes](../examples/router/multiple_brokers.py)
  shows one task routed through several brokers with explicit subscription
  ownership.
- [Shared task package](../examples/router/shared_task_package.py) shows
  `task_builder(...)`, a custom `base_cls`, later registration and a prepared
  queued invocation.
