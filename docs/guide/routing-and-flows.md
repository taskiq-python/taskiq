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
Legacy sends without a selected flow keep working; selecting an explicit flow
requires the broker adapter to implement that capability.
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
router for that broker. Use an explicit shared router when one application
sends tasks through several brokers under the same routing policy.

The worker CLI can explicitly listen to several brokers from one process. Export
a non-empty module-level sequence and pass its import path as the existing
broker argument:

```python
# application/worker.py
worker_brokers = (commands_broker, events_broker)
```

```bash
taskiq worker application.worker:worker_brokers application.tasks
```

The sequence is the complete listener set. Other brokers registered in the
same Router do not receive listeners; the runtime starts them in client mode so
tasks can publish routed child invocations through them. Passing one broker
keeps the legacy lifecycle and does not start Router peers.

`InMemoryBroker` remains a deliberate special case: it executes sends in the
client process, so its legacy lifecycle always includes both client and worker
event phases even when it is an outbound-only Router peer.

All listener brokers must be distinct, use the same Router and have unique
broker names. One child `Receiver` owns each transport's formatter, middleware,
result backend, acknowledgement and iterator. The executor,
`--max-async-tasks`, `--max-prefetch` and `--max-tasks-per-child` budgets remain
process-wide. Startup is sequential, listener shutdown overlaps, and broker
cleanup runs in reverse startup order.

Every managed broker must own distinct middleware and result-backend Python
instances. Separate instances may point to the same external service, but
sharing one lifecycle object across brokers is rejected before startup so
Taskiq cannot initialize or close it twice.

Without `--wait-tasks-timeout`, listener shutdown, including running callbacks,
has no deadline, preserving single-broker CLI behavior. When the option is set,
each Receiver cancels and awaits callbacks at that boundary; the coordinator
allows the configured `--shutdown-timeout` as post-drain cleanup time before
hard-cancelling a listener. Each broker shutdown then receives its own timeout.

Each listener owns one pending transport read. If several deliveries complete
at the exact `--max-tasks-per-child` boundary, the runtime drains those accepted
messages instead of discarding them; the final count can exceed the threshold
by at most the number of additional listeners. It never becomes a separate
full task budget per broker.

Router routes and subscriptions must be configured before worker startup. A
custom Receiver may inherit the base runtime-state behavior; an override of
`attach_runtime_state()` must call `super()` and preserve the returned state.
Importing a Router itself is not a worker target.

The core runtime accepts existing adapters, but transport-specific constructor
attachment and Flow translation are adapter capabilities. Verify those against
the adapter version used by the application before combining RabbitMQ, Kafka,
Redis, NATS or another transport in one worker.

Pass broker objects to routing APIs. If broker names come from configuration,
resolve them explicitly first:

```python
broker = router.get_broker("priority")
route = router.route_task("billing.charge", broker=broker)
```

This keeps routing errors local and avoids hidden string references. A task
route declared without `flow=` follows the selected broker's current
`default_flow` at dispatch time. Changing that default after task registration
therefore affects later normal sends; an explicit route flow remains fixed.

Router registry properties are immutable point-in-time snapshots for
diagnostics: `brokers`, `task_registry` and `routes` return read-only mappings,
while `subscriptions` returns a tuple. They are not live views. Use
`set_broker()`, `register_task()`, `route_task()`, `remove_route()`,
`subscribe()` and `unsubscribe()` so every mutation passes through the owning
registry's validation.

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

`router.route_task()` returns a resolved `TaskiqRoute`. You can pass that object
to a kicker when you want the current broker and flow to be an explicit
call-site snapshot.

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

Kicker routing modifiers are applied in call order. The last modifier wins for
the state it controls: `with_route()` replaces broker and flow selection,
`with_broker()` clears every earlier route and flow override, and `with_flow()`
replaces only the flow selected so far.

`with_broker(...)` remains available for shared-task style invocation. Taskiq
then resolves the task for the override broker: it uses that broker's explicit
same-broker task route flow when one exists, otherwise the broker's current
`default_flow`. A later `with_flow(flow)` overrides that fallback.

`with_flow()` requires a flow object. `None` is intentionally rejected because
it could mean either "remove my override" or "send without a flow". To bypass a
configured broker default explicitly, use a fully specified route:

```python
from taskiq import TaskiqRoute

no_flow_route = TaskiqRoute(broker=default_broker, flow=None)
task = await charge.kicker().with_route(no_flow_route).kiq(42)
```

The resulting resolution contract is:

| Source | Effect |
| --- | --- |
| `with_route(route)` | Uses exactly that route's broker and flow, including an explicit `flow=None`. A later broker or flow modifier replaces the corresponding selection. |
| `with_broker(broker)` | Clears earlier route/flow overrides. A later `with_flow(flow)` wins; otherwise Taskiq uses an explicit same-broker task route flow, then the broker's current `default_flow`. |
| `with_flow(flow)` | Replaces the flow selected by an earlier route/broker/default policy. A later route or broker modifier replaces or clears it. |
| Router task route | Selects the default outbound broker. Its explicit flow wins; an omitted flow is resolved from the broker's current `default_flow` for each normal send. |
| Broker default flow | Provides the dispatch-time fallback when no explicit route flow exists. |

`prepare(...)` resolves this table once. The resulting `PreparedKiq` retains the
resolved broker and flow even if the route table or broker `default_flow`
changes before `prepared.kiq()`.

Routed and legacy sends use the same client middleware/error contract. Taskiq
runs `pre_send` in registration order and `post_send` in reverse order.
Middleware and route-configuration exceptions keep their original types;
formatter and transport exceptions are raised as `SendTaskError` with the
original exception as their cause. Cancellation is never converted into a send
failure, and `post_send` is not called when formatting or transport dispatch
fails.

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

`router.unsubscribe(broker, flow)` removes one compatible explicit
subscription and returns the removed `TaskiqSubscription`, or `None` when no
such subscription exists. A flow declaration with the same identity but
different broker options is rejected instead of removing an ambiguous entry.
Removing an explicit subscription does not disable the broker's
`default_flow`; change broker configuration explicitly when that default should
no longer be listened to.

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
concurrent worker or client activity. Immutable snapshots prevent accidental
invariant bypass, but they do not add synchronization for concurrent writers.

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

Scheduler startup owns producer lifecycle for every broker already attached to
that router. Brokers start in registration order and shut down in reverse
order. A partial startup failure still shuts down every broker whose startup
was attempted. Configure the router before scheduler startup; brokers attached
later are not part of that lifecycle snapshot. A scheduler whose broker has no
other router members keeps the legacy single-broker lifecycle.

The same distinct-resource rule described for multi-broker workers applies to
these scheduler-managed brokers.

`with_broker(...)`, `with_route(...)` and `with_flow(...)` on a kicker are not
persisted schedule route metadata. They can affect `CreatedSchedule.kiq()`,
which is an immediate queued invocation helper, but they do not change the
stored schedule payload.

`Context.requeue()` is current-broker sticky. It publishes a replacement copy
through the broker currently executing the task, even if the task's default
route now points to another broker. Requeue flow selection is explicit
low-level override, same-broker task route flow, current broker `default_flow`,
then no flow. Taskiq does not currently preserve the inbound source flow because
flow provenance is not stored in `TaskiqMessage` or `Context`.

Replacement publication and acknowledgement follow an explicit ownership
contract:

- The replacement gets an incremented `X-Taskiq-requeue` label. The current
  Context message is updated only after publication succeeds.
- After successful publication, normal `when_executed` and `when_saved` timing
  remains intact. A `manual` delivery is acknowledged by Receiver after the
  terminal requeue path. `AckController` keeps every acknowledgement
  idempotent.
- A failed or cancelled publication remains unacknowledged for
  `when_executed`, `when_saved` and `manual`, allowing broker redelivery. The
  original label is not incremented.
- `UnsupportedFlowError` follows the same failure contract. A legacy adapter
  with an explicit same-broker Flow can therefore redeliver the original until
  the route is corrected or the transport's dead-letter policy intervenes.
- `when_received` is intentionally different: the original was acknowledged
  before task execution, so a later publication failure cannot restore it.
- If acknowledgement itself fails after successful publication, that error is
  propagated and the controller remains unacknowledged. The replacement may
  already exist, so broker redelivery can produce a duplicate and task handlers
  must remain idempotent.
- A non-ackable broker can publish the replacement but cannot settle the
  original through Taskiq.

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

`TaskDefinition` preserves the wrapped callable's name, qualified name,
module, documentation and inspectable signature. Binding the same definition
independently in separate applications or Routers does not rename or otherwise
mutate the package-level function. Within one shared Router, register the
definition once; every listener broker can resolve that Router task by name.
Bound task objects expose the same callable metadata, while Taskiq uses a
stable internal module-level callable for worker execution and process pool
serialization.

Factory-generated declarations from the same Python source location must use
stable, unique task names. Taskiq includes `task_name` in the deterministic
module export identity, so each declaration remains independently importable
and picklable in spawned worker processes.

As with regular synchronous Taskiq tasks, functions intended for a spawned
process pool must be declared at module scope so the worker process can import
their module. The internal binding callable is an implementation detail and
does not change the task name or message contract.

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
task using `TracingTask`. An explicit `base_cls` takes precedence over broker
defaults. If `base_cls` is not provided, Taskiq uses the selected broker's
`decorator_class`, preserving broker-specific task and kicker APIs.

Custom task classes do not bypass broker middleware. Send and execute lifecycle
hooks are still owned by the selected broker and worker path.

Binding is published in two phases: Router validates task-name availability
before constructing a bound task, the broker stores it through
`store_registered_task()`, and Router then publishes the task and route. A
storage failure therefore does not leave a partial Router entry.

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
- Replace direct mutation of Router registry properties with the validated
  registration, route and subscription methods described above.
- Replace `route_task(..., subscribe=True)` usage with explicit
  `router.subscribe(...)`.
- Do not use labels as a transport routing schema. Labels remain task metadata.
- Register a `TaskDefinition` without `task_name` or label overrides; plain
  callable registration still supports those overrides.
- Keep task names unique within one Router. Re-registering a different task
  object under an existing Router task name raises `ValueError` instead of
  silently replacing execution policy.
- To listen on several brokers in one worker process, export a module-level
  broker tuple and pass that single import path to `taskiq worker`. Do not pass
  several broker paths as positional arguments and do not use Router membership
  as implicit listener configuration.
- Brokers in the listener tuple share process-wide CLI limits. Remaining
  brokers in that Router receive client lifecycle only; keep them out of the
  tuple when this worker must not consume their transport.

Migration guidance for broker packages:

- Existing brokers that implement only `kick(message)` and `listen()` remain
  valid. The default `kick_to_flow(message, flow=None)` calls `kick(message)`.
  If an explicit flow reaches that fallback, it raises
  `UnsupportedFlowError` instead of silently sending to the broker's old
  default destination.
- Flow-aware brokers can override `kick_to_flow(...)` and map `flow.name` to a
  queue, topic, subject or stream.
- The built-in `InMemoryBroker` explicitly accepts every Flow as local routing
  metadata because it has one in-process execution destination.
- A successful `kick_to_flow(...)` return is the publication boundary used by
  `Context.requeue()` before Receiver acknowledges the original delivery.
  Flow-aware adapters must not return success before their normal send contract
  considers the replacement accepted.
- Flow-aware listeners can call `get_subscribed_flows()` during listen setup to
  add router-owned subscriptions while keeping old default listen behavior when
  there are no explicit subscriptions.
- Broker-specific declaration settings belong in broker-specific flow objects
  and `broker_options()`, not in task names, labels or scheduler payloads.
- Brokers that store registered tasks outside `local_task_registry` must
  override `store_registered_task()`. `AsyncSharedBroker`, for example, keeps
  bound definitions in the global task registry just like its old decorator
  path.
- Ack/nack behavior, serializer/formatter behavior, retry, middleware hooks and
  result backend ownership remain broker responsibilities.

Scheduler and requeue compatibility:

- `ScheduledTask` does not store routes, broker names, flow objects or
  broker-specific flow options in this iteration.
- An explicit kicker task id is stored consistently for cron, interval and
  time schedules; when absent, the scheduler generates an invocation id at
  dispatch time.
- Scheduled dispatch resolves the route late in the scheduler process through
  the scheduler broker's router.
- Scheduler startup starts all brokers already attached to that router and
  shuts them down in reverse order, including cleanup after partial startup.
  Configure broker membership before scheduler startup.
- `Context.requeue()` sends through the broker currently executing the task. It
  does not re-resolve a task route across brokers.
- Immediate retries published by `SimpleRetryMiddleware` or
  `SmartRetryMiddleware` stay on the broker currently executing the task, so
  the retry keeps that broker's send lifecycle and result backend. When
  `SmartRetryMiddleware` uses a `ScheduleSource`, it creates a normal
  `ScheduledTask`; the scheduler then applies the late route-resolution rules
  above.
- Taskiq does not preserve inbound source-flow provenance yet because that data
  is not stored in `TaskiqMessage` or `Context`.

## Examples

See executable examples for the two main usage shapes:

- [Multiple brokers and explicit routes](../examples/router/multiple_brokers.py)
  shows one task routed through several brokers with explicit subscription
  ownership.
- [Multi-broker worker](../examples/router/multi_broker_worker.py) shows the
  explicit listener tuple and an outbound-only Router broker used by the CLI
  runtime.
- [Shared task package](../examples/router/shared_task_package.py) shows
  `task_builder(...)`, a custom `base_cls`, later registration and a prepared
  queued invocation.
