---
title: Batching tasks
order: 6
---

# Batching tasks

Some tasks have a high fixed cost per call but become much cheaper when processed together. Think of database writes, calls to an external API, ML inference, event publishing or search indexing — running them one by one wastes most of the time on overhead. If a single call takes ~1 second, then 10 separate calls take ~10 seconds, but processing all 10 at once might take only ~3 seconds.

Taskiq can collect many task invocations into a single batched call. Instead of running every message on its own, the worker buffers messages of the same task and executes the function once with the whole list.

## Defining a batched task

Pass `batch=True` to the `task` decorator and declare the function with a single parameter that receives the list of items.

```python
@broker.task(batch=True, batch_size=100, batch_timeout=3)
async def process_items(items: list[int]) -> int:
    return sum(items)
```

Each `.kiq` call sends a single item, exactly like a normal task:

```python
await process_items.kiq(1)
await process_items.kiq(2)
```

The worker accumulates these items and calls `process_items` once with the collected list (e.g. `[1, 2, ...]`).

::: tip Typed by design

`.kiq` accepts a single element, while the function body receives `list[item]`.
Both sides are correctly typed: `process_items.kiq(1)` type-checks, but `process_items.kiq([1, 2])` is reported as a type error.

:::

## When a batch is flushed

A batch is sent for execution as soon as **either** condition is met:

- **`batch_size`** — the buffer reaches this number of items, or
- **`batch_timeout`** — this many seconds pass since the first item entered the buffer.

Whichever happens first wins. You must set at least one of the two; you can set both. The timer starts with the first item of a fresh buffer and resets after each flush. When a worker shuts down gracefully, any buffered items are flushed so nothing is lost.

Each worker buffers independently and keeps a separate buffer per task name.

## Results and acknowledgement

A batch produces a single result. That same return value (or error) is stored for **every** task in the batch, so each `.kiq` call can still await its own result. If the batched function raises, every task in the batch receives that error. Every message is acknowledged according to the configured [acknowledgement type](./cli.md).

::: caution Per-item granularity

Batching trades per-item isolation for throughput. The whole batch shares one result and one fate — there are no per-item results or per-item error handling. A batched task must take exactly one positional argument (the list); keyword arguments are not part of the batched call.

:::

## Trying it locally

Batching is a worker-side feature, but the `InMemoryBroker` supports it too, so you can try it without setting up a real broker. Call `wait_all` to flush any pending batches and wait for them to finish before reading results.

@[code python](../examples/batching/inmemory_batch.py)

Running this prints a single batch execution and the shared result:

```bash:no-line-numbers
$ python broker.py
Processing a batch of 10 items.
Returned value: 45
... (10 times)
```

::: warning InMemoryBroker behavior

The `InMemoryBroker` executes tasks inplace, so batches are flushed by `batch_size`, by `wait_all`, or — with `await_inplace=True` — immediately as one-item batches. This is convenient for tests, but to see real batching across processes you need a distributed broker and a worker.

:::

## Running with a worker

In production, batching happens inside the worker. Using [taskiq-redis](https://pypi.org/project/taskiq-redis/) as an example:

@[code python](../examples/batching/redis_batch.py)

Start one or more workers:

```bash:no-line-numbers
taskiq worker broker:broker
```

Then run the script to send items. The worker collects them and runs `process_items` once per batch. With several workers, each one batches the messages it receives independently, so the load is spread across all of them.
