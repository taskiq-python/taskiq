---
order: 7
---

# State and events

The `TaskiqState` is a global variable where you can keep the variables you want to use later.
For example, you want to open a database connection pool at a broker's startup.

This can be acieved by adding event handlers.

You can use one of these events:
* `WORKER_STARTUP`
* `CLIENT_STARTUP`
* `WORKER_SHUTDOWN`
* `CLIENT_SHUTDOWN`

Worker events are called when you start listening to the broker messages using taskiq.
Client events are called when you call the `startup` method of your broker from your code.

This is an example of code using event handlers:

@[code python](../examples/state/events_example.py)

::: tip Cool tip!

If you want to add handlers programmatically, you can use the `broker.add_event_handler` function.

:::

As you can see in this example, this worker will initialize the Redis pool at the startup.
You can access the state from the context.
