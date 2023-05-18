---
order: 7
---

# State and Dependencies

## State

The `TaskiqState` is a global variable where you can keep the variables you want to use later.
For example, you want to open a database connection pool at a broker's startup.

This can be achieved by adding event handlers.

You can use one of these events:

- `WORKER_STARTUP`
- `CLIENT_STARTUP`
- `WORKER_SHUTDOWN`
- `CLIENT_SHUTDOWN`

Worker events are called when you start listening to the broker messages using taskiq.
Client events are called when you call the `startup` method of your broker from your code.

This is an example of code using event handlers:

@[code python](../examples/state/events_example.py)

::: tip Cool tip!

If you want to add handlers programmatically, you can use the `broker.add_event_handler` function.

:::

As you can see in this example, this worker will initialize the Redis pool at the startup.
You can access the state from the context.

## Dependencies

Using context directly is nice, but this way you won't get code-completion.

That's why we suggest you try TaskiqDependencies. The implementation is very similar to FastApi's dependencies. You can use classes, functions, and generators as dependencies.

We use the [taskiq-dependencies](https://pypi.org/project/taskiq-dependencies/) package to provide autocompetion.
You can easily integrate it in your own project.

::: danger Cool alarm!

FastAPI's `Depends` is not compatible with `TaskiqDepends`.

:::

### How dependencies are useful

You can use dependencies for better autocompletion and reduce the amount of code you write.
Since the state is generic, we cannot guess the types of the state fields.
Dependencies can be annotated with type hints and therefore provide better auto-completion.

Let's assume that you've stored a Redis connection pool in the state as in the example above.

```python
@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(state: TaskiqState) -> None:
    # Here we store connection pool on startup for later use.
    state.redis = ConnectionPool.from_url("redis://localhost/1")

```

You can access this variable by using the current execution context directly, like this:

```python
@broker.task
async def my_task(context: Context = TaskiqDepends()) -> None:
    async with Redis(connection_pool=context.state.redis, decode_responses=True) as redis:
        await redis.set('key', 'value')
```

If you hit the `TAB` button after the `context.state.` expression, your IDE won't give you any auto-completion.
But we can create a dependency function to add auto-completion.

```python

def redis_dep(context: Context = TaskiqDepends()) -> Redis:
    return Redis(connection_pool=context.state.redis, decode_responses=True)

@broker.task
async def my_task(redis: Redis = TaskiqDepends(redis_dep)) -> None:
    await redis.set('key', 'value')

```

Now, this dependency injection will be autocompleted. But, of course, state fields cannot be autocompleted,
even in dependencies. But this way, you won't make any typos while writing tasks.

### How do dependencies work

We build a graph of dependencies on startup. If the parameter of the function has
the default value of `TaskiqDepends` this parameter will be treated as a dependency.

Dependencies can also depend on something. Also dependencies are optimized to **not** evaluate things many times.

For example:

@[code python](../examples/state/dependencies_tree.py)

In this code, the dependency `common_dep` is going to be evaluated only once and the `dep1` and the `dep2` are going to receive the same value. You can control this behavior by using the `use_cache=False` parameter to you dependency. This parameter will force the
dependency to reevaluate all it's subdependencies.

In this example we cannot predict the result. Since the `dep2` doesn't use cache for the `common_dep` function.
@[code python](../examples/state/no_cache.py)

The graph for cached dependencies looks like this:

```mermaid
graph TD
    A[common_dep]
    B[dep1]
    C[dep2]
    D[my_task]
    A --> B
    A --> C
    B --> D
    C --> D
```

The dependencies graph for `my_task` where `dep2` doesn't use cached value for `common_dep` looks like this:

```mermaid
graph TD
    A[common_dep]
    B[dep1]
    D[my_task]
    C[dep2]
    subgraph without cache
        A1[common_dep]
    end
    A --> B
    A1 --> C
    B --> D
    C --> D
```

### Class as a dependency

You can use classes as dependencies, and they can also use other dependencies too.

Let's see an example:

@[code python](../examples/state/class_dependency.py)

As you can see, the dependency for `my_task` function is declared with `TaskiqDependency()`.
It's because you can omit the class if it's declared in type-hint for the parameter. This feature doesn't
work with dependency functions, it's only for classes.

You can pass dependencies for classes in the constructor.

### Generator dependencies

Generator dependencies are used to perform startup before task execution and teardown after the task execution.

@[code python](../examples/state/generator_deps.py)

In this example, we can do something at startup before the execution and at shutdown after the task is completed.

If you want to do something asynchronously, convert this function to an asynchronous generator. Like this:

@[code python](../examples/state/async_generator_deps.py)

### Default dependencies

By default taskiq has only two dependencies:

- Context from `taskiq.context.Context`
- TaskiqState from `taskiq.state.TaskiqState`


### Adding first-level dependencies

You can expand default list of available dependencies for you application.
Taskiq have an ability to add new first-level dependencies using brokers.

The AsyncBroker interface has a function called `add_dependency_context` and you can add
more default dependencies to the taskiq. This may be useful for libraries if you want to
add new dependencies to users.
