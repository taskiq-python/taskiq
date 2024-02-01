---
title: Dynamic Environments
order: 9
---

This article is for people who want to:

* Create brokers dynamically.
* Register tasks, and run them inside their code.
* Implement more complex logic.

Taskiq allows you to set up broker instances throughout your application and register tasks for dynamic execution. However, tasks created this way won't be found by the `taskiq worker` command.

To define tasks and assign them to a broker, use `register_task` method.

@[code python](../examples/dynamics/dyn_broker.py)

In this example, the task is defined using a lambda within the `main` function. As the lambda is not visible outside of the `main` function scope, the task is not executable by `taskiq worker` command.

To overcome this issue, you can:

* Create a dynamic worker task within the current event loop.
* Implement your own broker listener with the information about all of your tasks.

Here's an example of a dynamic worker task creation:

@[code python](../examples/dynamics/dyn_receiver.py)

In this example, a named dynamic lambda task is created and registered in a broker, similar to the previous example. The difference is the creation of a new receiver coroutine for the worker task. It will listen to the new messages and execute them. The worker task will be executed in the current event loop. After exiting the scope, the worker task will get cancelled. For illustration purposes it is cancelled explicitly.

It's possible to run a scheduler in the current event loop as well:

@[code python](../examples/dynamics/dyn_scheduler.py)
