---
title: Dynamic Environments
order: 9
---

This article is for all the people who want to dynamically create brokers, register tasks, and run them inside their code. Or maybe implement more complex logic.

The Taskiq allows you to create broker instances in all parts of your application. You
can register tasks dynamically and run them. But when tasks are created dynamically,
the `taskiq worker` command won't be able to find them.

To define tasks and assign them to broker, use `register_task` method.

@[code python](../examples/dynamics/broker.py)

The problem with this code is that if we run the `taskiq worker` command, it won't be able
to execute our tasks. Because lambdas are created within the `main` function and they
are not visible outside of it.

To surpass this issue, we need to create a dynamic worker task within the current loop.
Or, we can create a code that can listen to our brokers and have all information about dynamic
functions.

Here I won't be showing how to create your own CLI command, but I'll show you how to create
a dynamic worker within the current loop.

@[code python](../examples/dynamics/receiver.py)

Here we define a dynamic lambda task with some name, assign it to broker, as we did before.
The only difference is that we start our receiver coroutine, that will listen to the new
messages and execute them. Receiver task will be executed in the current loop, and when main function
exits, the receriver task is canceled. But for illustration purpose, I canceled it manually.

Sometimes you need to run not only receiver, but a scheduler as well. You can do it, by using
another function that also can work within the current loop.

@[code python](../examples/dynamics/scheduler.py)
