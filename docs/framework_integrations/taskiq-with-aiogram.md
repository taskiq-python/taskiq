# Taskiq + Aiogram

[Taskiq-Aiogram](https://github.com/taskiq-python/taskiq-aiogram) is a nice integration with one of the best telegram bot libraries - [aiogram](https://docs.aiogram.dev/en/latest/).

This integration allows you to easily send delayed messages or run intensive functions without blocking the message handing.

This integration adds three main dependencies which you can use in your taskiq functions:

- `aiogram.Bot` - the bot instance that you can use to send messages or perform other actions. If multiple bots listen to the same dispatcher, this dependency will be resolved to the latest bot passed in the `taskiq_aiogram.init` function.
- `aiogram.Dispatcher` - current dispatcher instance.
- `List[aiogram.Bot]` - list of all bots that were passed to the `taskiq_aiogram.init` function.

To enable the integration, please install the `taskiq-aiogram` library:

```bash:no-line-numbers
pip install "taskiq-aiogram"
```

After the installation is complete, add an initialization function call to your broker's main file so it becomes something like this:

```python title="tkq.py"
import asyncio

import taskiq_aiogram
from aiogram import Bot
from taskiq import TaskiqDepends
from taskiq_redis import ListQueueBroker

broker = ListQueueBroker("redis://localhost")

# This line is going to initialize everything.
taskiq_aiogram.init(
    broker,
    # This is path to the dispatcher.
    "bot:dp",
    # This is path to the bot instance.
    "bot:bot",
    # You can specify more bots here.
)


@broker.task(task_name="my_task")
async def my_task(chat_id: int, bot: Bot = TaskiqDepends()) -> None:
    print("I'm a task")
    await asyncio.sleep(4)
    await bot.send_message(chat_id, "task completed")

```

Let's see how to use this integration.

```python title="bot.py"
import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

from tkq import broker, my_task

dp = Dispatcher()
bot = Bot(token="TOKEN")


# Taskiq calls this function when starting the worker.
@dp.startup()
async def setup_taskiq(bot: Bot, *_args, **_kwargs):
    # Here we check if it's a clien-side,
    # Becuase otherwise you're going to
    # create infinite loop of startup events.
    if not broker.is_worker_process:
        logging.info("Setting up taskiq")
        await broker.startup()


# Taskiq calls this function when shutting down the worker.
@dp.shutdown()
async def shutdown_taskiq(bot: Bot, *_args, **_kwargs):
    if not broker.is_worker_process:
        logging.info("Shutting down taskiq")
        await broker.shutdown()


## Simple command to handle
@dp.message(Command("task"))
async def message(message: types.Message):
    await my_task.kiq(message.chat.id)


## Main function that starts the bot.
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())

```

That's it. Now you can easily call tasks from your bots and access bots from within your tasks.
