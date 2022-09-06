from time import sleep
from typing import Any

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult


class MyMiddleware(TaskiqMiddleware):
    def pre_execute(self, message: "TaskiqMessage") -> TaskiqMessage:
        sleep(1)
        return message

    def post_save(self, message: "TaskiqMessage", result: "TaskiqResult[Any]") -> None:
        sleep(1)
        print("Post save")
