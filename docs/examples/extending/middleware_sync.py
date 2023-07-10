from time import sleep
from typing import Any

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult


class MyMiddleware(TaskiqMiddleware):
    def startup(self) -> None:
        print("RUN STARTUP")
        sleep(1)

    def shutdown(self) -> None:
        print("RUN SHUTDOWN")
        sleep(1)

    def pre_execute(self, message: "TaskiqMessage") -> TaskiqMessage:
        sleep(1)
        return message

    def post_save(self, message: "TaskiqMessage", result: "TaskiqResult[Any]") -> None:
        sleep(1)
        print("Post save")
