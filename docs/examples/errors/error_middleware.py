from typing import Any

from taskiq import InMemoryBroker, TaskiqMessage, TaskiqMiddleware, TaskiqResult


class ErrorReporterMiddleware(TaskiqMiddleware):
    def on_error(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> None:
        # This code runs on the worker, so the traceback
        # of the exception is still available here.
        print(
            f"Task {message.task_name} with id {message.task_id} "
            f"failed: {exception!r}",
        )


broker = InMemoryBroker().with_middlewares(ErrorReporterMiddleware())
