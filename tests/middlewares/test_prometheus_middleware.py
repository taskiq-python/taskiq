import pytest

from taskiq import PrometheusMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

pytest.importorskip("prometheus_client")


def _make_message(task_name: str = "test_task") -> TaskiqMessage:
    return TaskiqMessage(
        task_id="test_id",
        task_name=task_name,
        labels={},
        args=[],
        kwargs={},
    )


def test_multiple_instances_do_not_raise_duplicate_timeseries() -> None:
    """Regression test for https://github.com/taskiq-python/taskiq/issues/397."""
    first = PrometheusMiddleware(server_port=19001)
    second = PrometheusMiddleware(server_port=19001)

    assert first.found_errors is second.found_errors
    assert first.received_tasks is second.received_tasks
    assert first.success_tasks is second.success_tasks
    assert first.saved_results is second.saved_results
    assert first.execution_time is second.execution_time


def test_metrics_still_work_after_reuse() -> None:
    first = PrometheusMiddleware(server_port=19002)
    second = PrometheusMiddleware(server_port=19002)

    message = _make_message()

    second.pre_execute(message)
    second.post_execute(
        message,
        TaskiqResult(is_err=False, return_value=None, execution_time=0.01),
    )
    second.post_execute(
        message,
        TaskiqResult(is_err=True, return_value=None, execution_time=0.02),
    )
    second.post_save(
        message,
        TaskiqResult(is_err=False, return_value=None, execution_time=0.01),
    )

    # Both instances share collectors, so increments are visible either way.
    assert first.received_tasks.labels(message.task_name)._value.get() >= 1
    assert first.success_tasks.labels(message.task_name)._value.get() >= 1
    assert first.found_errors.labels(message.task_name)._value.get() >= 1
    assert first.saved_results.labels(message.task_name)._value.get() >= 1
