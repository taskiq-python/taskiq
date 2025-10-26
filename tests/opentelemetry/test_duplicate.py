import unittest

from taskiq.instrumentation import TaskiqInstrumentor

from .taskiq_test_tasks import broker


class TestUtils(unittest.TestCase):
    @staticmethod
    def test_duplicate_instrumentation() -> None:
        first = TaskiqInstrumentor()
        first.instrument_broker(broker)
        second = TaskiqInstrumentor()
        second.instrument_broker(broker)
        TaskiqInstrumentor().uninstrument()
