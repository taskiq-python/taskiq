---
order: 3
---

# Taskiq + FastStream

[FastStream](https://faststream.ag2.ai/latest/) is a library that allows you to write consumers and producers for different message brokers almost like taskiq. But the difference is that taskiq is more focused on tasks for a specific project and more like celery but async, while FastStream is more focused on events and defining how different systems communicate with each other using distributed brokers.

If you want to declare communication between different projects you can use taskiq, but it might be a bit more complex than using FastStream.

Although these libraries solve different problems, they have integration between each other, so you can use FastStream as a broker for taskiq. It allows FastStream to use taskiq's scheduler along with its own features.

To use FastStream as a broker for taskiq you need to install the `taskiq-faststream` library:

```bash:no-line-numbers
pip install "taskiq-faststream"
```

And you can use it like this:

```python
from faststream import FastStream
from faststream.kafka import KafkaBroker
from taskiq_faststream import BrokerWrapper

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

taskiq_broker = BrokerWrapper(broker)
```

You can read more about scheduling tasks for FastStream in the [FastStream documentation](https://faststream.ag2.ai/latest/scheduling/?h=schedule).
