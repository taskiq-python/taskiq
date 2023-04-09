---
order: 1
---

# Brokers

To add a new broker you need to implement two methods `kick` and `listen` of the `taskiq.abc.broker.AsyncBroker` abstract class.
But along with them we have helper methods. Such as shutdown and startup.

Here is a template for new brokers:

@[code python](../examples/extending/broker.py)


# About kick and listen

The `kick` method takes a `BrokerMessage` as a parameter. The `BrokerMessage` class is a handy helper class for brokers. You can use information from the BrokerMessage to alter the delivery method.

::: warning "cool warning!"

As a broker developer, please send only raw bytes from the `message` field of a BrokerMessage if possible. Serializing it to the string may result in a problem if message bytes are not utf-8 compatible.

:::


The `listen` method should yield raw bytes that were sent over the network.

## Conventions

For brokers, we have several conventions. It's good if your broker implements them.
These rules are optional, and it's ok if your broker doesn't implement them.

1. If the message has the `delay` label with int or float number, this task's `execution` must be delayed
   with the same number of seconds as in the delay label.
2. If the message has the `priority` label, this message must be sent with priority. Tasks with
   higher priorities are executed sooner.
