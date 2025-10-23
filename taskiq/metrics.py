from typing import Any, Self

from pydantic import BaseModel, Field, computed_field, model_validator


class Label(BaseModel):
    """Base Label class."""

    name: str
    count: int = 0


class CounterLabel(Label):
    """
    Counter label class.

    Has just one count metric.
    Used withe count or quantity data.
    Attributes:
        count: int
    """

    def inc(self) -> None:
        """Increments counter by 1."""
        self.count += 1


class AggregateLabel(Label):
    """
    Aggregate label class.

    Aggregation counter.
    Used for aggregation of data with different values.
    Duration or so.
    Attributes:
        count: int - the quantity of aggregated values
        average: float - the average value since start
        avg_dev: float - average deviation from average value
        max_dev: float - maximum absolute deviation from average value
    """

    average: float = 0.0
    avg_dev: float = 0.0
    max_dev: float = 0.0

    def aggregate(self, value: float) -> None:
        """
        Aggregates value and calculates average, avg_dev, max_dev.

        Aggregation increases count attribute.
        :param value: float - value to aggregate
        """
        new_count = self.count + 1
        delta = value - self.average
        self.average = (self.average * self.count + value) / new_count
        self.avg_dev = (self.avg_dev * (self.count - 1) + value) / new_count
        self.count = new_count
        if abs(delta) > abs(self.max_dev):
            self.max_dev = delta


class Labels[T: Label](BaseModel):
    """
    Base stat class.

    Base collection class for counters.
    Implements label(label_name) function.

    Attributes:
        name: name of the stat counter
        description: description of the counter
        labels: list of counter labels
        labels_dict: convenience dict to speed up label selection
        label: function of selecting or creating label with specified name
        reset: function of resetting counter labels and values, used in serialization
    """

    label_class: type[T] = Field(
        default=CounterLabel,
        exclude=True,
    )
    name: str
    description: str | None = Field(default=None)
    labels: list[T] = Field(default=[])
    labels_dict: dict[str, T] = Field(
        default={},
        exclude=True,
    )

    @model_validator(mode="after")
    def _post_create(self) -> Self:
        if self.labels and not self.labels_dict:
            self.labels_dict = {label.name: label for label in self.labels}
        return self

    def label(self, label_name: str) -> T:
        """Returns Label subclass instance from labels_dict."""
        if label := self.labels_dict.get(label_name):
            return label
        label = self.label_class(name=label_name)
        self.labels_dict[label_name] = label
        self.labels.append(label)
        return label

    def reset(self) -> None:
        """Resets counter."""
        self.labels = []
        self.labels_dict = {}


class Counter(Labels[CounterLabel]):
    """
    Counter stats class.

    Simple counter class.
    Just increments count value.
    Counts sum count value of all labels.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(label_class=CounterLabel, **kwargs)

    @computed_field()
    def count(self) -> int:
        """Returns Counter value."""
        return sum([cnt.count for cnt in self.labels])

    def __add__(self, other: "Counter") -> "Counter":
        if not isinstance(other, Counter):
            raise ValueError("other is not instance of type Counter.")
        counter = Counter(name=self.name, description=self.description)
        for label_name in set(
            list(self.labels_dict.keys()) + list(other.labels_dict.keys()),
        ):
            self_label = self.labels_dict.get(label_name) or self.label_class(
                name=label_name,
            )
            other_label = other.labels_dict.get(label_name) or other.label_class(
                name=label_name,
            )
            counter.label(label_name).count = self_label.count + other_label.count
        return counter


class Aggregator(Labels[AggregateLabel]):
    """
    Aggregator stats class.

    Aggregator counter class is more complex stat metric, for using with duration data.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(label_class=AggregateLabel, **kwargs)

    def __add__(self, other: "Aggregator") -> "Aggregator":
        if not isinstance(other, Aggregator):
            raise ValueError("other is not instance of type Aggregator.")
        aggregator = Aggregator(name=self.name, description=self.description)
        for label_name in set(
            list(self.labels_dict.keys()) + list(other.labels_dict.keys()),
        ):
            self_label = self.labels_dict.get(label_name) or self.label_class(
                name=label_name,
            )
            other_label = other.labels_dict.get(label_name) or other.label_class(
                name=label_name,
            )
            agg_label = aggregator.label(label_name)
            sum_count = self_label.count + other_label.count
            agg_label.count = sum_count
            agg_label.average = (
                self_label.average * self_label.count
                + other_label.average * other_label.count
            ) / sum_count
            agg_label.avg_dev = (
                self_label.avg_dev * self_label.count
                + other_label.avg_dev * other_label.count
            ) / sum_count
            agg_label.max_dev = (
                other_label.max_dev
                if abs(other_label.max_dev) > abs(self_label.max_dev)
                else self_label.max_dev
            )
        return aggregator

    @computed_field()
    def count(self) -> int:
        """Returns sum count value of all labels."""
        return sum([cnt.count for cnt in self.labels])

    @computed_field()
    def average(self) -> float:
        """Returns average value of all labels."""
        sum_count = sum([cnt.count for cnt in self.labels])
        if sum_count:
            return sum([cnt.average * cnt.count for cnt in self.labels]) / sum_count
        return 0.0

    @computed_field()
    def avg_dev(self) -> float:
        """Returns average deviation value of all labels."""
        sum_count = sum([cnt.count for cnt in self.labels])
        if sum_count:
            return sum([cnt.avg_dev * cnt.count for cnt in self.labels]) / sum_count
        return 0.0

    @computed_field()
    def max_dev(self) -> float:
        """Returns maximum deviation value of all labels."""
        devs = [cnt.max_dev for cnt in self.labels]
        if devs:
            abs_devs = [abs(dev) for dev in devs]
            return devs[abs_devs.index(max(abs_devs))]
        return 0.0


class Stat(BaseModel):
    """
    Stats model for the worker.

    Attributes:
        task_errors: Counter for tasks with errors
        received_tasks: Counter for all received tasks
        execution_time: Aggregator for all executed tasks
    """

    task_errors: Counter = Field(
        default=Counter(
            name="task errors",
            description="Number of tasks with errors",
        ),
    )
    received_tasks: Counter = Field(
        default=Counter(
            name="received tasks",
            description="Number of received tasks",
        ),
    )
    execution_time: Aggregator = Field(
        default=Aggregator(
            name="execution time",
            description="Time of function execution",
        ),
    )

    def __add__(self, other: "Stat") -> "Stat":
        if not isinstance(other, Stat):
            raise ValueError("other is not instance of type Stat.")
        stat = self.model_copy(deep=True)
        stat.task_errors = self.task_errors + other.task_errors
        stat.received_tasks = self.received_tasks + other.received_tasks
        stat.execution_time = self.execution_time + other.execution_time
        return stat

    def reset(self) -> None:
        """Resets all counters."""
        self.task_errors.reset()
        self.received_tasks.reset()
        self.execution_time.reset()


class Stats(Stat):
    """Summarized statistics for workers."""

    workers: dict[int, Stat] | None = {}

    @model_validator(mode="after")
    def _post_create(self) -> Self:
        if self.workers:
            self.reset()
            for worker in self.workers.values():
                self.task_errors += worker.task_errors
                self.received_tasks += worker.received_tasks
                self.execution_time += worker.execution_time
        return self
