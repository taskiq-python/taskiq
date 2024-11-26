import random

from taskiq.metrics import Aggregator, Counter, Stat, Stats

random.seed()

data = [5, 7, 9, 0, 1, 3, 5, 5, 3, 2, 6, 9, 5, 1, 7, 9, 9, 4, 3, 8, 7, 5, 9, 0, 2, 4, 5]
labels = ["label 1", "label 2", "label 3", "label 4", "label 5"]


def test_counter() -> None:
    metrix = {}
    for worker_pid in (1, 2):
        counter = Counter(
            name=f"test counter for worker {worker_pid}",
            description="test counter description",
        )
        metrix[worker_pid] = counter
        for _ in data:
            label = random.choice(labels)
            counter.label(label).inc()
        dump = counter.model_dump()
        assert Counter.model_validate(dump)
    cnt1, cnt2 = metrix.values()
    cnt = cnt1 + cnt2
    assert cnt.count == cnt1.count + cnt2.count  # type: ignore


def test_aggregator() -> None:
    metrix = {}
    for worker_pid in (1, 2):
        agg = Aggregator(
            name=f"test aggregator for worker {worker_pid}",
            description="test aggregator description",
        )
        metrix[worker_pid] = agg
        for val in data:
            label = random.choice(labels)
            agg.label(label).aggregate(val)
        dump = agg.model_dump()
        assert Aggregator.model_validate(dump)
    agg1, agg2 = metrix.values()
    agg = agg1 + agg2
    assert agg.count == agg1.count + agg2.count  # type: ignore


def test_stat() -> None:
    stats: dict[int, Stat] = {}
    for worker_pid in (1, 2):
        stat = Stat()
        stats[worker_pid] = stat
        for val in data:
            label = random.choice(labels)
            stat.received_tasks.label(label).inc()
            stat.execution_time.label(label).aggregate(val)
        dump = stat.model_dump(exclude_none=True)
        assert Stat.model_validate(dump)
    st1, st2 = stats.values()
    st = st1 + st2
    assert (
        st.received_tasks.count == st1.received_tasks.count + st2.received_tasks.count  # type: ignore
    )
    assert (
        st.execution_time.count == st1.execution_time.count + st2.execution_time.count  # type: ignore
    )


def test_stats() -> None:
    stats = {}
    for worker_pid in [11, 22]:
        stat = Stat()
        stats[worker_pid] = stat
        for val in data:
            label = random.choice(labels)
            stat.received_tasks.label(label).inc()
            stat.execution_time.label(label).aggregate(val)
    sum_stats = Stats(workers=stats)
    dump = sum_stats.model_dump(exclude_none=True)
    assert sum_stats.received_tasks.count == sum(
        [stat.received_tasks.count for stat in stats.values()],  # type: ignore
    )
    sum_stats1 = Stats.model_validate(dump)
    assert isinstance(sum_stats1, Stats)
    assert sum_stats.received_tasks.count == sum_stats1.received_tasks.count
    assert sum_stats.execution_time.count == sum_stats1.execution_time.count
    assert sum_stats.execution_time.average == sum_stats1.execution_time.average
    assert (
        sum_stats.execution_time.average  # type: ignore
        == sum_stats1.execution_time.average
        == sum(data) / len(data)
    )
