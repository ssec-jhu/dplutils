from collections import defaultdict

import pandas as pd
import pytest
from test_suite import PipelineExecutorTestSuite

from dplutils.pipeline import PipelineTask
from dplutils.pipeline.stream import LocalSerialExecutor, StreamTask


class TestStreamingExecutorDefault(PipelineExecutorTestSuite):
    executor = LocalSerialExecutor


def test_stream_task_wrapper():
    st = StreamTask(PipelineTask("task_name", lambda x: 1))
    assert st.name == "task_name"
    assert hash(st)  # ensure it is hashable
    assert st.total_pending() == 0
    st.data_in.append(1)
    st.pending.append(1)
    st.split_pending.append(1)
    assert st.total_pending() == 3


def test_stream_exhausted_indicator_considers_splits(dummy_steps):
    pl = LocalSerialExecutor(dummy_steps)
    a_task = pl.stream_graph.task_map["task2"]
    assert pl.task_exhausted(a_task)
    a_task.split_pending.append(1)
    assert not pl.task_exhausted(a_task)


@pytest.mark.parametrize("max_batches", [1, 10, None])
def test_stream_executor_generator_override(max_batches):
    st = PipelineTask("task_name", lambda x: x)

    def generator():
        n = 12
        for i in range(n):
            yield pd.DataFrame({"customgen": [i]})

    pl = LocalSerialExecutor([st], max_batches=max_batches, generator=generator)
    res = list(i.data for i in pl.run())
    expected_rows = max_batches if max_batches else 12
    assert len(res) == expected_rows
    assert pd.concat(res).customgen.to_list() == list(range(expected_rows))


@pytest.mark.parametrize("gen_n", [0, 4])
def test_stream_executor_exhausts_input_when_source_batchsize_larger_than_input(gen_n):
    st = PipelineTask("task_name", lambda x: x, batch_size=10)

    def generator():
        n = gen_n
        for i in range(n):
            yield pd.DataFrame({"customgen": [i]})

    pl = LocalSerialExecutor([st], generator=generator)
    res = [i.data for i in pl.run()]
    if gen_n > 0:
        assert len(res) == 1
        assert len(res[0]) == gen_n
    else:
        # do not submit empty source batches
        assert len(res) == 0


def test_stream_executor_input_batch_size_splits(dummy_steps):
    def generator():
        for i in range(4):
            yield pd.DataFrame({"col": range(2)})

    # sanity check to ensure below test is actually inspect the split action
    pl = LocalSerialExecutor(dummy_steps, generator=generator)
    res = [i.data for i in pl.run()]
    assert len(res) == 4
    # explicitly set batch size so we should call split on each input
    pl = LocalSerialExecutor(dummy_steps, generator=generator).set_config("task1.batch_size", 1)
    res = [i.data for i in pl.run()]
    assert len(res) == 8


def test_stream_source_round_robin_is_strict():
    # a source that cannot be submitted must block the sources behind it,
    # otherwise one with a smaller request would take all the capacity
    class MyExec(LocalSerialExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.counts = defaultdict(int)
            self.blocked = "big"

        def task_submit(self, task, data):
            self.counts[task.name] += 1
            return super().task_submit(task, data)

        def task_submittable(self, t, rank):
            return t.name != self.blocked

    big = PipelineTask("big", lambda x: x)
    small = PipelineTask("small", lambda x: x)
    sink = PipelineTask("sink", lambda x: x)
    p = MyExec([(big, sink), (small, sink)], max_batches=4)
    p.n_sourced = 0
    p.source_exhausted = False
    p.source_generator = p.generator_fun()
    p.enqueue_tasks()
    # the blocked source sorts first (neither has been submitted), so the other
    # must not run ahead of it
    assert p.counts["big"] == p.counts["small"] == 0
    p.blocked = None
    p.enqueue_tasks()
    assert p.counts["big"] > 0 and p.counts["small"] > 0


def test_stream_source_with_pending_split_holds_rotation():
    # a source still waiting on a split from a previous pass must neither be
    # fed again nor let the sources behind it get ahead
    one = PipelineTask("one", lambda x: x, batch_size=1)
    two = PipelineTask("two", lambda x: x)
    sink = PipelineTask("sink", lambda x: x)
    p = LocalSerialExecutor([(one, sink), (two, sink)], max_batches=10)
    p.n_sourced = 0
    p.source_exhausted = False
    p.source_generator = p.generator_fun()
    p.stream_graph.task_map["one"].split_pending.append("in flight")

    p.enqueue_tasks()
    assert p.n_sourced == 0  # nothing fed from the generator
    assert all(i.counter == 0 for i in p.stream_graph)  # and nothing submitted


@pytest.mark.parametrize("rows_per_input", [1, 10])
def test_stream_source_cadence_follows_batch_size(rows_per_input):
    # sources with differing batch size should submit in inverse proportion to
    # it, and interleave as they go rather than one lagging the other
    class MyExec(LocalSerialExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.submissions = []

        def task_submit(self, task, data):
            if task in self.graph.source_tasks:
                self.submissions.append(task.name)
            return super().task_submit(task, data)

    def generator():
        for i in range(20):
            yield pd.DataFrame({"run_id": ["r"] * rows_per_input, "id": range(rows_per_input)})

    one = PipelineTask("one", lambda x: x, batch_size=1)
    ten = PipelineTask("ten", lambda x: x, batch_size=10)
    sink = PipelineTask("sink", lambda x: x)
    p = MyExec([(one, sink), (ten, sink)], max_batches=20, generator=generator)
    list(p.run())

    n_rows = 20 * rows_per_input
    assert p.submissions.count("one") == n_rows  # one row at a time
    assert p.submissions.count("ten") == n_rows / 10  # ten at a time
    # the larger batch must not lag: at most its own batch worth of the smaller
    # one goes by between its submissions
    runs = "".join("ot"[i == "ten"] for i in p.submissions).split("t")
    assert max(len(i) for i in runs) <= 10


def test_stream_submission_ordering_evaluation_priority():
    # tracking class adds counts and a parallel submission which allows us to
    # locally test that the re-prioritization during submission is working. If
    # so, we should expect terminal tasks having even numbers of calls and being
    # preferred (as opposed to submitting n parallel all to one task, or
    # submitting some to upstream tasks).
    class MyExec(LocalSerialExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.counts = defaultdict(int)
            self.parallel_submissions = 4
            self.n_parallel = 0

        def task_submit(self, task, data):
            self.counts[task.name] += 1
            self.n_parallel += 1
            return super().task_submit(task, data)

        def task_submittable(self, t, rank):
            if t in self.graph.source_tasks:
                return True
            return self.n_parallel < self.parallel_submissions

        def poll_tasks(self, pending):
            self.n_parallel = 0

    a = PipelineTask("a", lambda x: x, batch_size=16)
    b = a("b", batch_size=1)
    (c, d, e) = (b("c"), b("d"), b("e"))
    # graph with multiple terminals. The large input batch size ensures we have
    # work to submit in parallel to exercise the re sorting logic.
    p = MyExec([(a, b), (b, c), (c, d), (c, e)], max_batches=16)
    p_run = p.run()
    _ = [next(p_run) for _ in range(4)]  # pop number based on parallel submissions
    assert p.counts["d"] == p.counts["e"] == 2  # terminals should have even counts
    assert p.counts["b"] == p.counts["c"] == 4  # only just enough to submit 4
    _ = [next(p_run) for _ in range(4)]
    assert p.counts["d"] == p.counts["e"] == 4  # we finish the 4 batch size
    assert p.counts["b"] == p.counts["c"] == 4  # but do no more upstream work
    _ = [next(p_run) for _ in range(4)]
    assert p.counts["d"] == p.counts["e"] == 6  # need to get more work, as above
    assert p.counts["b"] == p.counts["c"] == 8  # more upstream for that work
