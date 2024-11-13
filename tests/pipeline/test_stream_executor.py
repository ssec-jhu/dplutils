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
