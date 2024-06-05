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
