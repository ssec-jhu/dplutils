from dplutils.pipeline import PipelineTask
from dplutils.pipeline.stream import LocalSerialExecutor, StreamTask
from test_suite import PipelineExecutorTestSuite


class TestStreamingExecutorDefault(PipelineExecutorTestSuite):
    executor = LocalSerialExecutor


def test_stream_task_wrapper():
    st = StreamTask(PipelineTask('task_name', lambda x: 1))
    assert st.name == 'task_name'
    assert hash(st)  # ensure it is hashable
    assert st.total_pending() == 0
    st.data_in.append(1)
    st.pending.append(1)
    st.split_pending.append(1)
    assert st.total_pending() == 3


def test_stream_exhausted_indicator_considers_splits(dummy_steps):
    pl = LocalSerialExecutor(dummy_steps)
    a_task = pl.stream_graph.task_map['task2']
    assert pl.task_exhausted(a_task)
    a_task.split_pending.append(1)
    assert not pl.task_exhausted(a_task)
