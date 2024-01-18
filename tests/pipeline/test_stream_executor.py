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
