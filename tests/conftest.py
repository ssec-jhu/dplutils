import pandas as pd
import pytest
import ray
from pathlib import Path
from dplutils.pipeline import PipelineTask, PipelineExecutor


DATA_PATH = Path(__file__).parent / "data"


@pytest.fixture
def pipelinestartdf():
    return pd.DataFrame({'run_id': ['uuid'], 'batch_id': [0], 'test_name': ['tst'], 'test_data': ['TESTDATA']})


@pytest.fixture(scope='session')
def raysession():
    ray.init('local', num_cpus=2, log_to_driver=False)


@pytest.fixture(scope="session")
def test_file(tmp_path_factory):
    testfile = tmp_path_factory.mktemp("testfile") / "test.txt"
    testfile.write_text('TESTDATA\n')
    return testfile


class DummyExecutor(PipelineExecutor):
    def execute(self):
        for i in range(10):
            yield pd.DataFrame({'id': range(10)})


@pytest.fixture
def dummy_steps():
    return [
        PipelineTask(
            'task1',
            func=lambda x: x.join(pd.DataFrame({'step1': [len(x)] * 10}), how='cross'),
        ),
        PipelineTask(
            'task2',
            func=lambda x: x.assign(step2 = len(x)),
        )
    ]


@pytest.fixture
def generic_task():
    def func(dataframe, optional = 1):
        pass
    return PipelineTask('name', func)


@pytest.fixture
def generic_task_with_required():
    def func(dataframe, required, optional = 1):
        pass
    return PipelineTask('name', func)


@pytest.fixture
def dummy_executor(dummy_steps):
    return DummyExecutor(graph = dummy_steps)
