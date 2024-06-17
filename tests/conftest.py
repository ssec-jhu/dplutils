from pathlib import Path

import pandas as pd
import pytest
import ray

from dplutils.pipeline import OutputBatch, PipelineExecutor, PipelineGraph, PipelineTask

DATA_PATH = Path(__file__).parent / "data"


@pytest.fixture
def pipelinestartdf():
    return pd.DataFrame({"run_id": ["uuid"], "batch_id": [0], "test_name": ["tst"], "test_data": ["TESTDATA"]})


@pytest.fixture(scope="session")
def raysession():
    ray.init("local", num_cpus=2, log_to_driver=False)


@pytest.fixture(scope="session")
def test_file(tmp_path_factory):
    testfile = tmp_path_factory.mktemp("testfile") / "test.txt"
    testfile.write_text("TESTDATA\n")
    return testfile


class DummyExecutor(PipelineExecutor):
    def execute(self):
        for i in range(10):
            yield OutputBatch(pd.DataFrame({"id": range(10)}), task=self.graph.sink_tasks[0].name)


@pytest.fixture
def dummy_steps():
    return [
        PipelineTask(
            "task1",
            func=lambda x: x.join(pd.DataFrame({"step1": [len(x)] * 10}), how="cross"),
        ),
        PipelineTask(
            "task2",
            func=lambda x: x.assign(step2=len(x)),
        ),
    ]


@pytest.fixture
def blackhole_step():
    return [PipelineTask("blackhole", func=lambda x: pd.DataFrame([]))]


@pytest.fixture
def dummy_pipeline_graph():
    t1 = PipelineTask("task1", lambda x: x.assign(t1="1"))
    t2A = PipelineTask("task2A", lambda x: x.assign(t2A="2A"))
    t2B = PipelineTask("task2B", lambda x: x.assign(t2B="2B"))
    t3 = PipelineTask("task3", lambda x: x.assign(t3="3"))
    return PipelineGraph([(t1, t2A), (t1, t2B), (t2A, t3), (t2B, t3)])


@pytest.fixture
def multi_output_graph():
    t1 = PipelineTask("task1", lambda x: x.assign(t1="1"))
    t2A = PipelineTask("task2A", lambda x: x.assign(t2A="2A"))
    t2B = PipelineTask("task2B", lambda x: x.assign(t2B="2B"))
    return PipelineGraph([(t1, t2A), (t1, t2B)])


@pytest.fixture
def graph_suite(dummy_steps, dummy_pipeline_graph, multi_output_graph):
    return {
        "dummy_steps": dummy_steps,
        "dummy_pipeline_graph": dummy_pipeline_graph,
        "multi_output_graph": multi_output_graph,
    }


@pytest.fixture
def generic_task():
    def func(dataframe, optional=1):
        pass

    return PipelineTask("name", func)


@pytest.fixture
def generic_task_with_required():
    def func(dataframe, required, optional=1):
        pass

    return PipelineTask("name", func)


@pytest.fixture
def dummy_executor(dummy_steps):
    return DummyExecutor(graph=dummy_steps)
