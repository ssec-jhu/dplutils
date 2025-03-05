import pytest
from test_suite import PipelineExecutorTestSuite

import dplutils.pipeline.hyperqueue


class TestClient:
    def __init__(self, name):
        pass
        self.task_id = -1

    def add_task(self, args, input=None, **kwargs):
        dplutils.pipeline.hyperqueue.hyperqueue_main(input)
        self.task_id += 1
        return self.task_id

    def get_tasks(self, task_ids=[]):
        return [{"id": i, "state": "completed"} for i in task_ids]

    def close_job(self):
        self.job_closed = True


@pytest.fixture(autouse=True)
def client_patch(monkeypatch, tmp_path):
    monkeypatch.setattr(dplutils.pipeline.hyperqueue, "HyperQueueClient", TestClient)
    monkeypatch.setattr(dplutils.pipeline.hyperqueue, "DEFAULT_STAGING_PATH", str(tmp_path))


class TestHyperQueueExecutor(PipelineExecutorTestSuite):
    executor = dplutils.pipeline.hyperqueue.HyperQueueStreamExecutor
