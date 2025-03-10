import pytest
from test_suite import PipelineExecutorTestSuite

import dplutils.pipeline.hyperqueue
import dplutils.pipeline.staging
from dplutils.pipeline.xpy import xpy_main


class TestClient:
    def __init__(self, name):
        self.task_id = -1
        self.job_id = 1

    def add_task(self, args, input=None, **kwargs):
        xpy_main(input)
        self.task_id += 1
        return self.task_id

    def get_tasks(self, task_ids=[]):
        return [{"id": i, "state": "finished"} for i in task_ids]

    def close_job(self):
        self.job_closed = True

    def cancel_job(self):
        self.job_cancelled = True


@pytest.fixture(autouse=True)
def client_patch(monkeypatch, tmp_path):
    monkeypatch.setattr(dplutils.pipeline.hyperqueue, "HyperQueueClient", TestClient)
    monkeypatch.setattr(dplutils.pipeline.staging, "DEFAULT_STAGING_PATH", tmp_path)


class TestHyperQueueExecutor(PipelineExecutorTestSuite):
    executor = dplutils.pipeline.hyperqueue.HyperQueueStreamExecutor
