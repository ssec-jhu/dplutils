import json
import subprocess
import time
from unittest.mock import MagicMock

import pytest
from test_suite import PipelineExecutorTestSuite

import dplutils.pipeline.hyperqueue
import dplutils.pipeline.staging
from dplutils.pipeline.xpy import xpy_main


def hq_subprocess_check_output_mock(cmd, *args, input=None, task_state="finished", **kwargs):
    cmd_string = " ".join(cmd)
    if cmd_string.startswith("hq --output-mode=json job open"):
        return b'{"id": 1}'
    elif cmd_string.startswith("hq --output-mode=json submit"):
        xpy_main(input)
        return b"blah {}"
    elif cmd_string.startswith("hq --output-mode=json task list"):
        ids = cmd[6].split(",")
        return json.dumps({"1": [{"id": int(i), "state": task_state} for i in ids]}).encode()
    return b"{}"


@pytest.fixture(autouse=True)
def client_patch(monkeypatch, tmp_path):
    # monkeypatch.setattr(dplutils.pipeline.hyperqueue, "HyperQueueClient",
    # TestClient)
    monkeypatch.setattr(subprocess, "check_output", hq_subprocess_check_output_mock)
    monkeypatch.setattr(dplutils.pipeline.staging, "DEFAULT_STAGING_PATH", tmp_path)


class TestHyperQueueExecutor(PipelineExecutorTestSuite):
    executor = dplutils.pipeline.hyperqueue.HyperQueueStreamExecutor


@pytest.mark.parametrize("task_state", ["running", "failed"])
def test_executor_errors_for_task_states(monkeypatch, task_state, dummy_steps):
    def mock_fail(*args, **kwargs):
        return hq_subprocess_check_output_mock(*args, task_state=task_state, **kwargs)

    monkeypatch.setattr(subprocess, "check_output", mock_fail)

    # in the case of running, stop the sleep so we don't go on forever
    class STOP(Exception):
        pass

    def terminate_sleep(*args, **kwargs):
        raise STOP()

    monkeypatch.setattr(time, "sleep", terminate_sleep)
    pl = dplutils.pipeline.hyperqueue.HyperQueueStreamExecutor(dummy_steps, poll_interval=0)
    if task_state == "running":
        with pytest.raises(STOP):
            next(pl.run())
        assert 0 in pl.task_queue.running  # ensure task added to running queue
    elif task_state == "failed":
        with pytest.raises(RuntimeError, match="Task .* failed"):
            next(pl.run())


def test_hq_client_proc_error(monkeypatch):
    def proc_raise_error(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "cmd", output=b"error")

    monkeypatch.setattr(subprocess, "check_output", proc_raise_error)
    with pytest.raises(ValueError):
        dplutils.pipeline.hyperqueue.HyperQueueClient(name="test")


def test_hq_client_job_not_open():
    client = dplutils.pipeline.hyperqueue.HyperQueueClient(name="test", auto_open=False)
    with pytest.raises(ValueError):
        client.add_task([])


def test_hq_client_close_open_job(monkeypatch):
    client = dplutils.pipeline.hyperqueue.HyperQueueClient(name="test")
    mock = MagicMock()
    monkeypatch.setattr(client, "_do_hq", mock)
    client.close_job()
    assert mock.call_args[0][0][0:2] == ["job", "close"]
    client.cancel_job()
    assert mock.call_args[0][0][0:2] == ["job", "cancel"]


def test_hq_replaces_bare_gpu_resource(monkeypatch):
    client = dplutils.pipeline.hyperqueue.HyperQueueClient(name="test")
    mock = MagicMock()
    monkeypatch.setattr(client, "_do_hq", mock)
    client.add_task([], resources={"gpus": 1})
    assert "--resource=gpus/nvidia=1" in mock.call_args[0][0]
