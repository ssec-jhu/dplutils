import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import cloudpickle
import pandas as pd
import pyarrow.parquet as pq

from dplutils.pipeline.stream import StreamBatch, StreamingGraphExecutor
from dplutils.pipeline.task import PipelineTask
from dplutils.pipeline.utils import calculate_splits, split_dataframe
from dplutils.pipeline.xpy import XPyStreamExecutor, XPyTask


class HyperQueueClient:
    def __init__(self, binary="hq", name=None, stream=None, auto_open=True):
        self.binary = binary
        self.stream = stream
        self.name = name
        if auto_open:
            self._open_job()

    def _do_hq(self, args, input=None):
        cmd = [self.binary, "--output-mode=json"] + args
        try:
            call_output = subprocess.check_output(cmd, input=input)
        except subprocess.CalledProcessError as exc:
            output = exc.output.decode()
            raise ValueError(f"Error in hyperqueue command: {output}") from exc
        json_starts = [call_output.find(b"{"), call_output.find(b"[")]
        json_start = min(i for i in json_starts if i >= 0)
        return json.loads(call_output[json_start:])

    @property
    def job_id(self):
        if self._job_id is None:
            raise ValueError("Job not open")
        return self._job_id

    def _open_job(self):
        self.stream = self.stream or tempfile.mkdtemp()
        self.name = self.name or Path(self.stream).name
        self.task_counter = -1
        self._job_id = self._do_hq(["job", "open", "--name", self.name])["id"]

    def close_job(self):
        self._do_hq(["job", "close", str(self.job_id)])

    def cancel_job(self):
        self._do_hq(["job", "cancel", str(self.job_id)])

    def add_task(self, args, input=None, priority=0, resources={}):
        task_cmd = ["submit", f"--job={self.job_id}", f"--stream={self.stream}", f"--priority={priority}"]
        if input is not None:
            task_cmd += ["--stdin"]
        for resource, request in resources.items():
            if request is None:
                continue
            if resource == "gpus":
                resource = "gpus/nvidia"
            task_cmd += [f"--resource={resource}={request}"]
        task_cmd += args
        self._do_hq(task_cmd, input=input)
        # this means we are not thread safe and tasks cannot be added to the
        # same job from outside this client
        self.task_counter += 1
        return self.task_counter

    def get_tasks(self, task_ids=[]):
        cmd = ["task", "list", str(self.job_id)]
        if task_ids:
            cmd += ["--tasks", ",".join(str(i) for i in task_ids)]
        return self._do_hq(cmd)[str(self.job_id)]


class HyperTaskQueue:
    def __init__(self):
        self.finished = set()
        self.running = set()
        self.sourced = set()


class HyperQueueStreamExecutor(XPyStreamExecutor):
    def __init__(self, *args, poll_interval=2, **kwargs):
        super().__init__(*args, **kwargs)
        self.poll_interval = poll_interval

    def pre_execute(self):
        super().pre_execute()
        self.hq_job = HyperQueueClient(name=self.run_id)
        self.task_queue = HyperTaskQueue()
        self._task_ranks = self.graph.rank_nodes()

    def submit_task_execution(self, task: PipelineTask, cmd: list[str], pickled_bundle: bytes):
        if task is None:  # split task
            return self.hq_job.add_task(cmd, input=pickled_bundle, priority=2)
        resources = {"cpus": task.num_cpus, "gpus": task.num_gpus, **task.resources}
        priority = 1 - self._task_ranks[task]
        task_id = self.hq_job.add_task(cmd, input=pickled_bundle, priority=priority, resources=resources)
        if task in self.graph.source_tasks:
            self.task_queue.sourced.add(task_id)
        return task_id

    def is_task_ready(self, task: XPyTask):
        if task.submitted_task_info in self.task_queue.finished:
            return True

    def task_submittable(self, task: PipelineTask, rank: int) -> bool:
        # In the case of sources, we need to prevent infinite queueing here, so
        # we submit thing so long as there are at least some waiting. Priority
        # should prevent them from taking over.
        if task in self.graph.source_tasks:
            sources_waiting = len(self.task_queue.sourced - self.task_queue.running)
            if sources_waiting > 10:
                return False
        # Otherwise we just queue with priority related to distance from the
        # sink so tasks are always submittable
        return True

    def task_resolve_output(self, task):
        if task.task in self.graph.source_tasks:
            self.task_queue.sourced.remove(task.submitted_task_info)
        return super().task_resolve_output(task)

    def poll_tasks(self, pending_task_list: list[XPyTask]):
        task_ids = [task.submitted_task_info for task in pending_task_list]
        states = self.hq_job.get_tasks(task_ids=task_ids)
        self.task_queue.finished.clear()
        self.task_queue.running.clear()
        for state in states:
            if state["state"] == "failed":
                raise RuntimeError(f"Task {state['id']} failed")
            elif state["state"] == "finished":
                self.task_queue.finished.add(state["id"])
            elif state["state"] == "running":
                self.task_queue.running.add(state["id"])
        if self.task_queue.finished:
            return
        time.sleep(self.poll_interval)
