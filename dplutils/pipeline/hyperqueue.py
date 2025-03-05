import json
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import cloudpickle
import pandas as pd
import pyarrow.parquet as pq

from dplutils.pipeline.stream import StreamBatch, StreamingGraphExecutor
from dplutils.pipeline.task import PipelineTask
from dplutils.pipeline.utils import calculate_splits, split_dataframe


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

    def add_task(self, args, input=None, priority=0, resources={}):
        task_cmd = ["submit", "--job", str(self.job_id), "--stream", self.stream, "--priority", str(priority)]
        if input is not None:
            task_cmd += ["--stdin"]
        for resource, request in resources.items():
            task_cmd += ["--resource", f"{resource}={request}"]
        task_cmd += args
        self._do_hq(task_cmd, input=input)
        self.task_counter += 1  # this means we are not thread safe
        return self.task_counter

    def get_tasks(self, task_ids=[]):
        cmd = ["task", "list", str(self.job_id)]
        if task_ids:
            cmd += ["--tasks", ",".join(str(i) for i in task_ids)]
        return self._do_hq(cmd)


@dataclass
class HyperQueueRemoteBundle:
    function: callable
    kwargs: dict
    input_files: list[Path | pd.DataFrame]
    output_files: list[Path]


@dataclass
class HyperQueueTask:
    task: PipelineTask
    hq_task_id: int
    input_files: list[Path]
    output_files: list[Path]


class FileStager:
    def __init__(self, staging_root):
        self.staging_root = Path(staging_root)
        self.staging_root.mkdir(exist_ok=True, parents=True)

    def get(self, name, num=1):
        tag = f"{name}-{uuid.uuid1()}"
        if num > 1:
            return [self.staging_root / f"{tag}-{i}.par" for i in range(num)]
        return self.staging_root / f"{tag}.par"

    def mark_usage(self, file, n=1):
        pass

    def mark_complete(self, file):
        pass


DEFAULT_STAGING_PATH = tempfile.tempdir


class HyperQueueStreamExecutor(StreamingGraphExecutor):
    def __init__(self, *args, poll_interval=2, staging_path=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.poll_interval = poll_interval
        self.staging_path = Path(staging_path or DEFAULT_STAGING_PATH)

    def _setup_hq(self):
        self.hq_job = HyperQueueClient(name=self.run_id)
        self.hq_completion_queue = set()
        self.filestager = FileStager(self.staging_path / f"hq-staging-{self.run_id}")

    def execute(self):
        self._setup_hq()
        for batch in super().execute():
            batch.data = pd.read_parquet(batch.data)
            yield batch
        self.hq_job.close_job()

    def is_task_ready(self, task):
        if task.hq_task_id in self.hq_completion_queue:
            return True

    def task_submittable(self, task: PipelineTask, rank: int) -> bool:
        # We just queue with priority related to distance from the sink so tasks
        # are always submittable
        # TODO: however, we need to ensure that we don't create infinite source
        # tasks which would otherwise happen if they were always eligible!!
        return True

    def task_submit(self, task: PipelineTask, df_list):
        bundle = HyperQueueRemoteBundle(
            function=task.func,
            kwargs=task.resolve_kwargs(self.ctx),
            input_files=df_list,
            output_files=[self.filestager.get(task.name)],
        )
        pickled_bundle = cloudpickle.dumps(bundle)
        resources = {"cpus": task.num_cpus, "gpus": task.num_gpus, **task.resources}
        task_id = self.hq_job.add_task(
            [sys.executable, "-m", "dplutils.pipeline.hyperqueue"], input=pickled_bundle, resources=resources
        )
        disk_input_files = [i for i in df_list if isinstance(i, Path)]
        return HyperQueueTask(
            task=task, hq_task_id=task_id, input_files=disk_input_files, output_files=bundle.output_files
        )

    def split_batch_submit(self, batch: StreamBatch, max_rows: int):
        num_splits = calculate_splits(batch.length, max_rows)
        output_files = self.filestager.get("split", num=num_splits)
        bundle = HyperQueueRemoteBundle(
            function=split_dataframe,
            kwargs={"num_splits": num_splits},
            input_files=[batch.data],
            output_files=output_files,
        )
        task_id = self.hq_job.add_task(
            [sys.executable, "-m", "dplutils.pipeline.hyperqueue"], input=cloudpickle.dumps(bundle)
        )
        return HyperQueueTask(task=None, hq_task_id=task_id, input_files=[batch.data], output_files=output_files)

    def task_resolve_output(self, task):
        for i in task.input_files:
            self.filestager.mark_complete(i)
        outs = []
        for output in task.output_files:
            self.filestager.mark_usage(output, n=self.graph.out_degree(task.task))
            output_len = pq.read_metadata(output).num_rows
            outs.append(StreamBatch(length=output_len, data=output))
        return outs if len(outs) > 1 else outs[0]

    def poll_tasks(self, pending_task_list):
        task_ids = [task.hq_task_id for task in pending_task_list]
        states = self.hq_job.get_tasks(task_ids=task_ids)
        self.hq_completion_queue.clear()
        for state in states:
            if state["state"] == "completed":
                self.hq_completion_queue.add(state["id"])
            if state["state"] == "failed":
                raise ValueError(f"Task {state['id']} failed")
        if self.hq_completion_queue:
            return
        time.sleep(self.poll_interval)


def hyperqueue_task_run(bundle):
    for i in range(len(bundle.input_files)):
        if isinstance(bundle.input_files[i], Path):
            bundle.input_files[i] = pd.read_parquet(bundle.input_files[i])
    df_in = pd.concat(bundle.input_files)
    df_out = bundle.function(df_in, **bundle.kwargs)
    if len(bundle.output_files) > 1:
        for df, of in zip(df_out, bundle.output_files):
            df.to_parquet(of)
    else:
        df_out.to_parquet(bundle.output_files[0])


def hyperqueue_main(data):
    bundle = cloudpickle.loads(data)
    hyperqueue_task_run(bundle)


if __name__ == "__main__":
    hyperqueue_main(sys.stdin.buffer.read())
