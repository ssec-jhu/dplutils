import sys
from abc import abstractmethod
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import cloudpickle
import pandas as pd
import pyarrow.parquet as pq

from dplutils.pipeline.executor import OutputBatch
from dplutils.pipeline.staging import FileStager
from dplutils.pipeline.stream import StreamBatch, StreamingGraphExecutor
from dplutils.pipeline.task import PipelineTask
from dplutils.pipeline.utils import calculate_splits, split_dataframe


@dataclass
class XPyRemoteBundle:
    function: callable
    kwargs: dict
    input_files: list[Path | pd.DataFrame]
    output_files: list[Path]


@dataclass
class XPyTask:
    task: PipelineTask
    submitted_task_info: Any
    input_files: list[Path]
    output_files: list[Path]


XPY_CMD = [sys.executable, "-m", __name__]


class XPyStreamExecutor(StreamingGraphExecutor):
    """Executor for cross-python execution.

    The executor is designed to work by submitting pickled batches to a python
    method (provided as a module-level main function) in another interpreter
    passing pickled data via stdin and communicating inputs and results and as
    files. It is abstract in how the execution command is scheduled,
    implementations must provide the mechanism to invoke the required command.
    """

    def __init__(self, *args, staging_path=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.staging_path = staging_path

    def pre_execute(self):
        self.stager = FileStager(self.staging_path or f"xpy-staging-{self.run_id}")

    def output_batch_transform(self, batch: OutputBatch) -> OutputBatch:
        df = pd.read_parquet(batch.data)
        self.stager.mark_complete(batch.data)
        return replace(batch, data=df)

    @abstractmethod
    def submit_task_execution(self, task: PipelineTask, cmd: list[str], pickled_bundle: bytes) -> Any:
        """Submit the requested command for execution to underlying scheduler.

        The pickled bundle is a cloudpickle'd :py:class:`XPyRemoteBundle`
        instance which must be unpacked and executed as per the provided
        `xpy_main` function (which see). The command in cmd provides a list of
        arguments that can be used to perform that call via subprocess or other
        means.

        If the provided `task` argument is None, the task represents a split.

        The return value can be anything, but should represent a handle to the
        task in whatever scheduling system is used. It will be available to the
        other methods, such as `task_resolve_output` and `poll_tasks` in the
        `submitted_task_info` field of the `XPyTask` given as arguments.
        """
        pass

    def task_submit(self, task: PipelineTask, df_list):
        bundle = XPyRemoteBundle(
            function=task.func,
            kwargs=task.resolve_kwargs(self.ctx),
            input_files=df_list,
            output_files=[self.stager.get(task.name)],
        )
        pickled_bundle = cloudpickle.dumps(bundle)
        cmd = XPY_CMD + [task.name]
        task_info = self.submit_task_execution(task, cmd, pickled_bundle)
        disk_input_files = [i for i in df_list if isinstance(i, Path)]
        return XPyTask(
            task=task, submitted_task_info=task_info, input_files=disk_input_files, output_files=bundle.output_files
        )

    def split_batch_submit(self, batch: StreamBatch, max_rows: int):
        num_splits = calculate_splits(batch.length, max_rows)
        output_files = self.stager.get("split", num=num_splits)
        bundle = XPyRemoteBundle(
            function=split_dataframe,
            kwargs={"num_splits": num_splits},
            input_files=[batch.data],
            output_files=output_files,
        )
        cmd = XPY_CMD + ["split"]
        task_info = self.submit_task_execution(None, cmd, cloudpickle.dumps(bundle))
        return XPyTask(task=None, submitted_task_info=task_info, input_files=[batch.data], output_files=output_files)

    def task_resolve_output(self, task: XPyTask):
        for i in task.input_files:
            self.stager.mark_complete(i)
        outs = []
        for output in task.output_files:
            n_consumers = self.graph.out_degree(task.task) if task.task else 1
            self.stager.mark_usage(output, n=n_consumers)
            output_len = pq.read_metadata(output).num_rows
            outs.append(StreamBatch(length=output_len, data=output))
        return outs if len(outs) > 1 else outs[0]


def xpy_run_task(bundle: XPyRemoteBundle):
    """Run the task specified in the bundle.

    This handles the reading and concatenation of input files/dataframes and
    writing of function output to corresponding output file from the bundle.
    `xpy_main` can be used to run this from a pickled bundle and is the typical
    target for remote calls.
    """
    for i in range(len(bundle.input_files)):
        # The input files can also be dataframes directly (e.g. in the case of
        # source batches where the driver has already in-memory) or a mix.
        if isinstance(bundle.input_files[i], Path):
            bundle.input_files[i] = pd.read_parquet(bundle.input_files[i])
    df_in = pd.concat(bundle.input_files)

    df_out = bundle.function(df_in, **bundle.kwargs)

    # output files would be more than one in the case of split, where we require
    # each element to correspond with an output created by the function (hence
    # strict=True on zip)
    if len(bundle.output_files) > 1:
        for df, of in zip(df_out, bundle.output_files, strict=True):
            df.to_parquet(of)
    else:
        df_out.to_parquet(bundle.output_files[0])


def xpy_main(data: bytes):
    """Decode the cloudpickle'd data and run the task.

    This call is made available as a module-level script entry point for running
    the provided task given as a bundle via stdin. The function specified in the
    bundle will be run against the input data and the output will be written as
    a parquet file(s) to the location(s) specified. If the bundle specifies
    multiple outputs, the function must return that number of outputs as an
    iterable.
    """
    bundle = cloudpickle.loads(data)
    xpy_run_task(bundle)


if __name__ == "__main__":
    xpy_main(sys.stdin.buffer.read())
