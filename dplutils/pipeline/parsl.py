from concurrent.futures import wait
from dataclasses import dataclass
import os
from typing import Any
import uuid
import cloudpickle
import numpy as np
import pandas as pd
import parsl
from parsl.dataflow.futures import AppFuture
from dplutils.pipeline.stream import StreamBatch, StreamingGraphExecutor
from dplutils.pipeline.task import PipelineTask
from dplutils.pipeline.utils import split_dataframe


def _get_app_wrapper(task, ctx):
    pickled = cloudpickle.dumps(task.func)
    kwargs = task.resolve_kwargs(ctx)
    def wrapper(inputs, outputs):
        # convert inputs from file(s) (parquet) to df
        func = cloudpickle.loads(pickled)
        in_df = pd.concat([pd.read_parquet(i) for i in inputs])
        out_df = func(in_df, **kwargs)
        # write out to parquet in outputs[0]
        out_df.to_parquet(outputs[0])
        return len(out_df)
    return parsl.python_app(wrapper)


@parsl.python_app
def splitter(inputs, outputs):
    df_in = pd.read_parquet(inputs[0])
    df_splits = split_dataframe(df_in, num_splits=len(outputs))
    for df_out, file in zip(df_splits, outputs):
        df_out.to_parquet(file)
    return [len(i) for i in df_splits]


@dataclass
class ParslTracker:
    future: AppFuture
    inputs: list[parsl.File]
    outputs: list[parsl.File]


class ParslHTStreamExecutor(StreamingGraphExecutor):
    def _setup_remotes(self):
        self.remotes = {
            name: _get_app_wrapper(task, self.ctx) for name, task in self.tasks_idx.items()
        }

    def execute(self):
        self._setup_remotes()
        for batch in super().execute():
            batch.data = pd.read_parquet(batch.data[0])
            yield batch

    def task_submittable(self, task: PipelineTask, rank: int) -> bool:
        dfk = parsl.dfk()
        eligible_executors = [e for e in dfk.config.executors if isinstance(e, parsl.HighThroughputExecutor)]
        executor = eligible_executors[0]
        return executor.outstanding <= executor.connected_workers

    def task_submit(self, task: PipelineTask, df_list: list[parsl.File]) -> Any:
        out_file = parsl.File(f"_dpl_parsl_-{task.name}-out-{uuid.uuid1()}.par")
        inputs = []
        for i, df in enumerate(df_list):
            print(f'processing submission from df_list, {i}: {df}')
            if isinstance(df, pd.DataFrame):
                fname = f"_dpl_parsl_-source-{i}-{uuid.uuid1()}.par"
                df.to_parquet(fname)
                df = parsl.File(fname)
                inputs.append(df)
            else:
                inputs.extend(df)
        app_future = self.remotes[task.name](inputs=inputs, outputs=[out_file])
        return ParslTracker(future=app_future, inputs=inputs, outputs=[out_file])

    def is_task_ready(self, pending_task: Any) -> bool:
        return pending_task.future.done()

    def task_resolve_output(self, pending_task: Any) -> StreamBatch:
        result = pending_task.future.result()
        if len(pending_task.outputs) > 1:
            return [StreamBatch(length=result[i], data=[o]) for i,o in enumerate(pending_task.outputs)]
        return StreamBatch(length=result, data=pending_task.outputs)

    def split_batch_submit(self, batch: StreamBatch, max_rows: int) -> Any:
        # need to know the number of output files up front
        n_out = int(np.ceil(batch.length / max_rows))
        outputs = [parsl.File(f"_dpl_parsl_-split-{i}-{uuid.uuid1()}.par") for i in range(n_out)]
        app_future = splitter(inputs=batch.data, outputs=outputs)
        return ParslTracker(future=app_future, inputs=batch.data, outputs=outputs)

    def poll_tasks(self, pending_task_list: list[Any]) -> None:
        futures = [i.future for i in pending_task_list]
        wait(futures, timeout=10)
