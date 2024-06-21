from collections import defaultdict
from copy import copy
from dataclasses import dataclass
from itertools import chain

import numpy as np
import pandas as pd
import ray

from dplutils import observer
from dplutils.pipeline import OutputBatch, PipelineExecutor, PipelineTask
from dplutils.pipeline.stream import StreamBatch, StreamingGraphExecutor
from dplutils.pipeline.utils import split_dataframe


def set_run_id(inputs, run_id):
    inputs.rename(columns={"id": "batch_id"}, inplace=True)
    inputs["run_id"] = run_id
    return inputs


def get_remote_wrapper(task: PipelineTask, context: dict):
    obs = observer.get_observer()

    def funcwrapper(indf, **kwargs):
        observer.set_observer(obs)
        return task.func(indf, **kwargs)

    def wrapper(indf):
        kwargs = task.resolve_kwargs(context)

        if task.batch_size is None:
            return funcwrapper(indf, **kwargs)

        splits = split_dataframe(indf, max_rows=task.batch_size)
        refs = [
            ray.remote(funcwrapper)
            .options(
                num_cpus=task.num_cpus,
                num_gpus=task.num_gpus,
                resources=task.resources,
            )
            .remote(i, **kwargs)
            for i in splits
        ]
        return pd.concat(ray.get(refs))

    # Ray data uses the function name to name the underlying remote tasks, override for the wrapper for better
    # observability
    wrapper.__name__ = f"{task.name}<{task.func.__name__}>"
    return wrapper


class RayDataPipelineExecutor(PipelineExecutor):
    """Executor using ray datasets pipelines.

    Ray datasets can execute a pipeline represented as a simple graph across a
    distributed cluster. This executor feeds tasks using the range source to
    mock an infinite stream.

    Some limitations of this executor:

    - Task ``batch_size`` is interpreted as a hard requirement and will split
      the workload into separate remote tasks, however it will block until all
      complete in order to return a merged result. The primary reason for this
      is ray data lacks streaming repartitioning support, but we want to be able
      to maximize (particularly GPU) utilization.
    - There is no ability to pause and resume, so batch sizes should be tuned to
      capture acceptable fault tolerance.

    args:
        graph: task graph, see
          :class:`PipelineExecutor<dplutils.pipeline.executor.PipelineExecutor>`.
        n_batches: total number of batches to feed using range source.
        **kwargs: kwargs passed to
          :class:`PipelineExecutor<dplutils.pipeline.executor.PipelineExecutor>`.
    """

    def __init__(self, graph, n_batches: int = 100, **kwargs):
        super().__init__(graph, **kwargs)
        self.tasks = self.graph.to_list()
        self.n_batches = n_batches

    def make_pipeline(self):
        pipeline = ray.data.range(self.n_batches, parallelism=self.n_batches).map_batches(
            set_run_id, batch_format="pandas", fn_kwargs={"run_id": self.run_id}
        )
        for task in self.tasks:
            ray_args = dict()
            if task.batch_size is None:
                # batch size set triggers the wrapper to run remote functions and resources must be set there,
                # map_batches task would get default of 1 cpu (could be 0?)
                ray_args = dict(
                    num_cpus=task.num_cpus,
                    num_gpus=task.num_gpus,
                    resources=task.resources,
                )
            pipeline = pipeline.map_batches(
                get_remote_wrapper(task, self.ctx),
                batch_format="pandas",
                batch_size=None,
                **ray_args,
            )
        return pipeline

    def execute(self):
        pipeline = self.make_pipeline()
        sink_task_n = self.graph.sink_tasks[0].name  # there can be only one
        for batch in pipeline.iter_batches(batch_size=None, batch_format="pandas", prefetch_batches=0):
            yield OutputBatch(batch, task=sink_task_n)


def get_stream_wrapper(task: PipelineTask, context: dict):
    obs = observer.get_observer()

    def wrapper(*df_list):
        observer.set_observer(obs)
        task_df = pd.concat(df_list)
        kwargs = task.resolve_kwargs(context)
        df = task.func(task_df, **kwargs)
        return len(df), df

    return wrapper


def stream_split_func(df, splits):
    splits = split_dataframe(df, num_splits=splits)
    return [len(i) for i in splits] + splits


def task_resources(task):
    r = copy(task.resources)
    r["num_cpus"] = task.num_cpus
    r["num_gpus"] = task.num_gpus
    return {k: v or 0 for k, v in r.items()}


@dataclass
class RemoteTracker:
    task: PipelineTask
    refs: list[ray.ObjectRef]
    is_split: bool = False


class RayStreamGraphExecutor(StreamingGraphExecutor):
    """Ray-based implementation of stream graph executor.

    All task outputs are kept in object store and only de-serialized as needed
    for execution, until yielded by :meth:`run`, where they are de-serialized on
    the driver.

    This executor will attempt to pack the cluster, irrespective of any other
    workloads.

    Note:
      Ray cluster will be initialized with defaults upon run if it hasn't
      already been initialized

    Args:
      ray_poll_timeout: After scheduling as many tasks as can fit, ray.wait on
        all pending tasks for ray_poll_timeout seconds. The timeout gives
        opportunity to re-evaluate cluster resources in case it has expanded
        since last scheduling loop
      \*args, \*\*kwargs: These are passed to
        :py:class:`StreamingGraphexecutor<dplutils.pipeline.stream.StreamingGraphExecutor>`
    """

    def __init__(self, *args, ray_poll_timeout: int = 20, **kwargs):
        super().__init__(*args, **kwargs)
        self.ray_poll_timeout = ray_poll_timeout
        self.remote_splitter = ray.remote(stream_split_func)
        self.sched_resources = defaultdict(float)

    def _setup_remote_tasks(self):
        # bootstrap remote tasks prior to execution and keep reference - this is
        # more efficient than doing so for each remote task run due to required
        # serialization. This should be done just prior to running, as task ray
        # configuration and resources will be baked into the remote.
        self.remote_task_map = {}
        for name, task in self.graph.task_map.items():
            self.remote_task_map[name] = ray.remote(get_stream_wrapper(task, self.ctx)).options(
                num_cpus=task.num_cpus,
                num_gpus=task.num_gpus,
                resources=task.resources,
                name=f"{task.name}<{task.func.__name__}>",
                num_returns=2,  # the remote wrapper returns (len of df, df)
            )

    def execute(self):
        if not ray.is_initialized():
            ray.init()
        self._setup_remote_tasks()
        for batch in super().execute():
            batch.data = ray.get(batch.data)
            yield batch

    def task_submittable(self, task, rank):
        cluster_r = ray.cluster_resources()
        ck_map = {"num_cpus": "CPU", "num_gpus": "GPU"}
        task_r = task_resources(task)
        for k in task_r:
            avail = cluster_r.get(ck_map.get(k, k), 0) - self.sched_resources.get(k, 0)
            # Overcommit the resources for all downstream tasks to ensure that
            # upstream tasks cant starve those that don't presently fit. Source
            # only overcommits if there is nothing downstream since the system
            # can be considered starved
            if task in self.graph.source_tasks and rank > 0 and task_r[k] > avail:
                return False
            elif avail < 0:
                return False
        return True

    def task_submit(self, task, df_list):
        remote_task = self.remote_task_map[task.name]
        for r, v in task_resources(task).items():
            self.sched_resources[r] += v
        refs = remote_task.remote(*df_list)
        return RemoteTracker(task, refs)

    def is_task_ready(self, remote_task):
        ready, _ = ray.wait(remote_task.refs, timeout=0, fetch_local=False)
        if len(ready) == 0:
            return False
        if not remote_task.is_split:
            for r, v in task_resources(remote_task.task).items():
                self.sched_resources[r] -= v
        return True

    def split_batch_submit(self, stream_batch, max_rows):
        splits = int(np.ceil(stream_batch.length / max_rows))
        refs = self.remote_splitter.options(num_returns=splits * 2).remote(stream_batch.data, splits)
        return RemoteTracker(None, refs, True)

    def task_resolve_output(self, remote_task):
        if remote_task.is_split:
            num = int(len(remote_task.refs) / 2)
            return [StreamBatch(ray.get(remote_task.refs[i]), remote_task.refs[i + num]) for i in range(num)]
        return StreamBatch(ray.get(remote_task.refs[0]), remote_task.refs[1])

    def poll_tasks(self, remote_task_list):
        all_refs = list(chain.from_iterable(i.refs for i in remote_task_list))
        # The timeout here is to ensure we can process through the tasks again
        # in the case where the cluster is expanded. The timescale here just
        # needs to be on the order of how long it takes to get new hardware
        # added to cluster (expected seconds/minutes timescale)
        ray.wait(all_refs, timeout=self.ray_poll_timeout, fetch_local=False)
