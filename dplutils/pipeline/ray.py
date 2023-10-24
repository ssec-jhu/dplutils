import ray
import pandas as pd
import numpy as np
from dplutils import observer
from dplutils.pipeline import PipelineTask, PipelineExecutor


def set_run_id(inputs, run_id):
    inputs.rename(columns={'id': 'batch_id'}, inplace=True)
    inputs['run_id'] = run_id
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

        splits = np.array_split(indf, np.ceil(len(indf)/task.batch_size))
        refs = [
            ray.remote(funcwrapper).options(
                num_cpus=task.num_cpus, num_gpus=task.num_gpus, resources=task.resources,
            ).remote(i, **kwargs)
            for i in splits
        ]
        return pd.concat(ray.get(refs))

    # Ray data uses the function name to name the underlying remote tasks, override for the wrapper for better
    # observability
    wrapper.__name__ = f'{task.name}<{task.func.__name__}>'
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
        self.n_batches = n_batches

    def make_pipeline(self):
        pipeline = ray \
            .data \
            .range(self.n_batches, parallelism=self.n_batches) \
            .map_batches(
                set_run_id,
                batch_format = 'pandas',
                fn_kwargs = {'run_id': self.run_id})
        for task in self.tasks:
            ray_args = dict()
            if task.batch_size is None:
                # batch size set triggers the wrapper to run remote functions and resources must be set there,
                # map_batches task would get default of 1 cpu (could be 0?)
                ray_args = dict(
                    num_cpus = task.num_cpus,
                    num_gpus = task.num_gpus,
                    resources = task.resources,
                )
            pipeline = pipeline.map_batches(
                get_remote_wrapper(task, self.ctx),
                batch_format = 'pandas',
                batch_size = None,
                **ray_args,
            )
        return pipeline

    def execute(self):
        pipeline = self.make_pipeline()
        for batch in pipeline.iter_batches(batch_size = None, batch_format = 'pandas', prefetch_batches = 0):
            yield batch
