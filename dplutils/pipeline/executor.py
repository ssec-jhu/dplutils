import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from dplutils.pipeline.graph import PipelineGraph
from dplutils.pipeline.utils import dict_from_coord


@dataclass
class OutputBatch:
    data: pd.DataFrame
    task: str = None


class PipelineExecutor(ABC):
    """Base class for pipeline executors.

    This class defines the interface for the execution of a graph of
    :class:`PipelineTask<dplutils.pipeline.task.PipelineTask>` objects.

    Subclasses must override the ``execute`` method, which is called by ``run``
    to execute the pipeline and return and generator of dataframes of the final
    tasks in the graph.
    """

    def __init__(self, graph: PipelineGraph):
        if isinstance(graph, list):
            self.graph = PipelineGraph(deepcopy(graph))
        else:
            self.graph = deepcopy(graph)
        self.ctx = {}
        self._run_id = None

    @classmethod
    def from_graph(cls, graph: PipelineGraph) -> "PipelineExecutor":
        return cls(graph)

    @property
    def tasks_idx(self):  # for back compat
        return self.graph.task_map

    def set_context(self, key, value) -> "PipelineExecutor":
        self.ctx[key] = value
        return self

    def set_config_from_dict(self, config) -> "PipelineExecutor":
        for task_name, confs in config.items():
            if task_name not in self.tasks_idx:
                raise ValueError(f"no such task: {task_name}")
            for key, value in confs.items():
                task = self.tasks_idx[task_name]
                task_val = getattr(task, key)
                if isinstance(task_val, dict) and isinstance(value, dict):
                    task_val.update(value)
                else:
                    setattr(task, key, value)
        return self

    def set_config(
        self,
        coord: str | dict | None = None,
        value: Any | None = None,
        from_yaml: str | Path | None = None,
    ) -> "PipelineExecutor":
        """Set task configuration options for this instance.

        This applies configurations to :class:`PipelineTask
        <dplutils.pipeline.task.PipelineTask>` instances by name.

        args:
            coord: either a:
                 * String specifying the task property to set, given by the
                   taskname and property coordinates separated by dots. For
                   example, ``taskname.kwargs.arg1`` would set the ``arg1``
                   keyword argument of task with name ``taskname``.
                 * Dictionary with the properties to be updated. The top-level keys
                   should be task names, within those the propeties to set. For
                   example, to set the num_cpus of ``taskname``, use
                   ``{'taskname': {'num_cpus': 2}}``
                * None, indicating that a yaml file should be supplied with the
                  config in ``from_yaml``
            value: If a coord is specified as a string, this is the value to set
                the parameter at those coordinates
            from_yaml: A yaml file containing task parameters to set. This
                should have the same structure as a dict supplied to ``coord``.
        """
        if coord is None:
            if from_yaml is None:
                raise ValueError("one of dict/string coordinate and value/file input is required")
            with open(from_yaml, "r") as f:
                return self.set_config_from_dict(yaml.load(f, yaml.SafeLoader))
        if isinstance(coord, dict):
            return self.set_config_from_dict(coord)

        return self.set_config_from_dict(dict_from_coord(coord, value))

    def validate(self) -> None:
        excs = []
        for task in self.tasks_idx.values():
            try:
                task.validate(self.ctx)
            except ValueError as e:
                excs.append(str(e))
        if len(excs) > 0:
            raise ValueError("Errors in validation:\n    - " + "\n    - ".join(excs))

    @property
    def run_id(self) -> str:
        if self._run_id is None:
            self._run_id = str(uuid.uuid1())
        return self._run_id

    @abstractmethod
    def execute(self) -> Iterable[OutputBatch]:
        """Execute the task graph in batches.

        This method must be overridden by implementations. It should arrange for
        the functions defined in the graph of ``PipelineTask``s to execute with
        provided configuration, passing the upstream task output DataFrame to
        the input of Task ``func``.

        By default, the graph of tasks is in ``self.tasks`` and an index
        implemented as a dictionary locating task by name in ``self.tasks_idx``.

        For example, given a dataframe from the previous task, a call for
        ``task`` would be:

            task.func(prev_out_dataframe, **task.resolve_kwargs(self.context))

        The method should return an iterator to the batches dataframes (stored
        as :py:meth:`OutputBatch<OutputBatch>`s) of the terminal tasks in the
        graph as they complete.
        """
        pass

    def run(self) -> Iterable[OutputBatch]:
        """Validate and run the pipeline.

        Calls the ``self.execute`` method, and returns an iterator to batches as
        they complete.
        """
        self.validate()
        self._run_id = None  # force reallocation
        return self.execute()

    def writeto(
        self, outdir: Path | str, partition_by_task: bool | None = None, task_partition_name: str = "task"
    ) -> None:
        """Run pipeline, writing results to parquet table.

        args:
            outdir: path to the directory in which to write files
            partition_by_task: If True, always create a hive-style partition by
              the output task name in the output directory. If false, never
              create such a partition. If None (default), create partitions if
              there are multiple sink tasks, otherwise do not.
            task_partition_name: name of the hive-style partition for
              tasks. Default is task, e.g. create paths like
              ``{outdir}/task={task}/...``
        """
        if partition_by_task is None:
            partition_by_task = False if len(self.graph.sink_tasks) == 1 else True

        Path(outdir).mkdir(parents=True, exist_ok=True)
        for c, batch in enumerate(self.run()):
            if partition_by_task:
                part_name = batch.task or "__HIVE_DEFAULT_PARTITION__"
                part_path = Path(outdir) / f"{task_partition_name}={part_name}"
                part_path.mkdir(exist_ok=True)
                outfile = part_path / f"{self.run_id}-{c}.parquet"
            else:
                outfile = Path(outdir) / f"{self.run_id}-{c}.parquet"
            batch.data.to_parquet(outfile, index=False)
