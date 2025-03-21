from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import Any, Callable

import networkx as nx
import numpy as np
import pandas as pd

from dplutils.pipeline import OutputBatch, PipelineExecutor, PipelineTask
from dplutils.pipeline.utils import deque_extract, split_dataframe


@dataclass
class StreamBatch:
    """Container for task output tracking

    Args:
      length: length of dataframe that is referenced by ``data``. This field is
        required as in many cases ``data`` will be something that eventually
        resolves to a dataframe, but not available to driver.
      data: data should contain a reference to a DataFrame in whatever way is
        meaningful to implementation. This field is not introspected, only
        passed by the framework.
    """

    length: int
    data: Any


@dataclass
class StreamTask:
    """Internal task wrapper for :class:`StreamingGraphExecutor`"""

    task: PipelineTask
    data_in: list[StreamBatch] = field(default_factory=deque)
    pending: list = field(default_factory=deque)
    counter: int = 0
    split_pending: list = field(default_factory=deque)

    def __hash__(self):
        return hash(self.task)

    @property
    def name(self):
        return self.task.name

    def total_pending(self):
        return sum(len(i) for i in [self.data_in, self.pending, self.split_pending])


class StreamingGraphExecutor(PipelineExecutor, ABC):
    """Base class for implementing streaming remote graph execution

    This class implements the :meth:`execute` method of
    :class:`PipelineExecutor` and contains logic necessary to schedule tasks,
    prioritizing completing those that are closer to terminals. It supports
    arbitrary pipeline graphs with branches, multiple inputs and outputs. By
    default, for each run, it generates a indefinite stream of input dataframes
    tagged with a monotonically incrementing batch id.

    Args:
        max_batches: maximum number of batches from the source generator to feed
          to the input task(s). Default is None, which means either exhaust the
          source generator or run indefinitely.
        generator: A callable that when called returns a generator which yields
          dataframes. The driver will call ``len()`` on the yielded dataframes to
          obtain the number of rows and will split and batch according to the input
          task settings. Each generated dataframe, regardless of size, counts as
          a single source batch with respect to ``max_batches``.


    Implementations must override abstract methods for (remote) task submission
    and polling. The following must be overriden, see their docs for more:

    - :meth:`is_task_ready`
    - :meth:`poll_tasks`
    - :meth:`split_batch_submit`
    - :meth:`task_resolve_output`
    - :meth:`task_submit`
    - :meth:`task_submittable`
    """

    def __init__(
        self, graph, max_batches: int = None, generator: Callable[[], Generator[pd.DataFrame, None, None]] = None
    ):
        super().__init__(graph)
        self.max_batches = max_batches
        # make a local copy of the graph with each node wrapped in a tracker
        # object
        self.stream_graph = nx.relabel_nodes(self.graph, StreamTask)
        self.generator_fun = generator or self.source_generator_fun

    def execute(self):
        self.n_sourced = 0
        self.source_exhausted = False
        self.source_generator = self.generator_fun()
        while True:
            batch = self.execute_until_output()
            if batch is None:
                return
            yield batch

    def source_generator_fun(self):
        bid = 0
        while True:
            yield pd.DataFrame({"run_id": [self.run_id], "id": [bid]})
            bid += 1

    def get_pending(self):
        return [p for tn in self.stream_graph for p in tn.pending]

    def task_exhausted(self, task=None):
        if task is not None and len(task.split_pending) > 0:
            return False
        for upstream in self.stream_graph.walk_back(task):
            if upstream.total_pending() > 0:
                return False
        return True

    def resolve_completed(self):
        # Walk graph forward to promote completed tasks to next task
        # queue. Dataframes for completed sink tasks are returned here in order
        # to prioritize flushing.
        for task in self.stream_graph.walk_fwd():
            for ready in deque_extract(task.pending, self.is_task_ready):
                block_info = self.task_resolve_output(ready)
                if block_info.length == 0:
                    continue
                if task in self.stream_graph.sink_tasks:
                    self.logger.debug(f"Batch <{task.name}>[l={block_info.length}] completed as output")
                    return OutputBatch(block_info.data, task=task.name)
                else:
                    for next_task in self.stream_graph.neighbors(task):
                        self.logger.debug(f"Moving <{task.name}>[l={block_info.length}] to <{next_task.name}>")
                        next_task.data_in.appendleft(block_info)

            for ready in deque_extract(task.split_pending, self.is_task_ready):
                self.logger.debug(f"Splits <{task.name}> completed, moving to input queue")
                task.data_in.extendleft(self.task_resolve_output(ready))
        return None

    def _feed_source(self, source):
        if self.source_exhausted:
            return
        total_length = sum(i.length for i in source.data_in)
        while total_length < (source.task.batch_size or 1):
            try:
                next_df = next(self.source_generator)
            except StopIteration:
                self.logger.debug("Source generator exhausted")
                self.source_exhausted = True
                break
            # We feed any generated source to all source tasks similar the way
            # upstream forked outputs broadcast. We add to data_in so that any
            # necessary batching and splitting can be handled by normal procedure.
            for task in self.stream_graph.source_tasks:
                task.data_in.append(StreamBatch(data=next_df, length=len(next_df)))
            self.n_sourced += 1
            if self.n_sourced == self.max_batches:
                self.logger.debug("Max batches reached, cancelling source generation")
                self.source_exhausted = True
                break
            total_length += len(next_df)

    def enqueue_tasks(self):
        # helper to make submission decision of a single task based on the batch
        # size, exhaustion conditions, and whether the implementation deems it
        # submittable. Returns flags (eligible, submitted) to indicate whether
        # it was eligible to be submitted based on input queue and batch size,
        # and whether it was actually submitted.
        def _handle_one_task(task, rank):
            eligible = submitted = False
            if len(task.data_in) == 0:
                return (eligible, submitted)

            batch_size = task.task.batch_size
            if batch_size is not None:
                for batch in deque_extract(task.data_in, lambda b: b.length > batch_size):
                    self.logger.debug(f"Enqueueing split for <{task.name}>[bs={batch_size}]")
                    task.split_pending.appendleft(self.split_batch_submit(batch, batch_size))

            num_to_merge = deque_num_merge(task.data_in, batch_size)
            if num_to_merge == 0:
                # If the feed is terminated and there are no more tasks that
                # will feed to this one, submit everything
                if self.source_exhausted and self.task_exhausted(task):
                    num_to_merge = len(task.data_in)
                else:
                    return (eligible, submitted)
            eligible = True
            if not self.task_submittable(task.task, rank):
                return (eligible, submitted)

            merged = [task.data_in.pop().data for _ in range(num_to_merge)]
            self.logger.debug(f"Enqueueing merged batches <{task.name}>[n={len(merged)};bs={batch_size}]")
            task.pending.appendleft(self.task_submit(task.task, merged))
            task.counter += 1
            submitted = True
            return (eligible, submitted)

        # proceed through all non-source tasks, which will be handled separately
        # below due to the need to feed from generator. We walk backwards,
        # re-evaluating the sort order of tasks of same depth after each single
        # submission, implementing a kind of "fair" submission, while still
        # prioritizing tasks closer to the sink.
        submitted = True
        while submitted:
            rank = 0
            submitted = False
            for task in self.stream_graph.walk_back(sort_key=lambda x: x.counter):
                if task in self.stream_graph.source_tasks:
                    continue
                eligible, submitted = _handle_one_task(task, rank)
                if eligible:  # update rank of this task if it _could_ be done, whether or not it was
                    rank += 1
                if submitted:
                    break

        # Source as many inputs as can fit on source tasks. We prioritize flushing the
        # input queue and secondarily on number of invocations in case batch sizes differ.
        while True:
            task_scheduled = False
            for task in sorted(self.stream_graph.source_tasks, key=lambda x: (-len(x.data_in), x.counter)):
                if self.task_submittable(task.task, rank):
                    self._feed_source(task)
                    _, task_scheduled = _handle_one_task(task, rank)
                    if task_scheduled:  # we want to re-evalute the sort order
                        break
            if not task_scheduled:
                break

    def execute_until_output(self):
        while True:
            if (completed := self.resolve_completed()) is not None:
                return completed

            if self.source_exhausted and self.task_exhausted():
                self.logger.debug("All tasks exhausted, pipeline run ends")
                return None

            self.enqueue_tasks()
            self.poll_tasks(self.get_pending())

    @abstractmethod
    def task_submit(self, task: PipelineTask, df_list: list[pd.DataFrame]) -> Any:
        """Run or arrange for the running of task

        Implementations must override this method and arrange for the function
        of ``task`` to be called on a dataframe made from the concatenation of
        ``df_list``. The return value will be maintained in a pending queue, and
        both ``task_resolve_output`` and ``is_task_ready`` will take these as
        input, but will otherwise not be inspected. Typically the return value
        would be a handle to the remote result or a future, or equivalent.

        Note:
            ``PipelineTask`` expects a single DataFrame as input, while this
            function receives a batch of such. It MUST concatenate these into a
            single DataFrame prior to execution (e.g. with
            ``pd.concat(df_list)``). This is not done in the driver code as the
            dataframes in ``df_list`` may not be local.
        """
        pass

    @abstractmethod
    def task_resolve_output(self, pending_task: Any) -> StreamBatch:
        """Return a :class:`StreamBatch` from completed task

        This function takes the output produced by either :meth:`task_submit` or
        :meth:`split_batch_submit`, and returns a :class:`StreamBatch` object which
        tracks the length of returned dataframe(s) and the object which
        references the underlying DataFrame.

        The ``data`` member of returned :class:`StreamBatch` will be either:

        - passed to another call of :meth:`task_submit` in a list container, or
        - yielded in the :meth:`execute` call (which yields in the user-called
          ``run`` method). If any handling must be done prior to yield,
          implementation should do so in overloaded :meth:`execute`.
        """
        pass

    @abstractmethod
    def is_task_ready(self, pending_task: Any) -> bool:
        """Return true if pending task is ready

        This method takes outputs from :meth:`task_submit` and
        :meth:`split_batch_submit` and must return ``True`` if the task is complete
        and can be passed to :meth:`task_resolve_output` or ``False`` otherwise.
        """
        pass

    @abstractmethod
    def task_submittable(self, task: PipelineTask, rank: int) -> bool:
        """Preflight check if task can be submitted

        Return ``True`` if current conditions enable the ``task`` to be
        submitted. The ``rank`` argument is an indicator of relative importance,
        and is incremented whenever the pending data for a given tasks meets the
        batching requirements as driver walks the task graph backward. Thus
        ``Rank=0`` represents the task furthest along and so the highest
        priority for submission.
        """
        pass

    @abstractmethod
    def split_batch_submit(self, batch: StreamBatch, max_rows: int) -> Any:
        """Submit a task to split batch into at most ``max_rows``

        Similart to task_submit, implementations should arrange by whatever
        means make sense to take the dataframe reference in ``batch.data`` of
        :class:`StreamBatch`, given its length in ``batch.length`` and split
        into a number of parts that result in no more than ``max_rows`` per
        part. The return value should be a list of objects that can be processed
        by :meth:`is_task_ready` and :meth:`task_resolve_output`.
        """
        pass

    @abstractmethod
    def poll_tasks(self, pending_task_list: list[Any]) -> None:
        """Wait for any change in status to ``pending_task_list``

        This method will be called after submitting as many tasks as
        possible. It gives a chance for implementations to wait in a io-friendly
        way, for example by waiting on async futures. The input is a list of
        objects as returned by :meth:`task_submit` or :meth:`split_batch_submit`. The
        return value is unused.
        """
        pass


class LocalSerialExecutor(StreamingGraphExecutor):
    """Implementation for reference and testing purposes

    This reference implementation demonstrates expected outputs for abstract
    methods, feeding a single batch at a time source to sink in the main thread.
    """

    sflag = 0

    def task_submit(self, pt, df_list):
        self.sflag = 1
        return pt.func(pd.concat(df_list))

    def split_batch_submit(self, stream_batch, max_rows):
        df = stream_batch.data
        return split_dataframe(df, max_rows=max_rows)

    def task_resolve_output(self, to):
        if isinstance(to, list):
            return [StreamBatch(len(i), i) for i in to]
        return StreamBatch(len(to), to)

    def task_submittable(self, t, rank):
        return self.sflag == 0

    def is_task_ready(self, t):
        return True

    def poll_tasks(self, pending):
        self.sflag = 0


def deque_num_merge(queue, batch_size):
    if batch_size is None:
        return 1 if len(queue) > 0 else 0
    else:
        # So long as batch size is set, try to merge if necessary. Proceed in
        # fifo order and take up to batch_size rows, but no more.
        s_accum = np.cumsum([i.length for i in reversed(queue)])
        (idxs,) = np.where(s_accum >= batch_size)
        if len(idxs) == 0:
            return 0
        return idxs[0] + 1
