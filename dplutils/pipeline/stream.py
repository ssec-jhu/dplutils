from abc import ABC, abstractmethod
from collections import defaultdict, deque
from collections.abc import Generator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

import networkx as nx
import numpy as np
import pandas as pd

from dplutils.pipeline import OutputBatch, PipelineExecutor, PipelineTask
from dplutils.pipeline.graph import TRM
from dplutils.pipeline.utils import deque_extract, split_dataframe


class LKind(Enum):
    TASK = "normal"
    SPLIT = "split"
    MERGE = "merge"
    SOURCE = "source"


@dataclass(frozen=True)
class LineageEntry:
    """Record of a division or processing event in batch lineage.

    Args:
      task: structured identifier for this point in the pipeline.
      num_segments: how many sibling batches exist at this point. 1 for normal
        processing, N for an N-way split or N-way fan-out.
      uid: globally unique monotonic counter assigned by the executor. Used as
        the grouping key for :attr:`join_by_origin` nodes.
    """

    task: str
    num_segments: int
    uid: int
    kind: LKind
    children: tuple["LineageEntry", ...] = field(default_factory=tuple)


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
    _lineage: LineageEntry | None = None


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

    @property
    def all_pending(self):
        return self.pending + self.split_pending

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
        # by name paths required for each join task, for finding complete origin
        # We truncate the end node and store as tuple of task names, as the
        # lineage stores name
        self._join_task_segments = {}
        for task in self.stream_graph:
            if task.task.join_by_origin:
                task_source = self.stream_graph.common_source(task)
                segments = self.stream_graph.paths_between(task_source, task)
                # No need to use join logic with only a single path
                if len(segments) <= 1:
                    continue
                # get the name of the task or None in the case of source,
                # truncating the end node which is join task itself. name used
                # for comparison to the lineage entries.
                segments = [tuple(None if isinstance(t, TRM) else t.name for t in s[:-1]) for s in segments]
                self._join_task_segments[task] = segments

    def _next_uid(self):
        """Return the next globally unique lineage entry identifier."""
        uid = self._uid_counter
        self._uid_counter += 1
        return uid

    def pre_execute(self):
        pass

    def output_batch_transform(self, batch: OutputBatch) -> OutputBatch:
        return batch

    def execute(self):
        self.n_sourced = 0
        self._uid_counter = 0
        self.source_exhausted = False
        self.source_generator = self.generator_fun()
        self.pre_execute()
        while True:
            batch = self.execute_until_output()
            if batch is None:
                return
            yield self.output_batch_transform(batch)

    def source_generator_fun(self):
        bid = 0
        while True:
            yield pd.DataFrame({"run_id": [self.run_id], "id": [bid]})
            bid += 1

    def get_pending(self):
        # Return the raw opaque handles (.data) from pending StreamBatch objects,
        # since poll_tasks and implementations expect the raw references.
        return [p.data for tn in self.stream_graph for p in tn.all_pending]

    def _is_task_ready(self, pending_batch):
        """Unwrap StreamBatch from pending queue and delegate to implementation."""
        return self.is_task_ready(pending_batch.data)

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
            for ready in deque_extract(task.pending, self._is_task_ready):
                block_info = self.task_resolve_output(ready.data)
                if block_info.length == 0:
                    continue

                if task in self.stream_graph.sink_tasks:
                    self.logger.debug(f"Batch <{task.name}>[l={block_info.length}] completed as output")
                    return OutputBatch(block_info.data, task=task.name)
                else:
                    num_neighbors = sum(1 for _ in self.stream_graph.neighbors(task))
                    block_info._lineage = LineageEntry(
                        task=task.name,
                        num_segments=num_neighbors,
                        uid=self._next_uid(),
                        kind=LKind.TASK,
                        children=(ready._lineage,),
                    )
                    for next_task in self.stream_graph.neighbors(task):
                        self.logger.debug(f"Moving <{task.name}>[l={block_info.length}] to <{next_task.name}>")
                        next_task.data_in.appendleft(block_info)

            for ready in deque_extract(task.split_pending, self._is_task_ready):
                self.logger.debug(f"Splits <{task.name}> completed, moving to input queue")
                resolved = self.task_resolve_output(ready.data)
                lineage = LineageEntry(
                    task=task.name,
                    num_segments=len(resolved),
                    uid=self._next_uid(),
                    kind=LKind.SPLIT,
                    children=(ready._lineage,),
                )
                for split in resolved:
                    batch = StreamBatch(length=split.length, data=split.data, _lineage=lineage)
                    task.data_in.appendleft(batch)
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
            lineage = LineageEntry(num_segments=1, uid=self._next_uid(), task="", kind=LKind.SOURCE)
            for task in self.stream_graph.source_tasks:
                task.data_in.append(StreamBatch(data=next_df, length=len(next_df), _lineage=lineage))
            self.n_sourced += 1
            if self.n_sourced == self.max_batches:
                self.logger.debug("Max batches reached, cancelling source generation")
                self.source_exhausted = True
                break
            total_length += len(next_df)

    def _submit_merged_to_task(self, task, batches):
        # Use lineage prefix up to and including the MRCA entry as the base
        lineage = LineageEntry(
            task=task.name,
            kind=LKind.MERGE,
            num_segments=1,
            children=tuple(b._lineage for b in batches),
            uid=self._next_uid(),
        )
        merged = [b.data for b in batches]
        total_length = sum(b.length for b in batches)
        self.logger.debug(f"Enqueueing merged batch for <{task.name}>;n={len(merged)};l={total_length}]")
        handle = self.task_submit(task.task, merged)
        task.pending.appendleft(StreamBatch(length=total_length, data=handle, _lineage=lineage))
        task.counter += 1

    def _find_complete_origin(self, task):
        # For a join task, find a set of origin uids from tasks within the queue
        # for which we have seen batches from all required paths. Due to
        # merging, the uids required to pass through to the join may be more
        # than one.
        required_paths = self._join_task_segments[task]
        mrca_name = required_paths[0][0]
        required_segments = defaultdict(dict)  # how many batches we need to see per path per origin uid
        found_segments = defaultdict(lambda: defaultdict(int))  # how many batches we did see per path per origin uid
        all_origins = []
        for batch in reversed(task.data_in):
            l_stack = [batch._lineage]  # stack for walking the lineage tree
            origin_ids = set()  # all origin ids found for this batch
            path = []  # stack for storing current visiting path
            multiplier = []  # stack for storing the current multiplication factor from splits
            while l_stack:
                le = l_stack.pop()
                if le.kind == LKind.TASK or le.kind == LKind.SOURCE:
                    path.append(le.task if le.kind == LKind.TASK else None)
                    multiplier.append(1)
                    if le.task == mrca_name or le.kind == LKind.SOURCE:
                        origin_ids.add(le.uid)
                        found_path = tuple(reversed(path))
                        required_segments[le.uid][found_path] = np.prod(multiplier)
                        found_segments[le.uid][found_path] += 1
                        path.pop()
                        multiplier.pop()
                        continue
                elif le.kind == LKind.SPLIT:
                    multiplier[-1] *= le.num_segments
                l_stack.extend(le.children)
            # add to all origins, combining with existing entry if there is
            # overlap in any of the uids, since then they need to be sent
            # together
            all_origins.append(origin_ids)
        merged = _merge_origin_sets(all_origins)
        for oid_set in merged:
            completeness = [
                # we always need at least one segment per path, even if we
                # haven't processed through the splits, e.g. the 0 and 1 defaults
                found_segments[oid].get(rp, 0) == required_segments[oid].get(rp, 1)
                for oid in oid_set
                for rp in required_paths
            ]
            if all(completeness):
                return oid_set
        return None

    def _handle_join_task(self, task, rank):
        eligible = submitted = False
        mrca_name = self._join_task_segments[task][0][0]
        complete_origin = self._find_complete_origin(task)

        if complete_origin is None:
            return (eligible, submitted)

        eligible = True
        if not self.task_submittable(task.task, rank):
            return (eligible, submitted)

        # Extract all batches for all origin uids in this complete set
        batches = list(deque_extract(task.data_in, lambda b: _batch_origins(b, mrca_name).issubset(complete_origin)))
        self._submit_merged_to_task(task, batches)
        submitted = True
        return (eligible, submitted)

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

            # join_by_origin tasks use a different submission path
            if task in self._join_task_segments:
                return self._handle_join_task(task, rank)

            batch_size = task.task.batch_size
            if batch_size is not None:
                for batch in deque_extract(task.data_in, lambda b: b.length > batch_size):
                    self.logger.debug(f"Enqueueing split for <{task.name}>[bs={batch_size}]")
                    split_ref = self.split_batch_submit(batch, batch_size)
                    task.split_pending.appendleft(
                        StreamBatch(length=batch.length, data=split_ref, _lineage=batch._lineage)
                    )

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

            batches = [task.data_in.pop() for _ in range(num_to_merge)]
            self._submit_merged_to_task(task, batches)
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


def _batch_origins(batch, mrca_name):
    entry = [batch._lineage]
    origins = set()
    while entry:
        le = entry.pop()
        if le.kind == LKind.TASK and le.task == mrca_name:
            origins.add(le.uid)
        elif le.kind == LKind.SOURCE and mrca_name is None:
            origins.add(le.uid)
        entry.extend(le.children)
    return origins


def _merge_origin_sets(sets):
    # Given a list of sets of origin ids, merge any that have overlap, since they
    # need to be merged together for join_by_origin submission, doing so
    # preserving the order in which they appear.
    merged = []
    for s in sets:
        new_merged = []
        for m in merged:
            if not s.isdisjoint(m):
                s = s.union(m)
            else:
                new_merged.append(m)
        new_merged.append(s)
        merged = new_merged
    return merged
