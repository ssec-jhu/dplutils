import numpy as np
import pandas as pd
import networkx as nx
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import Any
from dplutils.pipeline import PipelineTask, PipelineExecutor
from dplutils.pipeline.utils import deque_extract


@dataclass
class StreamBatch:
    length: int
    data: Any


@dataclass
class StreamTask:
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
        return sum(len(i)  for i in [self.data_in, self.pending, self.split_pending])


class StreamingGraphExecutor(PipelineExecutor, ABC):
    def __init__(self, graph, max_batches=None):
        super().__init__(graph)
        self.max_batches = max_batches
        # make a local copy of the graph with each node wrapped in a tracker
        # object
        self.stream_graph = nx.relabel_nodes(self.graph, StreamTask)

    def execute(self):
        self.n_sourced = 0
        self.source_exhausted = False
        self.source_generator = self.source_generator_fun()
        while True:
            batch = self.execute_until_output()
            if batch is None:
                return
            yield batch

    def source_generator_fun(self):
        bid = 0
        while True:
            yield pd.DataFrame({'run_id': [self.run_id], 'id': [bid]})
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
                if task in self.stream_graph.sink_tasks:
                    return block_info.data
                else:
                    for next_task in self.stream_graph.neighbors(task):
                        next_task.data_in.appendleft(block_info)

            for ready in deque_extract(task.split_pending, self.is_task_ready):
                task.data_in.extendleft(self.task_resolve_output(ready))
        return None

    def process_source(self, source):
        source_batch = []
        for _ in range(source.task.batch_size or 1):
            source_batch.append(next(self.source_generator))
            self.n_sourced += 1
            if self.n_sourced == self.max_batches:
                self.source_exhausted = True
                break
        source.pending.appendleft(self.task_submit(source.task, source_batch))
        source.counter += 1

    def enqueue_tasks(self):
        # Work through the graph in reverse order, submitting any tasks as
        # needed. Reverse order ensures we prefer to send tasks that are closer
        # to the end of the pipeline and only feed as necessary.
        rank = 0
        for task in self.stream_graph.walk_back(sort_key=lambda x: x.counter):
            if task in self.stream_graph.source_tasks or len(task.data_in) == 0:
                continue

            batch_size = task.task.batch_size
            if batch_size is not None:
                for batch in deque_extract(task.data_in, lambda b: b.length > batch_size):
                    task.split_pending.appendleft(self.split_batch_submit(batch, batch_size))

            eligible = False
            while len(task.data_in) > 0:
                num_to_merge = deque_num_merge(task.data_in, batch_size)
                if num_to_merge == 0:
                    # If the feed is terminated and there are no more tasks that
                    # will feed to this one, submit everything
                    if self.source_exhausted and self.task_exhausted(task):
                        num_to_merge = len(task.data_in)
                    else:
                        break
                eligible = True
                if not self.task_submittable(task.task, rank):
                    break
                merged = [task.data_in.pop().data for i in range(num_to_merge)]
                task.pending.appendleft(self.task_submit(task.task, merged))
                task.counter += 1

            if eligible:  # update rank of this task if it _could_ be done, whether or not it was
                rank += 1

        # in least-run order try to enqueue as many of the source tasks as can fit
        while True:
            task_scheduled = False
            for source in sorted(self.stream_graph.source_tasks, key=lambda x: x.counter):
                if self.source_exhausted or not self.task_submittable(source.task, rank):
                    continue
                self.process_source(source)
                task_scheduled = True
            if not task_scheduled:
                break

    def execute_until_output(self):
        while True:
            if (completed := self.resolve_completed()) is not None:
                return completed

            if self.source_exhausted and self.task_exhausted():
                return None

            self.enqueue_tasks()
            self.poll_tasks(self.get_pending())

    @abstractmethod
    def task_submit(self, task: PipelineTask, df_list: list[pd.DataFrame]) -> Any:
        pass

    @abstractmethod
    def task_resolve_output(self, pending_task: Any) -> StreamBatch:
        pass

    @abstractmethod
    def is_task_ready(self, pending_task: Any) -> bool:
        pass

    @abstractmethod
    def task_submittable(self, task: PipelineTask, rank: int) -> bool:
        pass

    @abstractmethod
    def split_batch_submit(self, batch: StreamBatch, max_rows: int) -> Any:
        pass

    @abstractmethod
    def poll_tasks(self, pending_task_list: list[Any]) -> None:
        pass


class LocalSerialExecutor(StreamingGraphExecutor):
    # for testing purposes
    sflag = 0

    def task_submit(self, pt, df_list):
        self.sflag = 1
        return pt.func(pd.concat(df_list))

    def split_batch_submit(self, stream_batch, max_rows):
        df = stream_batch.data
        return np.array_split(df, np.ceil(len(df) / max_rows))

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
        idxs, = np.where(s_accum >= batch_size)
        if len(idxs) == 0:
            return 0
        return idxs[0] + 1
