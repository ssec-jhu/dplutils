from enum import Enum

from networkx import DiGraph, all_simple_paths, bfs_edges, is_directed_acyclic_graph, path_graph

from dplutils.pipeline.task import PipelineTask


class TRM(Enum):
    sink = "sink"
    source = "source"


class PipelineGraph(DiGraph):
    """Graph of pipeline tasks.

    This class adds convenience functionality for task pipeline handling on top
    of :class:`networkx.DiGraph` on which it is based.

    Args:
      graph: This is either a list of :class:`PipelineTask` objects representing a
        simple-graph, or anything that is legal input to :class:`networkx.DiGraph`.
    """

    def __init__(self, graph=None):
        if isinstance(graph, list) and isinstance(graph[0], PipelineTask):
            graph = path_graph(graph, DiGraph)
        super().__init__(graph)
        if not is_directed_acyclic_graph(self):
            raise ValueError("cycles detected in graph")

    @property
    def task_map(self):
        return {i.name: i for i in self}

    @property
    def source_tasks(self):
        return [n for n, d in self.in_degree() if d == 0]

    @property
    def sink_tasks(self):
        return [n for n, d in self.out_degree() if d == 0]

    def to_list(self):
        """Return list representation of task iff it is a simple-path graph"""
        if len(self.source_tasks) != 1 or len(self.sink_tasks) != 1:
            raise ValueError("to_list requires a graph with only one start and end task")
        source = self.source_tasks[0]
        sink = self.sink_tasks[0]
        if source == sink:
            return [source]
        paths = list(all_simple_paths(self, source, sink))
        if len(paths) != 1:
            raise ValueError("to_list requires a single path from start to end task, found {len(paths)}")
        return paths[0]

    def with_terminals(self):
        graph = self.copy()
        graph.add_edges_from((TRM.source, i) for i in self.source_tasks)
        graph.add_edges_from((i, TRM.sink) for i in self.sink_tasks)
        return graph

    def _walk(self, source, back=False, sort_key=None):
        graph = self.with_terminals()

        # doubly wrap the sort key function for conveneince (since bfs search
        # takes list, not sort key) and to inject the ignoring of terminal
        # nodes. This makes the walk sort key behave a bit more like `sorted()`
        def _sort_key(x):
            return 0 if isinstance(x, TRM) else sort_key(x)

        sorter = (lambda x: sorted(x, key=_sort_key)) if sort_key else None
        for _, node in bfs_edges(graph, source, reverse=back, sort_neighbors=sorter):
            if not isinstance(node, TRM):
                yield node

    def walk_fwd(self, source=None, sort_key=None):
        """Walk graph forward in breadth-first order from ``source``

        This is a generator that yields tasks encountered as it walks along
        edges in the forward direction, starting at ``source``, or at the set of
        :attr:`source_tasks` if not supplied.

        Args:
          source: starting task of walk, defaults to :attr:`source_tasks`
          sort_key: when multiple out-egdes are encountered, sort the yielded
            tasks in order of callable `sort_key`, which should return a
            sortable object given :class:`PipelineTask` as input.
        """
        return self._walk(source or TRM.source, back=False, sort_key=sort_key)

    def walk_back(self, source=None, sort_key=None):
        """Walk graph backward in breadth-first order from ``source``

        This is a generator that yields tasks encountered as it walks along
        edges in the reverse direction, starting at ``source``, or at the set of
        :attr:`sink_tasks` if not supplied.

        Args:
          source: starting task of walk, defaults to :attr:`source_tasks`
          sort_key: when multiple in-egdes are encountered, sort the yielded
            tasks in order of callable `sort_key`, which should return a
            sortable object given :class:`PipelineTask` as input.
        """
        return self._walk(source or TRM.sink, back=True, sort_key=sort_key)
