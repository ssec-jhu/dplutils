from dataclasses import dataclass

import pytest

from dplutils.pipeline import PipelineGraph, PipelineTask
from dplutils.pipeline.graph import TRM


def make_graph_struct(edges, sources, sinks, simple=None):
    @dataclass
    class GraphInfo:
        edges: list[tuple]
        sources: list[PipelineTask]
        sinks: list[PipelineTask]
        simple: list[PipelineTask] | None

    return GraphInfo(edges, sources, sinks, simple)


def graph_suite():
    a = PipelineTask("a", 1)
    b = PipelineTask("b", 2)
    c = PipelineTask("c", 3)
    d = PipelineTask("d", 4)
    e = PipelineTask("e", 5)
    f = PipelineTask("f", 6)
    g = PipelineTask("g", 7)

    return {
        "simple": make_graph_struct([(a, b), (b, c), (c, d)], [a], [d], [a, b, c, d]),
        "branched": make_graph_struct([(a, b), (b, c), (b, d), (c, e), (d, e)], [a], [e]),
        "multisource": make_graph_struct([(a, c), (b, c), (c, d)], [a, b], [d]),
        "multisink": make_graph_struct([(a, b), (b, c), (b, d)], [a], [c, d]),
        "branchmulti": make_graph_struct([(a, c), (b, c), (c, d), (c, e), (d, f), (e, g)], [a, b], [f, g]),
        "branchmultiout": make_graph_struct([(a, b), (b, c), (b, d), (d, e), (d, f), (f, g)], [a], [c, e, g]),
        "nesteddiamond": make_graph_struct([(a, b), (a, c), (b, d), (b, e), (d, f), (e, f), (c, g), (f, g)], [a], [g]),
    }


@pytest.mark.parametrize("graph_info", graph_suite().values())
class TestGraph:
    def test_graph_instantiation(self, graph_info):
        PipelineGraph(graph_info.edges)

    def test_graph_sinks_sources(self, graph_info):
        p = PipelineGraph(graph_info.edges)
        assert p.source_tasks == graph_info.sources
        assert p.sink_tasks == graph_info.sinks

    def test_graph_nodes_name_map(self, graph_info):
        p = PipelineGraph(graph_info.edges)
        assert isinstance(p.task_map, dict)
        for i in p:
            assert p.task_map[i.name] == i

    def test_graph_with_terminals(self, graph_info):
        p = PipelineGraph(graph_info.edges)
        w_term = p.with_terminals()
        for i in graph_info.sources:
            assert len(p.in_edges(i)) == 0
            assert len(w_term.in_edges(i)) == 1
        for i in graph_info.sinks:
            assert len(p.out_edges(i)) == 0
            assert len(w_term.out_edges(i)) == 1

    def test_graph_to_list(self, graph_info):
        p = PipelineGraph(graph_info.edges)
        if graph_info.simple is not None:
            p.to_list() == graph_info.simple
        else:
            with pytest.raises(ValueError):
                p.to_list()

    def test_graph_walk_returns_node_list(self, graph_info):
        p = PipelineGraph(graph_info.edges)
        walked = list(p.walk_fwd())
        assert walked[0] in graph_info.sources
        assert len(walked) == len(p)
        walked = list(p.walk_back())
        assert walked[0] in graph_info.sinks
        assert walked[-1] in graph_info.sources
        assert len(walked) == len(p)


    def test_graph_walk_back_respects_dependencies(self, graph_info):
        """Every node must appear after all of its successors in a backward walk."""
        p = PipelineGraph(graph_info.edges)
        walked = list(p.walk_back())
        index = {node: i for i, node in enumerate(walked)}
        for u, v in graph_info.edges:
            assert index[v] < index[u], f"{v.name} should appear before {u.name}"


    def test_graph_walk_excludes_starting_node(self, graph_info):
        p = PipelineGraph(graph_info.edges)
        source = graph_info.sinks[0]
        walked = list(p.walk_back(source))
        assert source not in walked
        walked = list(p.walk_fwd(source))
        assert source not in walked


def test_graph_walk_with_priority():
    test = graph_suite()["branched"]
    p = PipelineGraph(test.edges)
    walked = list(p.walk_back(sort_key=lambda x: x.func))
    assert walked == [p.task_map[i] for i in ["e", "c", "d", "b", "a"]]
    walked = list(p.walk_fwd(sort_key=lambda x: x.func))
    assert walked == [p.task_map[i] for i in ["a", "b", "c", "d", "e"]]
    # now reverse order
    walked = list(p.walk_back(sort_key=lambda x: -x.func))
    assert walked == [p.task_map[i] for i in ["e", "d", "c", "b", "a"]]
    walked = list(p.walk_fwd(sort_key=lambda x: -x.func))
    assert walked == [p.task_map[i] for i in ["a", "b", "d", "c", "e"]]
    # make sure to test with multi output, which can make priority in BFS more
    # challenging, specifically in the back direction. Critically below, nodes
    # "b" and "d" are both 2 away from the sink at minimum, but "f" is farther
    # along so it should be priority, while all sinks should still be
    # prioritized
    p = PipelineGraph(graph_suite()["branchmultiout"].edges)
    walked = list(p.walk_back(sort_key=lambda x: x.func))
    assert walked == [p.task_map[i] for i in ["c", "e", "g", "f", "d", "b", "a"]]
    walked = list(p.walk_back(sort_key=lambda x: -x.func))
    assert walked == [p.task_map[i] for i in ["g", "e", "c", "f", "d", "b", "a"]]


@pytest.mark.parametrize(
    ("graph", "expected"),
    [
        ("simple", {"a": 3, "b": 2, "c": 1, "d": 0}),
        ("branchmultiout", {"c": 0, "e": 0, "g": 0, "f": 1, "d": 2, "b": 3, "a": 4}),
    ],
)
def test_graph_ranking(graph, expected):
    g = graph_suite()[graph]
    p = PipelineGraph(g.edges)
    ranked = p.rank_nodes()
    assert min(ranked.values()) == 0
    ranked = {t.name: r for t, r in ranked.items()}
    assert ranked == expected


def test_single_node_graph_to_list():
    t = PipelineTask("t", 1)
    p = PipelineGraph([t])
    assert p.to_list() == [t]


def test_graph_instantiation_raises_for_cycles():
    a = PipelineTask("a", 1)
    b = PipelineTask("b", 1)
    with pytest.raises(ValueError):
        PipelineGraph([(a, b), (b, a)])


class TestPathsBetween:
    def test_simple_graph_single_path(self):
        g = graph_suite()["simple"]
        p = PipelineGraph(g.edges)
        paths = p.paths_between()
        assert len(paths) == 1
        # Should include terminal sentinels
        assert paths[0][0] == TRM.source
        assert paths[0][-1] == TRM.sink
        assert paths[0][1:-1] == [p.task_map[n] for n in ["a", "b", "c", "d"]]

    def test_branched_graph_two_paths(self):
        g = graph_suite()["branched"]
        p = PipelineGraph(g.edges)
        paths = p.paths_between()
        assert len(paths) == 2
        inner = sorted([[n.name for n in path[1:-1]] for path in paths])
        assert inner == [["a", "b", "c", "e"], ["a", "b", "d", "e"]]

    def test_multisource_graph_two_paths(self):
        g = graph_suite()["multisource"]
        p = PipelineGraph(g.edges)
        paths = p.paths_between()
        assert len(paths) == 2
        inner = sorted([[n.name for n in path[1:-1]] for path in paths])
        assert inner == [["a", "c", "d"], ["b", "c", "d"]]

    def test_multisink_graph_two_paths(self):
        g = graph_suite()["multisink"]
        p = PipelineGraph(g.edges)
        paths = p.paths_between()
        assert len(paths) == 2
        inner = sorted([[n.name for n in path[1:-1]] for path in paths])
        assert inner == [["a", "b", "c"], ["a", "b", "d"]]

    def test_branchmulti_graph_four_paths(self):
        g = graph_suite()["branchmulti"]
        p = PipelineGraph(g.edges)
        paths = p.paths_between()
        assert len(paths) == 4
        inner = sorted([[n.name for n in path[1:-1]] for path in paths])
        assert inner == [
            ["a", "c", "d", "f"],
            ["a", "c", "e", "g"],
            ["b", "c", "d", "f"],
            ["b", "c", "e", "g"],
        ]

    def test_branchmultiout_graph_three_paths(self):
        g = graph_suite()["branchmultiout"]
        p = PipelineGraph(g.edges)
        paths = p.paths_between()
        assert len(paths) == 3
        inner = sorted([[n.name for n in path[1:-1]] for path in paths])
        assert inner == [
            ["a", "b", "c"],
            ["a", "b", "d", "e"],
            ["a", "b", "d", "f", "g"],
        ]

    def test_custom_start_node(self):
        g = graph_suite()["branched"]
        p = PipelineGraph(g.edges)
        b = p.task_map["b"]
        paths = p.paths_between(start_node=b)
        assert len(paths) == 2
        inner = sorted([[n.name for n in path[:-1]] for path in paths])
        assert inner == [["b", "c", "e"], ["b", "d", "e"]]

    def test_custom_end_node(self):
        g = graph_suite()["branched"]
        p = PipelineGraph(g.edges)
        e = p.task_map["e"]
        paths = p.paths_between(end_node=e)
        assert len(paths) == 2
        inner = sorted([[n.name for n in path[1:]] for path in paths])
        assert inner == [["a", "b", "c", "e"], ["a", "b", "d", "e"]]

    def test_custom_start_and_end_node(self):
        g = graph_suite()["branchmultiout"]
        p = PipelineGraph(g.edges)
        b = p.task_map["b"]
        g_task = p.task_map["g"]
        paths = p.paths_between(start_node=b, end_node=g_task)
        assert len(paths) == 1
        assert [n.name for n in paths[0]] == ["b", "d", "f", "g"]


class TestCommonSource:
    def test_single_predecessor_returns_source_sentinel(self):
        """A node with one incoming path has TRM.source as common source."""
        g = graph_suite()["simple"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["b"]) == TRM.source

    def test_branched_merge_node(self):
        """Node 'e' in branched graph merges paths from 'c' and 'd'; common source is 'b'."""
        g = graph_suite()["branched"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["e"]) == p.task_map["b"]

    def test_multisource_merge_returns_source_sentinel(self):
        """Node 'c' in multisource graph merges from two source tasks; common source is TRM.source."""
        g = graph_suite()["multisource"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["c"]) == TRM.source

    def test_branchmulti_merge_at_c(self):
        """Node 'c' in branchmulti merges from two graph sources; common source is TRM.source."""
        g = graph_suite()["branchmulti"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["c"]) == TRM.source

    def test_source_task_returns_source_sentinel(self):
        """A source task has only one path (from TRM.source), so returns TRM.source."""
        g = graph_suite()["simple"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["a"]) == TRM.source

    def test_sink_task_no_merge(self):
        """Sink task in simple graph has single path, returns TRM.source."""
        g = graph_suite()["simple"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["d"]) == TRM.source

    def test_branched_non_merge_node(self):
        """Node 'c' in branched graph has only one predecessor, returns TRM.source."""
        g = graph_suite()["branched"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["c"]) == TRM.source

    def test_nested_diamond_outer_source(self):
        """In nesteddiamond graph, node 'f' merges from 'd' and 'e', which both merge from 'b', so common source is 'b'."""
        g = graph_suite()["nesteddiamond"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["g"]) == p.task_map["a"]

    def test_nested_diamond_inner_source(self):
        """In nesteddiamond graph, node 'f' merges from 'd' and 'e', which both merge from 'b', so common source is 'b'."""
        g = graph_suite()["nesteddiamond"]
        p = PipelineGraph(g.edges)
        assert p.common_source(p.task_map["f"]) == p.task_map["b"]
