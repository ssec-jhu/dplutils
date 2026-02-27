"""Tests for join_by_origin feature in StreamingGraphExecutor."""

import pandas as pd
import pytest

from dplutils.pipeline import PipelineGraph, PipelineTask
from dplutils.pipeline.stream import (
    LineageEntry,
    LKind,
    LocalSerialExecutor,
    StreamBatch,
    _batch_origins,
    _merge_origin_sets,
)

_UID = 0


@pytest.fixture(autouse=True)
def reset_uid():
    global _UID
    _UID = 0


@pytest.fixture
def diamond_join_graph():
    """Diamond: source -> A, source -> B, A -> join, B -> join (join_by_origin)"""
    source = PipelineTask("source", lambda x: x)
    a = PipelineTask("A", lambda x: x.assign(branch="A"))
    b = PipelineTask("B", lambda x: x.assign(branch="B"))
    join = PipelineTask("join", lambda x: x, join_by_origin=True)
    return PipelineGraph([(source, a), (source, b), (a, join), (b, join)])


@pytest.fixture
def diamond_join_graph_with_sink():
    """Diamond with a downstream sink after the join."""
    source = PipelineTask("source", lambda x: x)
    a = PipelineTask("A", lambda x: x.assign(branch="A"))
    b = PipelineTask("B", lambda x: x.assign(branch="B"))
    join = PipelineTask("join", lambda x: x, join_by_origin=True)
    sink = PipelineTask("sink", lambda x: x.assign(done=True))
    return PipelineGraph([(source, a), (source, b), (a, join), (b, join), (join, sink)])


class TestLineageEntry:
    def test_frozen(self):
        le = LineageEntry(task="source", num_segments=1, uid=0, kind=LKind.SOURCE)
        with pytest.raises(AttributeError):
            le.task = "other"

    def test_hashable(self):
        le = LineageEntry(task="source", num_segments=1, uid=0, kind=LKind.SOURCE, time_in=1)
        assert hash(le)
        assert le == LineageEntry(task="source", num_segments=1, uid=0, kind=LKind.SOURCE, time_in=1)

    def test_in_tuple(self):
        t = (
            LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE),
            LineageEntry(task="A", num_segments=1, uid=1, kind=LKind.TASK),
        )
        assert len(t) == 2

    def test_uid_distinguishes_entries(self):
        a = LineageEntry(task="A", num_segments=1, uid=5, kind=LKind.TASK)
        b = LineageEntry(task="A", num_segments=1, uid=6, kind=LKind.TASK)
        assert a != b

    def test_children_tree(self):
        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        task = LineageEntry(task="A", num_segments=1, uid=1, kind=LKind.TASK, children=(source,))
        assert task.children == (source,)
        assert task.children[0].kind == LKind.SOURCE


class TestBatchOrigins:
    """Tests for _batch_origins which returns a set of origin UIDs."""

    def test_with_mrca_task(self):
        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        mrca = LineageEntry(task="source", num_segments=2, uid=1, kind=LKind.TASK, children=(source,))
        task_a = LineageEntry(task="A", num_segments=1, uid=3, kind=LKind.TASK, children=(mrca,))
        b = StreamBatch(length=1, data=None, _lineage=task_a)
        assert _batch_origins(b, "source") == {1}

    def test_with_mrca_none_uses_source_uid(self):
        source = LineageEntry(task="", num_segments=1, uid=42, kind=LKind.SOURCE)
        b = StreamBatch(length=1, data=None, _lineage=source)
        assert _batch_origins(b, None) == {42}

    def test_skips_merge_entries(self):
        """MERGE entries are traversed but not matched as origins."""
        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        mrca = LineageEntry(task="source", num_segments=2, uid=1, kind=LKind.TASK, children=(source,))
        merge = LineageEntry(task="A", num_segments=1, uid=2, kind=LKind.MERGE, children=(mrca,))
        task_a = LineageEntry(task="A", num_segments=1, uid=3, kind=LKind.TASK, children=(merge,))
        b = StreamBatch(length=1, data=None, _lineage=task_a)
        assert _batch_origins(b, "source") == {1}

    def test_multiple_origins_from_merge(self):
        """A batch whose lineage tree has multiple mrca invocations returns all UIDs."""
        source1 = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        source2 = LineageEntry(task="", num_segments=1, uid=1, kind=LKind.SOURCE)
        mrca1 = LineageEntry(task="source", num_segments=2, uid=10, kind=LKind.TASK, children=(source1,))
        mrca2 = LineageEntry(task="source", num_segments=2, uid=20, kind=LKind.TASK, children=(source2,))
        merge = LineageEntry(task="A", num_segments=1, uid=30, kind=LKind.MERGE, children=(mrca1, mrca2))
        task_a = LineageEntry(task="A", num_segments=1, uid=31, kind=LKind.TASK, children=(merge,))
        b = StreamBatch(length=1, data=None, _lineage=task_a)
        assert _batch_origins(b, "source") == {10, 20}

    def test_returns_empty_set_for_unmatched_mrca(self):
        """If mrca_name doesn't match any entry in lineage, returns empty set."""
        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        task_a = LineageEntry(task="A", num_segments=1, uid=1, kind=LKind.TASK, children=(source,))
        b = StreamBatch(length=1, data=None, _lineage=task_a)
        assert _batch_origins(b, "nonexistent") == set()


class TestFindCompleteOrigin:
    """Tests for _find_complete_origin as a method on StreamingGraphExecutor.

    _find_complete_origin walks lineage of batches in the join task's data_in,
    checks completeness against required paths from _join_task_segments, and
    returns a set of origin UIDs when complete, or None otherwise.
    """

    def _get_join_task(self, executor):
        return [t for t in executor.stream_graph if t.name == "join"][0]

    def _make_diamond_batch(self, task_name, origin_uid, task_uid, source_uid=0):
        """Build batch with lineage for diamond graph: TASK(task_name) -> TASK("source") -> SOURCE."""
        source = LineageEntry(task="", num_segments=1, uid=source_uid, kind=LKind.SOURCE)
        mrca = LineageEntry(task="source", num_segments=2, uid=origin_uid, kind=LKind.TASK, children=(source,))
        task = LineageEntry(task=task_name, num_segments=1, uid=task_uid, kind=LKind.TASK, children=(mrca,))
        return StreamBatch(length=1, data=None, _lineage=task)

    def test_empty_data_in(self, diamond_join_graph):
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)
        assert pl._find_complete_origin(join_task) is None

    def test_complete_origin(self, diamond_join_graph):
        """Both branches present for same origin -> returns set with that origin UID."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)
        join_task.data_in.append(self._make_diamond_batch("A", origin_uid=10, task_uid=100))
        join_task.data_in.append(self._make_diamond_batch("B", origin_uid=10, task_uid=101))
        result = pl._find_complete_origin(join_task)
        assert result == {10}

    def test_complete_origin_implicit_source(self):
        """Both branches present when MRCA is the implicit source (TRM.source)."""
        a = PipelineTask("A", lambda x: x.assign(branch="A"))
        b = PipelineTask("B", lambda x: x.assign(branch="B"))
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(a, join), (b, join)])
        pl = LocalSerialExecutor(graph, max_batches=1)
        join_task = self._get_join_task(pl)
        # Lineage: TASK -> SOURCE (no intermediate MRCA task)
        source_entry = LineageEntry(task="", num_segments=1, uid=10, kind=LKind.SOURCE)
        batch_a = StreamBatch(
            length=1,
            data=None,
            _lineage=LineageEntry(task="A", num_segments=1, uid=100, kind=LKind.TASK, children=(source_entry,)),
        )
        batch_b = StreamBatch(
            length=1,
            data=None,
            _lineage=LineageEntry(task="B", num_segments=1, uid=101, kind=LKind.TASK, children=(source_entry,)),
        )
        join_task.data_in.append(batch_a)
        join_task.data_in.append(batch_b)
        result = pl._find_complete_origin(join_task)
        assert result == {10}

    def test_incomplete_origin(self, diamond_join_graph):
        """Only one branch present -> None."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)
        join_task.data_in.append(self._make_diamond_batch("A", origin_uid=10, task_uid=100))
        assert pl._find_complete_origin(join_task) is None

    def test_multiple_origins_oldest_first(self, diamond_join_graph):
        """When multiple origins exist, returns the oldest complete one."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)
        # Origin 10: complete
        join_task.data_in.append(self._make_diamond_batch("A", origin_uid=10, task_uid=100))
        join_task.data_in.append(self._make_diamond_batch("B", origin_uid=10, task_uid=101))
        # Origin 20: incomplete
        join_task.data_in.append(self._make_diamond_batch("A", origin_uid=20, task_uid=200))
        result = pl._find_complete_origin(join_task)
        assert result == {10}

    def test_extra_lineage_segments_ignored(self, diamond_join_graph):
        """MRCA lineage has num_segments=3 (extra fan-out) but only 2 paths feed the join."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)
        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        mrca = LineageEntry(task="source", num_segments=3, uid=10, kind=LKind.TASK, children=(source,))
        a = StreamBatch(
            length=1,
            data=None,
            _lineage=LineageEntry(task="A", num_segments=1, uid=100, kind=LKind.TASK, children=(mrca,)),
        )
        b = StreamBatch(
            length=1,
            data=None,
            _lineage=LineageEntry(task="B", num_segments=1, uid=101, kind=LKind.TASK, children=(mrca,)),
        )
        join_task.data_in.append(a)
        join_task.data_in.append(b)
        result = pl._find_complete_origin(join_task)
        assert result == {10}

    def test_with_split_complete(self, diamond_join_graph):
        """Split of A into 2 fragments, both present, plus B -> complete."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)

        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        mrca = LineageEntry(task="source", num_segments=2, uid=10, kind=LKind.TASK, children=(source,))
        split = LineageEntry(task="A", num_segments=2, uid=50, kind=LKind.SPLIT, children=(mrca,))
        frag1 = StreamBatch(
            length=1,
            data=None,
            _lineage=LineageEntry(task="A", num_segments=1, uid=51, kind=LKind.TASK, children=(split,)),
        )
        frag2 = StreamBatch(
            length=1,
            data=None,
            _lineage=LineageEntry(task="A", num_segments=1, uid=52, kind=LKind.TASK, children=(split,)),
        )
        b_batch = self._make_diamond_batch("B", origin_uid=10, task_uid=60)

        join_task.data_in.append(frag1)
        join_task.data_in.append(frag2)
        join_task.data_in.append(b_batch)
        result = pl._find_complete_origin(join_task)
        assert result == {10}

    def test_with_split_on_split_complete(self):
        """Split of A into 2 fragments, both present, plus B -> complete."""
        # this graph needs a series of source -> a -> c -> join to split on both
        # a and c testing the multiplicative update.
        source = PipelineTask("source", lambda x: x)
        a = PipelineTask("A", lambda x: x.assign(branch="A"))
        c = PipelineTask("C", lambda x: x.assign(branch="C"))
        b = PipelineTask("B", lambda x: x.assign(branch="B"))
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(source, a), (source, b), (a, c), (c, join), (b, join)])
        pl = LocalSerialExecutor(graph, max_batches=1)
        join_task = self._get_join_task(pl)

        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        mrca = LineageEntry(task="source", num_segments=2, uid=10, kind=LKind.TASK, children=(source,))
        split = LineageEntry(task="A", num_segments=2, uid=50, kind=LKind.SPLIT, children=(mrca,))
        a_frag1 = LineageEntry(task="A", num_segments=1, uid=51, kind=LKind.TASK, children=(split,))
        a_frag2 = LineageEntry(task="A", num_segments=1, uid=52, kind=LKind.TASK, children=(split,))
        split2_1 = LineageEntry(task="C", num_segments=2, uid=53, kind=LKind.SPLIT, children=(a_frag1,))
        split2_2 = LineageEntry(task="C", num_segments=2, uid=54, kind=LKind.SPLIT, children=(a_frag2,))
        # we should need 4 fragments of C
        c_fragments = []
        for uid, split in [(55, split2_1), (56, split2_1), (57, split2_2), (58, split2_2)]:
            cfrag = StreamBatch(
                length=1,
                data=None,
                _lineage=LineageEntry(task="C", num_segments=1, uid=uid, kind=LKind.TASK, children=(split,)),
            )
            c_fragments.append(cfrag)

        b_batch = self._make_diamond_batch("B", origin_uid=10, task_uid=60)

        join_task.data_in.append(b_batch)
        join_task.data_in.extend(c_fragments[:3])  # only 3 of 4 C fragments -> incomplete
        result = pl._find_complete_origin(join_task)
        assert result is None
        join_task.data_in.append(c_fragments[3])  # add last C fragment -> complete
        result = pl._find_complete_origin(join_task)
        assert result == {10}

    def test_split_incomplete(self, diamond_join_graph):
        """Only 1 of 2 split fragments from A -> incomplete."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)

        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        mrca = LineageEntry(task="source", num_segments=2, uid=10, kind=LKind.TASK, children=(source,))
        split = LineageEntry(task="A", num_segments=2, uid=50, kind=LKind.SPLIT, children=(mrca,))
        frag1 = StreamBatch(
            length=1,
            data=None,
            _lineage=LineageEntry(task="A", num_segments=1, uid=51, kind=LKind.TASK, children=(split,)),
        )
        b_batch = self._make_diamond_batch("B", origin_uid=10, task_uid=60)

        join_task.data_in.append(frag1)
        join_task.data_in.append(b_batch)
        assert pl._find_complete_origin(join_task) is None

    def test_merged_origins_requires_all_complete(self, diamond_join_graph):
        """A batch with merged lineage (multiple origin UIDs) requires all origins complete."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)

        # Batch from A produced by merging two source invocations
        src1 = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        src2 = LineageEntry(task="", num_segments=1, uid=1, kind=LKind.SOURCE)
        mrca1 = LineageEntry(task="source", num_segments=2, uid=10, kind=LKind.TASK, children=(src1,))
        mrca2 = LineageEntry(task="source", num_segments=2, uid=20, kind=LKind.TASK, children=(src2,))
        merge = LineageEntry(task="A", num_segments=1, uid=30, kind=LKind.MERGE, children=(mrca1, mrca2))
        a_merged = StreamBatch(
            length=1,
            data=None,
            _lineage=LineageEntry(task="A", num_segments=1, uid=31, kind=LKind.TASK, children=(merge,)),
        )

        # B for origin 10 only -> not yet complete (missing B for origin 20)
        b_10 = self._make_diamond_batch("B", origin_uid=10, task_uid=40)
        join_task.data_in.append(a_merged)
        join_task.data_in.append(b_10)
        assert pl._find_complete_origin(join_task) is None

        # Add B for origin 20 -> now complete
        b_20 = self._make_diamond_batch("B", origin_uid=20, task_uid=41)
        join_task.data_in.append(b_20)
        result = pl._find_complete_origin(join_task)
        assert result == {10, 20}


class TestHandleJoinTask:
    """Tests for _handle_join_task submission logic."""

    def _get_join_task(self, executor):
        return [t for t in executor.stream_graph if t.name == "join"][0]

    def test_eligible_but_not_submittable(self, diamond_join_graph):
        """Complete origin exists but executor refuses submission -> (True, False)."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = self._get_join_task(pl)

        # Build two batches with matching origin so the origin is complete
        source = LineageEntry(task="", num_segments=1, uid=0, kind=LKind.SOURCE)
        mrca = LineageEntry(task="source", num_segments=2, uid=10, kind=LKind.TASK, children=(source,))
        join_task.data_in.append(
            StreamBatch(
                length=1,
                data=None,
                _lineage=LineageEntry(task="A", num_segments=1, uid=100, kind=LKind.TASK, children=(mrca,)),
            )
        )
        join_task.data_in.append(
            StreamBatch(
                length=1,
                data=None,
                _lineage=LineageEntry(task="B", num_segments=1, uid=101, kind=LKind.TASK, children=(mrca,)),
            )
        )

        # Make the executor refuse submissions (sflag=1 -> task_submittable returns False)
        pl.sflag = 1
        eligible, submitted = pl._handle_join_task(join_task, rank=0)
        assert eligible is True
        assert submitted is False
        # Batches should remain in data_in (not consumed)
        assert len(join_task.data_in) == 2


class TestMRCAComputation:
    """Test that the MRCA is correctly computed for join nodes."""

    def test_simple_diamond_mrca_is_source(self, diamond_join_graph):
        """source -> A, source -> B, A -> join, B -> join  =>  MRCA = source."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = [t for t in pl.stream_graph if t.name == "join"][0]
        assert pl._join_task_segments[join_task][0][0] == "source"

    def test_deeper_diamond_mrca(self):
        """source -> A -> B -> C, B -> D, C -> join, D -> join  =>  MRCA = B."""
        source = PipelineTask("source", lambda x: x)
        a = PipelineTask("A", lambda x: x)
        b = PipelineTask("B", lambda x: x)
        c = PipelineTask("C", lambda x: x)
        d = PipelineTask("D", lambda x: x)
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(source, a), (a, b), (b, c), (b, d), (c, join), (d, join)])
        pl = LocalSerialExecutor(graph, max_batches=1)
        join_task = [t for t in pl.stream_graph if t.name == "join"][0]
        assert pl._join_task_segments[join_task][0][0] == "B"

    def test_task_segments_stores_paths(self, diamond_join_graph):
        """_join_task_segments should store paths (by task name) between origin and join."""
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=1)
        join_task = [t for t in pl.stream_graph if t.name == "join"][0]
        paths = pl._join_task_segments[join_task]
        # Paths are lists of task names from source up to (but not including) join
        assert sorted([tuple(p) for p in paths]) == sorted(
            [
                ("source", "A"),
                ("source", "B"),
            ]
        )

    def test_deeper_diamond_task_segments(self):
        """Deeper graph: source->A->B->C->join, B->D->join => paths through B."""
        source = PipelineTask("source", lambda x: x)
        a = PipelineTask("A", lambda x: x)
        b = PipelineTask("B", lambda x: x)
        c = PipelineTask("C", lambda x: x)
        d = PipelineTask("D", lambda x: x)
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(source, a), (a, b), (b, c), (b, d), (c, join), (d, join)])
        pl = LocalSerialExecutor(graph, max_batches=1)
        join_task = [t for t in pl.stream_graph if t.name == "join"][0]
        paths = pl._join_task_segments[join_task]
        assert sorted([tuple(p) for p in paths]) == sorted(
            [
                ("B", "C"),
                ("B", "D"),
            ]
        )

    def test_join_with_one_segments_ignores_joins(self):
        """If join is directly after source, the single segment should be the source, not the join."""
        source = PipelineTask("source", lambda x: x)
        a = PipelineTask("A", lambda x: x)
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(source, a), (a, join)])
        pl = LocalSerialExecutor(graph, max_batches=1)
        # With one path we don't need to trigger join logic
        assert pl._join_task_segments == {}


class TestJoinByOriginDiamond:
    """End-to-end tests with a diamond graph and join_by_origin."""

    def test_basic_diamond_join(self, diamond_join_graph):
        """Batches from the same source invocation are grouped at the join node."""

        def generator():
            for i in range(3):
                yield pd.DataFrame({"val": [i]})

        pl = LocalSerialExecutor(diamond_join_graph, max_batches=3, generator=generator)
        results = [b.data for b in pl.run()]
        assert len(results) == 3  # 3 batches from generator, the extra batches from fork/splits get combined
        final = pd.concat(results, ignore_index=True)
        assert len(final) == 6  # 3 origins * 2 branches
        assert set(final["branch"]) == {"A", "B"}

    def test_diamond_no_cross_origin_mixing(self, diamond_join_graph):
        """Different MRCA invocations should not be mixed in the same join submission."""

        submissions = []
        original_submit = LocalSerialExecutor.task_submit

        class TrackingExecutor(LocalSerialExecutor):
            def task_submit(self, task, df_list):
                if task.name == "join":
                    submissions.append(pd.concat(df_list))
                return original_submit(self, task, df_list)

        def generator():
            for i in range(4):
                yield pd.DataFrame({"val": [i]})

        pl = TrackingExecutor(diamond_join_graph, max_batches=4, generator=generator)
        list(pl.run())

        for df in submissions:
            assert df["val"].nunique() == 1, f"Cross-origin mixing detected: {df['val'].tolist()}"

    def test_diamond_with_downstream_sink(self, diamond_join_graph_with_sink):
        """Join node followed by another task works correctly."""

        def generator():
            for i in range(2):
                yield pd.DataFrame({"val": [i]})

        pl = LocalSerialExecutor(diamond_join_graph_with_sink, max_batches=2, generator=generator)
        results = [b.data for b in pl.run()]
        final = pd.concat(results, ignore_index=True)
        assert len(final) == 4  # 2 origins * 2 branches
        assert "done" in final.columns
        assert all(final["done"])


class TestJoinByOriginWithSplits:
    """Test join_by_origin when upstream tasks split batches."""

    def test_split_then_join(self):
        """Upstream split produces multiple fragments; join waits for all."""
        source = PipelineTask("source", lambda x: x)
        a = PipelineTask("A", lambda x: x.assign(branch="A"), batch_size=1)
        b = PipelineTask("B", lambda x: x.assign(branch="B"))
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(source, a), (source, b), (a, join), (b, join)])

        def generator():
            for i in range(2):
                yield pd.DataFrame({"val": [i, i]})

        pl = LocalSerialExecutor(graph, max_batches=2, generator=generator)
        results = [b.data for b in pl.run()]
        assert len(results) == 2  # 2 source batches, each split into 2 fragments by A, but join combines them
        final = pd.concat(results, ignore_index=True)
        assert len(final) == 8  # 2 origins * (2 rows from A + 2 rows from B)
        assert set(final["branch"]) == {"A", "B"}


class TestJoinByOriginExhaustion:
    """Test that pipelines with join_by_origin terminate correctly."""

    def test_exhaustion(self, diamond_join_graph):
        def generator():
            yield pd.DataFrame({"val": [1]})

        pl = LocalSerialExecutor(diamond_join_graph, generator=generator)
        results = list(pl.run())
        assert len(results) == 1
        assert len(results[0].data) == 2

    def test_exhaustion_with_max_batches(self, diamond_join_graph):
        pl = LocalSerialExecutor(diamond_join_graph, max_batches=5)
        results = list(pl.run())
        final = pd.concat([b.data for b in results], ignore_index=True)
        assert len(final) == 10  # 5 origins * 2 branches


class TestNoJoinByOriginUnchanged:
    """When join_by_origin is not used, behavior should be identical to before."""

    def test_simple_pipeline(self):
        steps = [
            PipelineTask("t1", lambda x: x.assign(step1=1)),
            PipelineTask("t2", lambda x: x.assign(step2=2)),
        ]
        pl = LocalSerialExecutor(steps, max_batches=3)
        results = list(pl.run())
        assert len(results) == 3

    def test_diamond_without_join(self):
        t1 = PipelineTask("t1", lambda x: x.assign(t1="1"))
        t2A = PipelineTask("t2A", lambda x: x.assign(t2A="2A"))
        t2B = PipelineTask("t2B", lambda x: x.assign(t2B="2B"))
        t3 = PipelineTask("t3", lambda x: x)
        graph = PipelineGraph([(t1, t2A), (t1, t2B), (t2A, t3), (t2B, t3)])
        pl = LocalSerialExecutor(graph, max_batches=2)
        results = list(pl.run())
        final = pd.concat([b.data for b in results], ignore_index=True)
        assert len(final) == 4  # 2 batches * 2 branches

    def test_lineage_always_active(self):
        """Lineage is tracked even without join_by_origin in the graph."""
        steps = [PipelineTask("t1", lambda x: x)]
        pl = LocalSerialExecutor(steps, max_batches=2)
        # Run and inspect: override resolve_completed to capture lineage
        observed = []
        orig = type(pl).resolve_completed

        def intercept(self_):
            for task in self_.stream_graph.walk_fwd():
                for b in task.pending:
                    observed.append(b._lineage)
            return orig(self_)

        type(pl).resolve_completed = intercept
        try:
            list(pl.run())
        finally:
            type(pl).resolve_completed = orig
        assert len(observed) > 0
        assert all(lin is not None for lin in observed)


class TestCrossOriginMergeUpstream:
    """Upstream tasks can freely merge batches from different sources when the
    MRCA is downstream of the merge point."""

    def test_upstream_merge_then_join(self):
        """When MRCA merges source batches, join groups by MRCA invocation."""
        source = PipelineTask("source", lambda x: x, batch_size=200)
        a = PipelineTask("A", lambda x: x.assign(branch="A"))
        b = PipelineTask("B", lambda x: x.assign(branch="B"))
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(source, a), (source, b), (a, join), (b, join)])

        def generator():
            for i in range(4):
                yield pd.DataFrame({"val": [i]})

        pl = LocalSerialExecutor(graph, max_batches=4, generator=generator)
        results = list(pl.run())
        final = pd.concat([b.data for b in results], ignore_index=True)
        # All 4 source batches should produce output through both branches
        assert set(final["branch"]) == {"A", "B"}
        # Total rows: each source yields 1 row, goes through A and B
        assert len(final) == 8


class TestDeeperDiamondJoin:
    """Test join with MRCA deeper in the graph (not the source)."""

    def test_deeper_mrca_groups_correctly(self):
        """source -> A -> B (fan-out) -> C -> join, B -> D -> join. MRCA=B."""
        source = PipelineTask("source", lambda x: x)
        a = PipelineTask("A", lambda x: x)
        b = PipelineTask("B", lambda x: x)
        c = PipelineTask("C", lambda x: x.assign(branch="C"))
        d = PipelineTask("D", lambda x: x.assign(branch="D"))
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(source, a), (a, b), (b, c), (b, d), (c, join), (d, join)])

        def generator():
            for i in range(3):
                yield pd.DataFrame({"val": [i]})

        pl = LocalSerialExecutor(graph, max_batches=3, generator=generator)
        results = list(pl.run())
        final = pd.concat([b.data for b in results], ignore_index=True)
        assert len(final) == 6  # 3 B-invocations * 2 branches
        assert set(final["branch"]) == {"C", "D"}

    def test_deeper_mrca_no_cross_invocation_mixing(self):
        """Each join submission should only contain data from a single B invocation."""
        source = PipelineTask("source", lambda x: x)
        a = PipelineTask("A", lambda x: x)
        b = PipelineTask("B", lambda x: x)
        c = PipelineTask("C", lambda x: x.assign(branch="C"))
        d = PipelineTask("D", lambda x: x.assign(branch="D"))
        join = PipelineTask("join", lambda x: x, join_by_origin=True)
        graph = PipelineGraph([(source, a), (a, b), (b, c), (b, d), (c, join), (d, join)])

        submissions = []
        original_submit = LocalSerialExecutor.task_submit

        class TrackingExecutor(LocalSerialExecutor):
            def task_submit(self, task, df_list):
                if task.name == "join":
                    submissions.append(pd.concat(df_list))
                return original_submit(self, task, df_list)

        def generator():
            for i in range(3):
                yield pd.DataFrame({"val": [i]})

        pl = TrackingExecutor(graph, max_batches=3, generator=generator)
        list(pl.run())

        for df in submissions:
            assert df["val"].nunique() == 1, f"Cross-invocation mixing: {df['val'].tolist()}"


class TestMergeOriginSets:
    """Tests for _merge_origin_sets which merges overlapping sets into disjoint groups."""

    def test_empty_input(self):
        assert _merge_origin_sets([]) == []

    def test_single_set(self):
        assert _merge_origin_sets([{1, 2}]) == [{1, 2}]

    def test_disjoint_sets_unchanged(self):
        result = _merge_origin_sets([{1}, {2}, {3}])
        assert result == [{1}, {2}, {3}]

    def test_two_overlapping_sets_merged(self):
        result = _merge_origin_sets([{1, 2}, {2, 3}])
        assert result == [{1, 2, 3}]

    def test_all_disjoint_results(self):
        """All returned sets must be mutually disjoint."""
        inputs = [{1, 2}, {3, 4}, {2, 5}, {6}, {4, 7}]
        result = _merge_origin_sets(inputs)
        for i, a in enumerate(result):
            for b in result[i + 1 :]:
                assert a.isdisjoint(b), f"Sets {a} and {b} are not disjoint"

    def test_cascading_merge(self):
        """Merging a new set may connect two previously disjoint groups.

        {1,2} and {3,4} are disjoint, but {2,3} bridges them."""
        result = _merge_origin_sets([{1, 2}, {3, 4}, {2, 3}])
        assert len(result) == 1
        assert result[0] == {1, 2, 3, 4}

    def test_cascading_merge_three_groups(self):
        """A single set bridges three previously disjoint groups."""
        result = _merge_origin_sets([{1}, {3}, {5}, {1, 3, 5}])
        assert len(result) == 1
        assert result[0] == {1, 3, 5}

    def test_cascading_merge_chain(self):
        """Chain of merges: each new set connects to the previous merged result."""
        result = _merge_origin_sets([{1, 2}, {3, 4}, {5, 6}, {2, 3}, {4, 5}])
        assert len(result) == 1
        assert result[0] == {1, 2, 3, 4, 5, 6}

    def test_preserves_insertion_order(self):
        """Merged groups appear in the order their first element was seen."""
        result = _merge_origin_sets([{10}, {20}, {30}])
        assert result == [{10}, {20}, {30}]

    def test_preserves_order_after_merge(self):
        """When sets merge, the merged result takes the position of the earliest contributor."""
        result = _merge_origin_sets([{1}, {2}, {3}, {2, 3}])
        # {2} and {3} merge; the merged set replaces those positions, {1} stays first
        assert result[0] == {1}
        assert {2, 3}.issubset(result[1])
        assert len(result) == 2

    def test_preserves_order_complex(self):
        """Merge ordering: non-overlapping sets keep relative order, merged set is placed last."""
        result = _merge_origin_sets([{1}, {5}, {10}, {1, 10}])
        # {1, 10} absorbs {1} and {10}; {5} (disjoint) retains its position
        assert result[0] == {5}
        assert result[1] == {1, 10}

    def test_all_overlap(self):
        result = _merge_origin_sets([{1, 2}, {2, 3}, {3, 4}, {4, 5}])
        assert len(result) == 1
        assert result[0] == {1, 2, 3, 4, 5}

    def test_union_covers_all_input_elements(self):
        """The union of all output sets equals the union of all input sets."""
        inputs = [{1, 2}, {3}, {2, 4}, {5, 6}, {6, 3}]
        result = _merge_origin_sets(inputs)
        assert set().union(*result) == set().union(*inputs)
        # Verify disjointness
        for i, a in enumerate(result):
            for b in result[i + 1 :]:
                assert a.isdisjoint(b)

    def test_duplicate_sets(self):
        result = _merge_origin_sets([{1, 2}, {1, 2}])
        assert result == [{1, 2}]

    def test_subset_absorbed(self):
        result = _merge_origin_sets([{1, 2, 3}, {2}])
        assert result == [{1, 2, 3}]

    def test_late_bridge_merges_early_groups(self):
        """Groups that were processed and settled get merged by a later bridging set."""
        result = _merge_origin_sets([{1}, {2}, {3}, {4}, {1, 4}])
        assert len(result) == 3
        assert result[0] == {2}
        assert result[1] == {3}
        assert result[2] == {1, 4}
