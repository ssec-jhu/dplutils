import pytest
import pandas as pd
from dplutils.pipeline import OutputBatch


@pytest.mark.parametrize('max_batches', (1, 4, 10))
class PipelineExecutorTestSuite:
    executor = None

    def test_run_simple_pipeline_iterator(self, dummy_steps, max_batches):
        pl = self.executor(dummy_steps, max_batches=max_batches)
        it = pl.run()
        for c in range(2):
            if max_batches < c+1:
                with pytest.raises(StopIteration):
                    next(it)
            else:
                res = next(it)
                assert isinstance(res, OutputBatch)
                assert res.task == 'task2'
                assert isinstance(res.data, pd.DataFrame)
                assert set(res.data.columns).issuperset({'id', 'step1', 'step2'})

    def test_run_dag_pipeline(self, dummy_pipeline_graph, max_batches):
        pl = self.executor(dummy_pipeline_graph, max_batches = max_batches)
        it = pl.run()
        total_df = pd.concat([b.data for b in it])
        assert set(total_df.columns).issuperset({'id', 't1', 't2A', 't2B', 't3'})
        # in this pipeline we don't expand the result size, but the forked dag adds
        # another batch
        assert len(total_df) == 2 * max_batches

    @pytest.mark.parametrize('partition_by_task', [None, True, False])
    @pytest.mark.parametrize('graph_type, factor', [('dummy_steps', 1), ('dummy_pipeline_graph', 2), ('multi_output_graph', 2)])
    def test_write_pipeline(self, partition_by_task, graph_type, factor, graph_suite, tmp_path, max_batches):
        pl = self.executor(graph_suite[graph_type], max_batches=max_batches)
        pl.writeto(tmp_path, partition_by_task=partition_by_task)
        # these two graphs have only one output
        if graph_type in ['dummy_steps', 'dummy_pipeline_graph']:
            sink = pl.graph.sink_tasks[0].name
            part_path = tmp_path / f'task={sink}'
            if partition_by_task is None or not partition_by_task:
                assert not part_path.exists() and not part_path.is_dir()
                assert len(list(tmp_path.glob('*.parquet'))) == max_batches*factor
            else:
                assert part_path.is_dir()
                assert len(list(part_path.glob('*.parquet'))) == max_batches*factor
        # other graph has multiple outputs:
        else:
            sinks = [i.name for i in pl.graph.sink_tasks]
            if partition_by_task is False:
                assert len(list(tmp_path.glob('*.parquet'))) == max_batches*factor
            else:
                for sink in sinks:
                    part_path = tmp_path / f'task={sink}'
                    assert part_path.is_dir()
                    assert len(list(part_path.glob('*.parquet'))) == max_batches

    def test_with_split_batch(self, dummy_steps, max_batches):
        pl = self.executor(dummy_steps, max_batches=max_batches)
        it = pl.set_config('task2.batch_size', 5).run()
        res = list(it)
        assert all([len(i.data) == 5 for i in res])
        final = pd.concat([i.data for i in res])
        assert final['id'].nunique() == max_batches

    def test_with_merge_batch(self, dummy_steps, max_batches):
        pl = self.executor(dummy_steps, max_batches=max_batches)
        it = pl.set_config('task2.batch_size', 20).run()
        res = list(it)
        expected_len = 20 if max_batches > 1 else 10
        assert all([len(i.data) == expected_len for i in res])
        final = pd.concat([i.data for i in res])
        assert final['id'].nunique() == max_batches
