import pytest
import pandas as pd


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
                assert isinstance(res, pd.DataFrame)
                assert set(res.columns).issuperset({'id', 'step1', 'step2'})

    def test_run_dag_pipeline(self, dummy_pipeline_graph, max_batches):
        pl = self.executor(dummy_pipeline_graph, max_batches = max_batches)
        it = pl.run()
        total_df = pd.concat([b for b in it])
        assert set(total_df.columns).issuperset({'id', 't1', 't2A', 't2B', 't3'})
        # in this pipeline we don't expand the result size, but the forked dag adds
        # another batch
        assert len(total_df) == 2 * max_batches

    def test_with_split_batch(self, dummy_steps, max_batches):
        pl = self.executor(dummy_steps, max_batches=max_batches)
        it = pl.set_config('task2.batch_size', 5).run()
        res = list(it)
        assert all([len(i) == 5 for i in res])
        final = pd.concat(res)
        assert final['id'].nunique() == max_batches

    def test_with_merge_batch(self, dummy_steps, max_batches):
        pl = self.executor(dummy_steps, max_batches=max_batches)
        it = pl.set_config('task2.batch_size', 20).run()
        res = list(it)
        expected_len = 20 if max_batches > 1 else 10
        assert all([len(i) == expected_len for i in res])
        final = pd.concat(res)
        assert final['id'].nunique() == max_batches
