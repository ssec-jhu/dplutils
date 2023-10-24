import pytest
import pandas as pd


def test_pipeline_executor_set_config(dummy_executor):
    assert dummy_executor.tasks_idx['task1'].num_gpus != 1
    assert dummy_executor.tasks_idx['task1'].kwargs != {'test': 1}
    ret = dummy_executor.set_config({'task1': {'num_gpus': 1, 'kwargs': {'test': 1}}})
    assert ret == dummy_executor
    assert dummy_executor.tasks_idx['task1'].num_gpus == 1
    assert dummy_executor.tasks_idx['task1'].kwargs == {'test': 1}
    # ensure we add to a dict-type value
    dummy_executor.set_config({'task1': {'num_gpus': 1, 'kwargs': {'test2': 0}}})
    assert dummy_executor.tasks_idx['task1'].kwargs == {'test': 1, 'test2': 0}
    with pytest.raises(AttributeError):
        dummy_executor.set_config({'task1': {'nonexistentconfig': 1}})
    with pytest.raises(ValueError):
        dummy_executor.set_config({'taskthatdoesnotexist': {}})


def test_pipeline_executor_set_config_from_coord(dummy_executor):
    assert dummy_executor.tasks_idx['task1'].num_gpus != 1
    assert dummy_executor.tasks_idx['task1'].kwargs != {'test': 1}
    dummy_executor.set_config('task1.num_gpus', 1).set_config('task1.kwargs.test', 1)
    assert dummy_executor.tasks_idx['task1'].num_gpus == 1
    assert dummy_executor.tasks_idx['task1'].kwargs == {'test': 1}


def test_pipeline_executor_set_config_from_file(dummy_executor, tmp_path):
    config_file = tmp_path / 'config.yaml'
    config_file.write_text('''
    task1:
      num_gpus: 1
      kwargs:
        test: 1
    ''')
    assert dummy_executor.tasks_idx['task1'].num_gpus != 1
    assert dummy_executor.tasks_idx['task1'].kwargs != {'test': 1}
    dummy_executor.set_config(from_yaml=config_file)
    assert dummy_executor.tasks_idx['task1'].num_gpus == 1
    assert dummy_executor.tasks_idx['task1'].kwargs == {'test': 1}


def test_pipeline_executor_set_context(dummy_executor):
    ret = dummy_executor.set_context('newcontext', 'somevalue')
    assert ret == dummy_executor
    assert dummy_executor.ctx['newcontext'] == 'somevalue'


def test_pipeline_executor_output_writer(dummy_executor, tmp_path):
    dummy_executor.writeto(tmp_path)
    for i in range(10):
        assert (tmp_path / f'{dummy_executor.run_id}-{i}.parquet').is_file()
    data = pd.read_parquet(tmp_path / f'{dummy_executor.run_id}-{i}.parquet')
    # these test the expected data inside each batch, hence len fixed at 10
    assert len(data) == 10
    assert len(data.id) == 10
