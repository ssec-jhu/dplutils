from textwrap import dedent

import pandas as pd
import pytest


def test_pipeline_executor_set_config(dummy_executor):
    assert dummy_executor.tasks_idx["task1"].num_gpus != 1
    assert dummy_executor.tasks_idx["task1"].kwargs != {"test": 1}
    ret = dummy_executor.set_config({"task1": {"num_gpus": 1, "kwargs": {"test": 1}}})
    assert ret == dummy_executor
    assert dummy_executor.tasks_idx["task1"].num_gpus == 1
    assert dummy_executor.tasks_idx["task1"].kwargs == {"test": 1}
    # ensure we add to a dict-type value
    dummy_executor.set_config({"task1": {"num_gpus": 1, "kwargs": {"test2": 0}}})
    assert dummy_executor.tasks_idx["task1"].kwargs == {"test": 1, "test2": 0}
    with pytest.raises(AttributeError):
        dummy_executor.set_config({"task1": {"nonexistentconfig": 1}})
    with pytest.raises(ValueError):
        dummy_executor.set_config({"taskthatdoesnotexist": {}})


def test_pipeline_executor_set_config_from_coord(dummy_executor):
    assert dummy_executor.tasks_idx["task1"].num_gpus != 1
    assert dummy_executor.tasks_idx["task1"].kwargs != {"test": 1}
    dummy_executor.set_config("task1.num_gpus", 1).set_config("task1.kwargs.test", 1)
    assert dummy_executor.tasks_idx["task1"].num_gpus == 1
    assert dummy_executor.tasks_idx["task1"].kwargs == {"test": 1}


def test_pipeline_executor_set_config_from_file(dummy_executor, tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        dedent("""
    context:
        testcontext: ctxvalue
    config:
      task1:
        num_gpus: 1
        kwargs:
          test: 1
    """)
    )
    assert dummy_executor.tasks_idx["task1"].num_gpus != 1
    assert dummy_executor.tasks_idx["task1"].kwargs != {"test": 1}
    assert "testcontext" not in dummy_executor.ctx
    dummy_executor.set_config(from_yaml=config_file)
    assert dummy_executor.tasks_idx["task1"].num_gpus == 1
    assert dummy_executor.tasks_idx["task1"].kwargs == {"test": 1}
    assert dummy_executor.ctx["testcontext"] == "ctxvalue"


def test_pipeline_config_raises_when_no_kwargs(dummy_executor):
    with pytest.raises(ValueError):
        dummy_executor.set_config()


def test_pipeline_executor_set_context(dummy_executor):
    ret = dummy_executor.set_context("newcontext", "somevalue")
    assert ret == dummy_executor
    assert dummy_executor.ctx["newcontext"] == "somevalue"


def test_pipeline_executor_output_writer(dummy_executor, tmp_path):
    dummy_executor.writeto(tmp_path, partition_by_task=False)
    for i in range(10):
        assert (tmp_path / f"{dummy_executor.run_id}-{i}.parquet").is_file()
    data = pd.read_parquet(tmp_path / f"{dummy_executor.run_id}-{i}.parquet")
    # these test the expected data inside each batch, hence len fixed at 10
    assert len(data) == 10
    assert len(data.id) == 10
    dummy_executor.writeto(tmp_path, partition_by_task=True)
    for i in range(10):
        assert (tmp_path / "task=task2" / f"{dummy_executor.run_id}-{i}.parquet").is_file()


def test_pipeline_executor_from_list_graph(dummy_executor, dummy_steps):
    executor_class = dummy_executor.__class__
    obj = executor_class.from_graph(dummy_steps)
    assert isinstance(obj, executor_class)
    assert obj.graph.to_list() == dummy_steps


def test_validate_records_and_raises_errors(dummy_executor):
    def func_with_args(x, y):
        pass

    # patch one of the executor tasks with func with required arg
    dummy_executor.tasks_idx["task1"].func = func_with_args

    with pytest.raises(ValueError, match="Errors in validation"):
        dummy_executor.validate()


def test_executor_describe(dummy_executor):
    description = str(dummy_executor)
    assert "task1" in description
    assert "task2" in description
    assert "Required context" not in description
    # add in context to test representation for that, render only when context is required by a task
    dummy_executor.tasks_idx["task1"].context_kwargs = {"test": "testcontext"}
    description = str(dummy_executor)
    assert "Required context" in description
    assert "- testcontext" in description
    dummy_executor.ctx = {"testcontext": "testvalue", "contextnotintask": "testvaluenotintask"}
    description = str(dummy_executor)
    assert "- testcontext (set to testvalue)" in description
    assert "contextnotintask" in description
