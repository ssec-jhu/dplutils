import pandas as pd
import pytest
import ray
from ray.exceptions import RayTaskError

from dplutils import observer
from dplutils.observer.ray import RayActorWrappedObserver, RayMetricsObserver
from dplutils.pipeline import OutputBatch, PipelineTask
from dplutils.pipeline.ray import RayDataPipelineExecutor, get_remote_wrapper, set_run_id


@pytest.fixture
def dummy_ray_pipeline(dummy_steps):
    return RayDataPipelineExecutor(dummy_steps, n_batches=10)


@pytest.fixture
def observer_pipeline():
    def observer_task(indf):
        observer.observe("gauge", len(indf))
        observer.increment("counter")
        with observer.timer("timer"):
            pass
        return indf

    return RayDataPipelineExecutor([PipelineTask("task", observer_task)], n_batches=10)


@pytest.mark.parametrize("task_batch_size", [None, 1])
def test_ray_actor_wrapped_observer(observer_pipeline, raysession, task_batch_size, tmp_path):
    obs = RayActorWrappedObserver(observer.InMemoryObserver)
    obs._wait = True
    observer.set_observer(obs)
    data = ray.get(observer.get_observer().actor.dump.remote())["metrics"]
    assert "gauge" not in data
    assert "counter" not in data
    for batch in observer_pipeline.set_config("task.batch_size", task_batch_size).run():
        pass
    data = ray.get(observer.get_observer().actor.dump.remote())["metrics"]
    assert "gauge" in data
    assert "counter" in data
    assert "timer" in data
    assert len(data["gauge"]) == observer_pipeline.n_batches
    assert data["gauge"][-1][1] == 1
    assert data["counter"][-1][1] == observer_pipeline.n_batches


@pytest.mark.parametrize("task_batch_size", [None, 1])
def test_ray_metrics_observer(observer_pipeline, raysession, task_batch_size):
    observer.set_observer(RayMetricsObserver())
    for batch in observer_pipeline.set_config("task.batch_size", task_batch_size).run():
        pass


@pytest.mark.parametrize("task_batch_size", [None, 1])
def test_ray_metrics_observer_incompatible_type_excepts(raysession, task_batch_size):
    observer.set_observer(RayMetricsObserver())

    def confused_task(indf):
        observer.observe("gauge", 1)
        observer.increment("counter")
        observer.increment("gauge")
        return indf

    pl = RayDataPipelineExecutor([PipelineTask("task", confused_task)], n_batches=2)
    # for ray 2.10+ unfortunately ray data no longer unfolds exception, and
    # in fact doesn't look like a way to get at it
    with pytest.raises((TypeError, RayTaskError)):
        for batch in pl.set_config("task.batch_size", task_batch_size).run():
            pass


def test_pipeline_create_returns_ray_dataset(dummy_ray_pipeline, raysession):
    pl = dummy_ray_pipeline.make_pipeline()
    assert isinstance(pl, ray.data.Dataset)


def test_pipeline_run_dummy_runs_steps_and_generates_outputs(dummy_ray_pipeline, raysession):
    it = dummy_ray_pipeline.run()
    batch1 = next(it).data
    assert isinstance(batch1, pd.DataFrame)
    assert batch1["run_id"].iloc[0] == dummy_ray_pipeline.run_id
    assert len(batch1) == 10
    # we expect first step to take 1 row, just identifiers, and generate batch of size
    assert batch1["step1"].iloc[0] == 1
    # the other steps should see that batch size
    if dummy_ray_pipeline.n_batches > 1:
        batch2 = next(it).data
        assert batch1["batch_id"].iloc[0] != batch2["batch_id"].iloc[0]
        assert len(batch2) == 10
        assert batch2["step2"].iloc[0] == 10


def test_pipeline_splits_tasks_into_separate_remotes_with_context(raysession, test_file):
    def expand(indf):
        return pd.DataFrame({"id": range(100)})

    def task_func(indf, ctxdata):
        indf["task_id"] = ray.get_runtime_context().get_task_id()
        indf["ctxdata"] = ctxdata.read_text()
        return indf

    it = (
        RayDataPipelineExecutor(
            [
                PipelineTask("in", expand),
                PipelineTask("task", task_func, context_kwargs={"ctxdata": "testfile"}, batch_size=10),
            ]
        )
        .set_context("testfile", test_file)
        .run()
    )
    batch = next(it)
    assert isinstance(batch, OutputBatch)
    assert batch.task == "task"
    assert len(set(batch.data["task_id"])) == 10
    assert batch.data["ctxdata"].iloc[0] == "TESTDATA\n"


def test_pipeline_non_split_task_has_access_to_context(raysession, test_file):
    def task_func(indf, ctxdata):
        indf["ctxdata"] = ctxdata.read_text()
        return indf

    it = (
        RayDataPipelineExecutor([PipelineTask("task", task_func, context_kwargs={"ctxdata": "testfile"})])
        .set_context("testfile", test_file)
        .run()
    )
    batch = next(it).data
    assert batch["ctxdata"].iloc[0] == "TESTDATA\n"


def test_remote_wrapper_sets_name_based_on_task_info():
    def funcname():
        pass

    wrapper = get_remote_wrapper(PipelineTask("taskname", funcname), None)
    assert wrapper.__name__ == "taskname<funcname>"


@pytest.mark.parametrize("batch_size", [None, 1, 10])
def test_remote_wrapper_wrapped_func(raysession, batch_size):
    def func(arg):
        return arg

    wrapper = get_remote_wrapper(PipelineTask("taskname", func, batch_size=batch_size), None)
    df_in = pd.DataFrame({"col": range(20)})
    res = wrapper(df_in)
    assert len(res) == 20
    assert set(res.columns) == set(df_in.columns)
    assert res.compare(df_in).empty


def test_pipeline_bootstrap_set_run_id():
    # the function is specific to ray.data.range behaviour, and expects dataframe with id column
    df_in = pd.DataFrame({"id": [1]})
    df_out = set_run_id(df_in, "runid")
    assert df_out.batch_id.iloc[0] == 1
    assert df_out.run_id.iloc[0] == "runid"
