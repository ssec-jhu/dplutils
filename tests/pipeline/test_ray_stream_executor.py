import pandas as pd
import pytest
import ray
from test_suite import PipelineExecutorTestSuite

import dplutils.pipeline.ray
from dplutils.pipeline import PipelineTask
from dplutils.pipeline.ray import RayStreamGraphExecutor, get_stream_wrapper, stream_split_func
from dplutils.pipeline.stream import StreamBatch


@pytest.fixture(autouse=True)
def rayinit(raysession):
    pass


class TestRayStreamingGraphExecutor(PipelineExecutorTestSuite):
    executor = RayStreamGraphExecutor


def test_ray_stream_wrapper_func(pipelinestartdf):
    def myfunc(df, ctx_in=None):
        return df.assign(ctx_in=ctx_in)

    task = PipelineTask("task_name", myfunc, context_kwargs={"ctx": "ctx_in"})
    wrapper = get_stream_wrapper(task, {"ctx": "test_ctx"})
    dfs = [pipelinestartdf, pipelinestartdf.assign(batch_id=1)]
    res_len, res_df = wrapper(*dfs)
    assert res_len == 2 == len(res_df)


def test_ray_steam_splitter_func():
    df = pd.DataFrame({"id": range(10)})
    ll = stream_split_func(df, 3)
    lens = ll[:3]
    df_list = ll[3:]
    assert sum(lens) == 10 == sum(len(i) for i in df_list)
    assert sorted(lens) == [3, 3, 4] == sorted(len(i) for i in df_list)
    assert set(pd.concat(df_list)["id"]) == set(df["id"])


def test_ray_stream_ray_autoinit(monkeypatch, dummy_steps):
    class raymock:
        inited = False

        def is_initialized(self):
            return self.inited

        def init(self):
            self.inited = True

    im = raymock()
    monkeypatch.setattr(dplutils.pipeline.ray.ray, "is_initialized", im.is_initialized)
    monkeypatch.setattr(dplutils.pipeline.ray.ray, "init", im.init)
    pl = RayStreamGraphExecutor(dummy_steps)
    assert not im.is_initialized()
    next(pl.run())
    assert im.is_initialized()


def test_ray_stream_remote_tasks_configuration_applies():
    def get_runtime_env(x):
        ncpu = ray.runtime_context.get_runtime_context().get_assigned_resources()["CPU"]
        return x.assign(ncpu=ncpu)

    pl = RayStreamGraphExecutor([PipelineTask("task", get_runtime_env)], max_batches=1)
    res_df = next(pl.run()).data
    assert res_df.iloc[0].ncpu == 1
    res_df = next(pl.set_config("task.num_cpus", 0.1).run()).data
    assert res_df.iloc[0].ncpu == 0.1


def test_ray_stream_ignores_resources_with_zero_requests(dummy_steps):
    pl = RayStreamGraphExecutor(dummy_steps)
    pl.set_config("task1.resources.someresource", 1)
    # we use rank 1 to ensure that it wont try to oversubscribe the resource
    # (the cluster should have none)
    assert not pl.task_submittable(pl.tasks_idx["task1"], 1)
    # now we explicitly set to zero, the resource should still be specified in
    # the config, but we should ignore it in scheduling due to zero request
    pl.set_config("task1.resources.someresource", 0)
    assert pl.task_submittable(pl.tasks_idx["task1"], 1)


@pytest.fixture
def gpu_pipeline(monkeypatch):
    monkeypatch.setattr(dplutils.pipeline.ray.ray, "cluster_resources", lambda: {"CPU": 64.0, "GPU": 1.0})
    tasks = [
        PipelineTask("source", lambda x: x, num_cpus=8, num_gpus=1),
        PipelineTask("next", lambda x: x, num_cpus=8, num_gpus=1, batch_size=10),
    ]
    return RayStreamGraphExecutor(tasks)


def test_ray_stream_source_never_overcommits(gpu_pipeline):
    source = gpu_pipeline.tasks_idx["source"]
    assert gpu_pipeline.task_submittable(source, 0)
    # with the single gpu taken, the source must not overcommit, even though
    # nothing downstream is eligible
    gpu_pipeline._claim_resources(source)
    assert not gpu_pipeline.task_submittable(source, 0)


def test_ray_stream_warns_once_for_oversized_task(gpu_pipeline, caplog):
    gpu_pipeline.set_config("source.num_gpus", 4)
    source = gpu_pipeline.tasks_idx["source"]
    with caplog.at_level("WARNING"):
        for _ in range(5):
            assert not gpu_pipeline.task_submittable(source, 0)
        assert len(caplog.records) == 1
        assert "requests num_gpus=4 but cluster has 1.0 in total" in caplog.records[0].message
        # a change in capacity (e.g. an expanded cluster) warns again
        caplog.clear()
        gpu_pipeline.warn_oversized(source, "num_gpus", 4, 2.0)
        assert len(caplog.records) == 1


def test_ray_stream_split_claims_target_task_resources(gpu_pipeline):
    next_task = gpu_pipeline.tasks_idx["next"]
    gpu_pipeline.split_batch_submit(next_task, StreamBatch(100, pd.DataFrame({"id": range(100)})), 10)
    # resources are claimed for the task the splits feed, so the source, which
    # would otherwise fit, must wait
    assert gpu_pipeline.sched_resources == {"num_cpus": 8, "num_gpus": 1}
    assert not gpu_pipeline.task_submittable(gpu_pipeline.tasks_idx["source"], 0)
