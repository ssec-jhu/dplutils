import pandas as pd
import pytest
import ray
from test_suite import PipelineExecutorTestSuite

import dplutils.pipeline.ray
from dplutils.pipeline import PipelineTask
from dplutils.pipeline.ray import RayStreamGraphExecutor, get_stream_wrapper, stream_split_func


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
