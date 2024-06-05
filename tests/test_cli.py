import pytest

from dplutils import cli
from dplutils.pipeline import PipelineTask

TEST_ARGV = [
    "",
    "--set-context",
    "ctx=ctx-value",
    "--set-config",
    "a.b.c=[1,2,3]",
    "--set-config",
    "a.b.x=99",
    "--set-config",
    "x.y=1",
    "--set-config",
    "l.m=string",
]


@pytest.fixture
def sys_argv(monkeypatch):
    monkeypatch.setattr("sys.argv", TEST_ARGV)


def test_get_argparser_adds_default_arguments(sys_argv):
    args = cli.get_argparser().parse_args()
    assert args.set_context == ["ctx=ctx-value"]
    assert args.set_config == ["a.b.c=[1,2,3]", "a.b.x=99", "x.y=1", "l.m=string"]


def test_pipeline_set_config_from_args_no_args(monkeypatch, dummy_executor):
    monkeypatch.setattr("sys.argv", [""])
    cli.set_config_from_args(dummy_executor, cli.get_argparser().parse_args())


def test_run_with_cli_helper(monkeypatch, dummy_executor, tmp_path):
    assert len(list(tmp_path.glob("*.parquet"))) == 0
    monkeypatch.setattr(
        "sys.argv",
        [
            "",
            "-o",
            str(tmp_path),
            "--set-context",
            "ctxvar=value",
            "--set-config",
            "task1.num_cpus=2",
            "--set-config",
            "task1.batch_size=10",
            "--set-config",
            "task_kw.kwargs.a=[1,2]",
            "--set-config",
            "task_kw.kwargs.b=99",
        ],
    )

    def task_with_kwargs(indf, a=None, b=None):
        return indf

    # patch in kwargs task to test kwarg setting via dotted notation
    dummy_executor.graph.add_edge(dummy_executor.graph.task_map["task2"], PipelineTask("task_kw", task_with_kwargs))
    cli.cli_run(dummy_executor)
    assert dummy_executor.ctx["ctxvar"] == "value"
    assert dummy_executor.tasks_idx["task1"].num_cpus == 2
    assert dummy_executor.tasks_idx["task1"].batch_size == 10
    assert dummy_executor.tasks_idx["task_kw"].kwargs["a"] == [1, 2]
    assert dummy_executor.tasks_idx["task_kw"].kwargs["b"] == 99
    assert len(list(tmp_path.glob("*.parquet"))) == 10
