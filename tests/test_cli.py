import pytest
from dplutils import cli


TEST_ARGV = [
    '',
    '--set-context', 'ctx=ctx-value',
    '--set-config', 'a.b.c=[1,2,3]',
    '--set-config', 'x.y=1',
    '--set-config', 'l.m=string',
]


@pytest.fixture
def sys_argv(monkeypatch):
    monkeypatch.setattr('sys.argv', TEST_ARGV)


def test_get_argparser_adds_default_arguments(sys_argv):
    args = cli.get_argparser().parse_args()
    assert args.set_context == ['ctx=ctx-value']
    assert args.set_config == ['a.b.c=[1,2,3]', 'x.y=1', 'l.m=string']


def test_config_from_args(sys_argv):
    args = cli.get_argparser().parse_args()
    config = cli.config_dict_from_args(args)
    assert config['a']['b']['c'] == [1, 2, 3]
    assert config['x']['y'] == 1
    assert config['l']['m'] == 'string'


def test_pipeline_set_config_from_args_no_args(monkeypatch, dummy_executor):
    monkeypatch.setattr('sys.argv', [''])
    cli.set_config_from_args(dummy_executor, cli.get_argparser().parse_args())


def test_run_with_cli_helper(monkeypatch, dummy_executor, tmp_path):
    assert len(list(tmp_path.glob('*.parquet'))) == 0
    monkeypatch.setattr(
        'sys.argv',
        ['', '-o', str(tmp_path), '--set-context', 'ctxvar=value', '--set-config', 'task1.num_cpus=2'])
    cli.cli_run(dummy_executor)
    assert dummy_executor.ctx['ctxvar'] == 'value'
    assert dummy_executor.tasks_idx['task1'].num_cpus == 2
    assert len(list(tmp_path.glob('*.parquet'))) == 10
