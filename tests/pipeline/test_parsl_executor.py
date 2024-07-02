import os
import sys
import pytest
import parsl
from parsl.configs import htex_local
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from dplutils.pipeline.task import PipelineTask
from test_suite import PipelineExecutorTestSuite
from dplutils.pipeline.parsl import ParslHTStreamExecutor


@pytest.fixture(scope="session", autouse=True)
def parsl_session(tmp_path_factory):
    tmp = tmp_path_factory.mktemp('intermediate_files')
    os.chdir(tmp)
    config = parsl.Config(
        executors=[
            parsl.HighThroughputExecutor(
                label="htex",
                cores_per_worker=1,
                provider=LocalProvider(
                    channel=LocalChannel(userhome=str(tmp)),
                    init_blocks=1,
                    max_blocks=1,
                ),
                working_dir=str(tmp),
            )
        ]
    )
    config.executors[0].launch_cmd = sys.exec_prefix + "/bin/" + config.executors[0].launch_cmd
    parsl.load(config)


class TestParslExecutor(PipelineExecutorTestSuite):
    executor = ParslHTStreamExecutor