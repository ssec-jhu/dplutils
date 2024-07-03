import os
import sys
from glob import glob

import parsl
import pytest
from parsl.channels import LocalChannel
from parsl.providers import LocalProvider
from test_suite import PipelineExecutorTestSuite

from dplutils.pipeline.parsl import ParslHTStreamExecutor


@pytest.fixture(scope="session", autouse=True)
def parsl_session(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("intermediate_files")
    os.chdir(tmp)
    os.environ["PATH"] = sys.exec_prefix + "/bin/:" + os.environ["PATH"]
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


def test_parsl_file_cleanup(dummy_pipeline_graph, tmp_path):
    def n_staging_files():
        return len(glob(str(tmp_path / "_dpl_parsl-*.par")))

    assert n_staging_files() == 0
    it = ParslHTStreamExecutor(dummy_pipeline_graph, max_batches=10, staging_root=tmp_path).run()
    out = [next(it)]
    assert n_staging_files() > 0
    out += list(it)
    assert len(out) == 20
    # there should be no intermediate files
    assert n_staging_files() == 0


class TestParslExecutor(PipelineExecutorTestSuite):
    executor = ParslHTStreamExecutor
