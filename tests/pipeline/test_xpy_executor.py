from typing import Any

from test_suite import PipelineExecutorTestSuite

from dplutils.pipeline.xpy import XPyStreamExecutor, xpy_main


class XpyTestExecutor(XPyStreamExecutor):
    """Stub class for testing Xpy executor abstract base."""

    def submit_task_execution(self, task, cmd, pickled_bundle):
        xpy_main(pickled_bundle)

    def is_task_ready(self, pending_task: Any) -> bool:
        return True

    def task_submittable(self, task, rank) -> bool:
        return True

    def poll_tasks(self, pending_task_list: list[Any]):
        pass


class TestXpyExecutor(PipelineExecutorTestSuite):
    executor = XpyTestExecutor
