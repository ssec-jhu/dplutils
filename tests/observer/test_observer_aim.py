import pytest
from aim import Run

import dplutils.observer.aim
from dplutils import observer
from dplutils.observer.aim import AimObserver


@pytest.fixture(scope="session")
def aimobserver(tmpdir_factory):
    repo_dir = str(tmpdir_factory.mktemp("aimrepo"))
    obs = AimObserver(repo=repo_dir, experiment="test")
    observer.set_observer(obs)
    return obs


def get_metric_last_value(run, metric):
    m = [i for i in run.metrics() if i.name == metric][0]
    return m.values.last_value()


def test_aim_no_run_instantiates_with_kwargs(aimobserver):
    assert isinstance(aimobserver.run, Run)
    assert aimobserver.run.experiment == "test"


def test_aim_run_use_existing_run(tmp_path):
    run = Run(repo=tmp_path, experiment="existing")
    obs = AimObserver(run=run)
    assert obs.run == run
    assert obs.run.experiment == "existing"


def test_set_param(aimobserver):
    observer.param("testparam", "value")
    assert aimobserver.run["testparam"] == "value"


def test_set_metric_value(aimobserver):
    observer.observe("testmetric", 99.9)
    assert get_metric_last_value(aimobserver.run, "testmetric") == 99.9


def test_increment_counter(aimobserver):
    observer.increment("testcounter")
    assert get_metric_last_value(aimobserver.run, "testcounter") == 1
    observer.increment("testcounter")
    assert get_metric_last_value(aimobserver.run, "testcounter") == 2


def test_instantiation_excepts_when_no_package(monkeypatch):
    monkeypatch.setattr(dplutils.observer.aim, "Run", None)
    with pytest.raises(ImportError):
        AimObserver()
