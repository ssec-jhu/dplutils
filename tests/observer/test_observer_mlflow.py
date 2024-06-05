import mlflow
import pytest

import dplutils.observer.mlflow
from dplutils import observer
from dplutils.observer.mlflow import MlflowObserver


@pytest.fixture(scope="session")
def mlflowobserver(tmpdir_factory):
    repo_dir = str(tmpdir_factory.mktemp("mlflow"))
    obs = MlflowObserver(tracking_uri=f"file://{repo_dir}", experiment="testexp", run_name="testrun")
    observer.set_observer(obs)
    return obs


@pytest.fixture(scope="session")
def mlflowclient(mlflowobserver):
    return mlflowobserver.mlflow_client


def test_mlflow_no_run_instantiates_with_kwargs(mlflowobserver, mlflowclient):
    assert isinstance(mlflowobserver.run, mlflow.entities.run.Run)
    assert mlflowobserver.run.info.run_name == "testrun"
    assert mlflowclient.get_experiment(mlflowobserver.run.info.experiment_id).name == "testexp"


def test_mlflow_run_use_existing_run(tmp_path):
    mlflow.set_tracking_uri(f"file://{tmp_path}/mlflow_test")
    run = mlflow.start_run(run_name="oneoffrun")
    obs = MlflowObserver(run=run)
    assert obs.run == run
    assert obs.run.info.run_name == "oneoffrun"
    assert obs.run_id == obs.run.info.run_id


def test_mlflow_use_create_run_existing_experiment(mlflowclient):
    eid = mlflowclient.create_experiment("mytestexperiment")
    obs = MlflowObserver(tracking_uri=mlflowclient.tracking_uri, experiment="mytestexperiment")
    assert obs.run.info.experiment_id == eid


def test_set_param(mlflowobserver, mlflowclient):
    observer.param("testparam", "value")
    assert mlflowclient.get_run(mlflowobserver.run_id).data.params["testparam"] == "value"


def test_set_metric_value(mlflowobserver, mlflowclient):
    observer.observe("testmetric", 99.9)
    assert mlflowclient.get_run(mlflowobserver.run_id).data.metrics["testmetric"] == 99.9


def test_increment_counter(mlflowobserver, mlflowclient):
    observer.increment("testcounter")
    assert mlflowclient.get_run(mlflowobserver.run_id).data.metrics["testcounter"] == 1
    observer.increment("testcounter")
    assert mlflowclient.get_run(mlflowobserver.run_id).data.metrics["testcounter"] == 2


def test_instantiation_excepts_when_no_package(monkeypatch):
    monkeypatch.setattr(dplutils.observer.mlflow, "mlflow", None)
    with pytest.raises(ImportError):
        MlflowObserver()
