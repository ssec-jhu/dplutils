from dplutils.observer import Observer

try:
    import mlflow
except ImportError:
    mlflow = None


class MlflowObserver(Observer):
    """Mlflow-based observer

    MLflow is an ML experiment tracker with included explorer UI. See
    https://github.com/mlflow/mlflow/ for details.

    Args:
        run: Existing mlflow run object to use for tracking. In this case it is
            assumed the tracking_uri is that set globally.
        experiment: Name of experiment under which to create run (if run not supplied).
        tracking_uri: tracking uri, e.g. ``file://...`` or ``mlflow://``,
            etc. See mlflow docs for details.
        mlflow_kwargs: In case an existing
        run is not supplied, one will be
            created, in which case mlflow_kwargs (excluding ``experiment_id``)
            will be passed to its instantiation, using
            ``mlflow.MlflowClient.create_run``.
    """

    def __init__(self, run=None, experiment=None, tracking_uri=None, **mlflow_kwargs):
        if mlflow is None:
            raise ImportError("mlflow must be installed to create observer run!")

        tracking_uri = tracking_uri or mlflow.get_tracking_uri()
        self.mlflow_client = mlflow.MlflowClient(tracking_uri=tracking_uri)

        if run is not None:
            self.run = run
        else:
            expid = None
            if experiment is not None:
                exp = self.mlflow_client.get_experiment_by_name(experiment)
                if exp is not None:
                    expid = exp.experiment_id
                else:
                    expid = self.mlflow_client.create_experiment(experiment)
            self.run = self.mlflow_client.create_run(experiment_id=expid, **mlflow_kwargs)

        self.run_id = self.run.info.run_id
        self._countercache = {}

    def observe(self, name, value, **kwargs):
        self.mlflow_client.log_metric(self.run_id, name, value)

    def increment(self, name, value=1, **kwargs):
        val = self._countercache.get(name, 0) + value
        self._countercache[name] = val
        self.mlflow_client.log_metric(self.run_id, name, val)

    def param(self, name, value, **kwargs):
        self.mlflow_client.log_param(self.run_id, name, value)
