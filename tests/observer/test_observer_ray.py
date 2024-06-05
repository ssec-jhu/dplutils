import pytest
import ray
from ray.util.metrics import Counter, Gauge

from dplutils import observer
from dplutils.observer.ray import RayActorWrappedObserver, RayMetricsObserver


@pytest.mark.parametrize("set_wait", [True, False])
def test_ray_actor_wrapped_observer(raysession, set_wait):
    obs = RayActorWrappedObserver(observer.InMemoryObserver)
    obs._wait = set_wait
    obs.observe("metric", 99.0)
    obs.increment("counter")
    obs.param("parm", "value")

    obs_vals = ray.get(obs.actor.dump.remote())
    print(obs_vals)
    assert obs_vals["params"]["parm"] == "value"
    assert obs_vals["metrics"]["counter"][0][1] == 1
    assert obs_vals["metrics"]["metric"][0][1] == 99.0


def test_ray_metrics_observer():
    obs = RayMetricsObserver()
    obs.observe("metric", 99.0)
    obs.increment("counter")
    obs.param("parm", "value")

    assert isinstance(obs.mmap["metric"], Gauge)
    assert isinstance(obs.mmap["counter"], Counter)

    # ensures we do the metric-mapping lookup
    obs.observe("metric", 0.0)

    with pytest.raises(TypeError):
        obs.increment("metric")
