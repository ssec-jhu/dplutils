import ray
from ray.util.metrics import Counter, Gauge

from dplutils.observer import Observer


class RayActorWrappedObserver(Observer):
    """Create actor from another observer

    For use with stateful observers that should not have multiple
    instances. Each call to this observer does a remote call to the actor-based
    observer.

    Args:
        cls: The observer class to start as actor
        *args: Args to pass to ``cls`` instantiation
        **kwargs: Keyword args to pass to ``cls`` instantiation
    """

    def __init__(self, cls, *args, **kwargs):
        self.actor = ray.remote(cls).remote(*args, **kwargs)
        self._wait = False  # for testing purposes. If true wait instead of fire-and-forget

    def _wrap_waiter(self, remote):
        if self._wait:
            ray.get(remote)

    def observe(self, name, value, **kwargs):
        self._wrap_waiter(self.actor.observe.remote(name, value, **kwargs))

    def increment(self, name, value=1, **kwargs):
        self._wrap_waiter(self.actor.increment.remote(name, value=1, **kwargs))

    def param(self, name, value, **kwargs):
        self._wrap_waiter(self.actor.param.remote(name, value, **kwargs))


class RayMetricsObserver(Observer):
    """Observer implemented using ray metrics

    Ray metrics are implemented in the raylet and exposed as a prometheus
    endpoint. While there is some state to store the underlying ray metric
    objects, this can be used directly having copies per worker (so does not
    need to be wrapped in actor).
    """

    def __init__(self):
        self.mmap = {}

    def _get_or_set_as(self, name, kind):
        if name in self.mmap:
            metric = self.mmap[name]
            if not isinstance(metric, kind):
                raise TypeError(f"setting metric requires {kind}, but {name} is {type(metric)}")
        else:
            metric = kind(name)
            self.mmap[name] = metric
        return metric

    def observe(self, name, value, **kwargs):
        self._get_or_set_as(name, Gauge).set(value)

    def increment(self, name, value=1, **kwargs):
        self._get_or_set_as(name, Counter).inc(value)

    def param(self, name, value, **kwargs):
        """Not yet implemented"""
        pass
