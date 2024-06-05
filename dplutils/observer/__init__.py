import time
from abc import ABC, abstractmethod
from collections import defaultdict


class Timer:
    """Timer context manager

    This starts a timer that logs value at exit to supplied observer using the
    ``observe`` method. In general, this should be called via the observers
    ``timer`` method. Any kwargs supplied to the initialization will be passed
    to the ``observe`` method on completion. The timer can be started and
    stopped multiple times, the accumulated time will be reported.

    Example:

        with observer.timer('calltime'):
            <<do something>>
    """

    def __init__(self, observer, name, **kwargs):
        self.observer = observer
        self.name = name
        self.kwargs = kwargs
        self.start_time = None
        self.started = False
        self.accum = 0

    def start(self):
        self.start_time = time.monotonic()
        self.started = True

    def stop(self):
        if self.start_time is not None:
            self.accum += (time.monotonic() - self.start_time) * 1000
            self.start_time = None

    def complete(self):
        if not self.started:
            raise ValueError("Timer not started!")
        self.stop()
        self.observer.observe(self.name, self.accum, **self.kwargs)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.complete()


class Observer(ABC):
    """Observer base class for recording metrics

    Since it is not known at runtime what concrete implementation will be used,
    any subclass should take the args in full and silently ignore any that do
    not apply. Be mindful of raises exceptions in any of these functions as many
    cases it is preferred to lose some observability for continued execution.

    While implementations are required to implement ``observe``, ``increment``
    and ``param``, there may be legitimit cases where the recording of
    """

    @abstractmethod
    def observe(self, name, value, **kwargs):
        """Observe a metric value

        This is for generic metric that may or may not be monotonic
        increasing. This can be called any number of times using the same
        ``name`` to record updated values of the metric.

        Calling ``increment`` on a counter created using this function results
        in undefined behavior.

        Args:

            name: Name of metric
            value: Value to record
            **kwargs: kwargs to be passed to implementation specific function
        """
        pass

    @abstractmethod
    def increment(self, name, value=1, **kwargs):
        """Increment a counter

        For monotonically increasing counters. This can be called any number of
        times using the same ``name`` to record updated values of the metric.

        Calling ``observe`` on a counter created using this function results in
        undefined behavior.

        Args:

            name: Name of metric
            value: Value to increment by
            **kwargs: kwargs to be passed to implementation specific function

        """
        pass

    @abstractmethod
    def param(self, name, value, **kwargs):
        """Record a run parameter

        Run parameters should be treated as global key,value pairs that contain
        a single value. In general, calling this more than once on the same name
        would result in overwiting the named parameter with the new value.

        Args:

            name: Name of parameter
            value: Value to set for parameter
            **kwargs: kwargs to be passed to implementation specific function
        """
        pass

    def timer(self, name, **kwargs):
        """Return a timer context manager recoding to this observer

        Args:

            name: Name of metric
            **kwargs: kwargs to be passed to implementation specific function
        """
        return Timer(self, name, **kwargs)


class NoOpObserver(Observer):
    """Default do-nothing implementation

    This is akin to the ``NullHandler<logging.NullHandler>`` in the logging
    module and is the default upon initialization.
    """

    def observe(*args):
        """This method does nothing"""
        pass

    def increment(*args):
        """This method does nothing"""
        pass

    def param(*args):
        """This method does nothing"""
        pass


class InMemoryObserver(Observer):
    """Example implementation storing metrics in memory

    Metrics are stored in a dict of lists keyed by the metric ``name``, where
    each element in the list is a tuple (recorded_unix_time, value). Params are
    stored in a separate dict keyed by the parameter ``name``.
    """

    def __init__(self):
        self.metrics = defaultdict(list)
        self.params = {}

    def observe(self, name, value, **kwargs):
        self.metrics[name].append((time.time(), value))

    def increment(self, name, value=1, **kwargs):
        if len(self.metrics[name]) == 0:
            val = value
        else:
            val = self.metrics[name][-1][1] + value
        self.metrics[name].append((time.time(), val))

    def param(self, name, value, **kwargs):
        self.params[name] = value

    def dump(self):
        return {"params": self.params, "metrics": self.metrics}


observer_map = {
    "root": NoOpObserver(),
}


def set_observer(obs, key="root"):
    """Set the global observer at ``key``"""
    observer_map[key] = obs


def get_observer(key="root"):
    """Get the global observer at ``key``"""
    return observer_map.get(key, observer_map["root"])


def observe(*args, **kwargs):
    """call observe on the root observer"""
    observer_map["root"].observe(*args, **kwargs)


def increment(*args, **kwargs):
    """call increment on the root observer"""
    observer_map["root"].increment(*args, **kwargs)


def param(*args, **kwargs):
    """call param on the root observer"""
    observer_map["root"].param(*args, **kwargs)


def timer(*args, **kwargs):
    """call timer on the root observer"""
    return observer_map["root"].timer(*args, **kwargs)
