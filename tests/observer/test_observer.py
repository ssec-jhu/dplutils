import time

import pytest

from dplutils import observer
from dplutils.observer import InMemoryObserver


@pytest.fixture
def obsfunc():
    def func():
        observer.observe("metric1", 1)
        observer.increment("counter1")
        observer.increment("counter2", 2)
        with observer.timer("timer1"):
            time.sleep(0.01)
        observer.param("param1", "value1")

    return func


@pytest.fixture
def globalmemobs():
    obs = InMemoryObserver()
    observer.set_observer(obs)
    return obs


def test_observer_default_passes(obsfunc):
    obsfunc()


def test_observer_override_records_observations(obsfunc, globalmemobs):
    obsfunc()
    obs = observer.get_observer()
    assert obs.metrics["metric1"][0][1] == 1
    assert obs.metrics["counter1"][0][1] == 1
    assert obs.metrics["counter2"][0][1] == 2
    assert obs.metrics["timer1"][0][1] > 0
    assert obs.params["param1"] == "value1"


def test_observer_non_root_override(obsfunc):
    obs = InMemoryObserver()
    observer.set_observer(obs, "nonroot")
    obsfunc()
    assert "metric1" not in obs.metrics
    observer.get_observer("nonroot").observe("nonrootmetric", 1)
    assert obs.metrics["nonrootmetric"][0][1] == 1


def test_timer_context(globalmemobs):
    with observer.timer("timer1"):
        time.sleep(0.01)

    with observer.timer("timer2") as t:
        time.sleep(0.01)
        t.stop()
        partial = t.accum
        t.start()

    obs = observer.get_observer()
    assert obs.metrics["timer1"][0][1] > 0
    assert partial > 0
    assert obs.metrics["timer2"][0][1] > partial


def test_timer_standalone():
    t = observer.timer("timer")
    with pytest.raises(ValueError):
        t.complete()
    # stop without ever starting should not accumulate anything
    t.stop()
    assert t.accum == 0
    t.start()
    time.sleep(0.01)
    t.stop()
    partial = t.accum
    t.start()
    time.sleep(0.01)
    t.complete()
    # complete implicitly calls stop
    assert partial > 0
    assert t.accum > partial
