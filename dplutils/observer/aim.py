from dplutils.observer import Observer

try:
    from aim import Run
except ImportError:
    Run = None


class AimObserver(Observer):
    """Aim-based observer

    Aim is an ML experiment tracker with included explorer UI. See
    https://github.com/aimhubio/aim for details.

    Args:
        run: Existing aim run object to use for tracking.
        aim_kwargs: In case an existing run is not supplied, one will be
            created, in which case aim_kwargs will be passed to its
            instantiation

    Note:
        Aim does not track the time with metric, only the step and this
        implementation uses the default auto-increment step counter.
    """

    def __init__(self, run=None, **aim_kwargs):
        if run is not None:
            self.run = run
        else:
            if Run is None:
                raise ImportError("aim must be installed to create observer run!")
            self.run = Run(**aim_kwargs)
        self._countercache = {}

    def observe(self, name, value, **kwargs):
        self.run.track(value, name=name)

    def increment(self, name, value=1, **kwargs):
        val = self._countercache.get(name, 0) + value
        self._countercache[name] = val
        self.run.track(val, name=name)

    def param(self, name, value, **kwargs):
        self.run[name] = value
