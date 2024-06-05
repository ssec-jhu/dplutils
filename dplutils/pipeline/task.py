from copy import copy
from dataclasses import dataclass, field, replace
from inspect import _empty, signature
from typing import Any, Callable

import pandas as pd


@dataclass
class PipelineTask:
    """Container representing a task and its runtime configuration and dependencies.

    The PipelineTask represents metadata about a task within a particular
    pipeline, the execution of which is handled by a
    :class:`PipelineExecutor<dplutils.pipeline.executor.PipelineExecutor>`. Each
    task function is expected to take a pandas Dataframe as its first positional
    argument and produce a dataframe. Any additional arguments should be keyword
    arguments (required or otherwise) and are expected to be passed to the
    function by the executor via ``kwargs`` and ``context_kwargs``. The values
    set in a PipelineTask represent the defaults for a task, which may be
    updated by a particular pipeline invocation.

    The ``__call__`` method can be used to return a new task with updated
    default parameters, for example to customize the name:

        >>> MyTask = PipelineTask('task', func=myfunc)
        >>> MyNewTask = MyTask('reconf', kwargs=dict(arg=value))

    args:
        name: name of task for reference in pipeline and configuration.
        func: the callable to execute task operations.
        context_kwargs: a mapping from pipeline context key to keyword arguments
            passed to func. Used for reusing arguments that are not
            task-specific. For example if the several steps access a key
            ``data``, then setting context_kwargs to {``data_in``: ``data``}
            would indicate the executor should pass the value of the context
            element ``data`` to the ``data_in`` of func at runtime.
        kwargs: a dict of task-specific kwargs to pass to function as ``**kwargs``
        num_cpus: CPU allocations requested for task
        num_gpus: GPU allocations requested for task
        resources: dict of any additional resources to pass to the executor
        batch_size: ideal batch size for this workload.
    """

    name: str
    func: Callable[[pd.DataFrame, ...], pd.DataFrame]
    context_kwargs: dict[str, str] = field(default_factory=dict)
    kwargs: dict[str, Any] = field(default_factory=dict)
    num_gpus: int = None
    num_cpus: int = 1
    resources: dict[str, Any] = field(default_factory=dict)
    batch_size: int = None

    def __call__(self, name: str = None, **kwargs):
        if name is None:
            name = self.name
        return replace(self, name=name, **kwargs)

    def resolve_kwargs(self, context: dict):
        """Return a dict of final keyword arguments for the given context.

        This method consults context_kwargs to build an updated list of kwargs
        based on the given context to pass to ``func``.
        """
        kwargs = copy(self.kwargs)
        kwargs.update({k: context[v] for k, v in self.context_kwargs.items() if v in context})
        return kwargs

    def validate(self, context: dict):
        """Validate the arguments of ``func`` given context prior to run.

        To enable a pre-execution check of the final configuration including
        context, this method consults the signature of ``func`` and the
        ``kwargs`` and ``context_kwargs`` to determine if the call is missing or
        has too many parameters. A pandas Dataframe is expected as the first
        parameter and thus ignored in this validation.
        """
        all_kwargs = self.resolve_kwargs(context)
        # we expect a dataframe as the first argument, so skip validation for that
        params = list(signature(self.func).parameters.items())[1:]
        for key, param in params:
            if param.default == _empty:
                if key not in all_kwargs:
                    msg = f"missing required argument {key} for task {self.name}"
                    if key in self.context_kwargs:
                        msg = f"{msg} - expected from context {self.context_kwargs[key]}"
                    raise ValueError(msg)
        extra = set(all_kwargs.keys()) - {k for k, v in params}
        if len(extra) > 0:
            raise ValueError(f"unkown arguments {extra} for task {self.name}")

    def __hash__(self):
        return hash(self.name)
