Configuring Pipelines
=====================

Here configuration refers to runtime variables used to control the behavior of a particular pipeline run. There are
three main types of configuration to consider:

* Task resource requirements: These are settings of the parameters of pipeline tasks such as `num_cpus`, `num_gpus`,
  `batch_size` etc.

* Task keyword arguments: Refers to keyword arguments of the function defined in a `PipelineTask`, and take their
  defaults from either the definition within a task or an associated `context` argument. In a case where a keyword
  argument does not have a default and there is no `context_kwargs` mapping, an error is raised unless the argument is
  supplied.

* Context arguments: Refers to arguments that may be shared between tasks and should be defined once per pipeline. In
  the case that any `PipelineTask` has a `context_kwargs` mapping, the context must be given or an error is raised.


Task Defaults
-------------

Default keyword arguments and resource requirements can be set for tasks in the definition of the
:py:meth:`PipelineTask<dplutils.pipeline.PipelineTask>`. Keyword arguments that are expected to come from pipeline
context are also set in the task, then mapped from context at runtime. See below for details on overriding config and
context settings.


Runtime Configuration
---------------------

The pipeline executor (see also :doc:`execution` and :py:meth:`PipelineExecutor<dplutils.pipeline.PipelineExecutor>`),
which transforms the task graph into a runnable form, has methods for setting the above argument types, for example::


  RayStreamGraphExecutor(graph) \
    .set_context('global_var', 1) \
    .set_config('task1.kwargs.num_iters', 10) \
    .set_config('task1.num_cpus', 2) \
    .run()


From the above example we can see that a context variable called `global_var` is set to `1` and the `task1` request of
cpus to 2 along with a setting of the `num_iters` keyword argument to `10`. This also shows the operator chaining that
is possible since each `set_` method returns the pipeline itself.


Context
-------

In the example above, we would expect some `PipelineTask` to have a mapping from the `global_var` context to a keyword
of its function, for example::

  def task_fun(indf: DataFrame, var: int = 2):
      ...

  task1 = PipelineTask('task1', task_fun, context_kwargs={'var': 'global_var'})
