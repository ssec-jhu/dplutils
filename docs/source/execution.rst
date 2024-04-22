Executing Pipelines
===================

A pipeline executor (some concrete implementation of :py:meth:`PipelineExecutor<dplutils.pipeline.PipelineExecutor>`) is
responsible for handling the runtime execution and dataflow described by a `PipelineGraph`.

There are two main entrypoints into execution:

* `run()`, which feeds inputs to the source task and yields :py:meth:`OutputBatch<dplutils.pipeline.OutputBatch>`
  objects containing the final dataframes and other metadata as they become available. Use this if you need to operate
  on output dataframes and/or do not need to save.

* `writeto(...)`, which provides a convenient wrapper around run which writes outputs to a parquet table. The cli
  utilities (see also :doc:`command_line`) provide a runner which utilizes this method.

Example
-------

See the :doc:`quickstart` for a simple example of defining and executing a pipeline.

Inputs
------

One particular target for this framework is pipelines that run infinitely/indefinitely and generate cases as needed for
scientific discovery, for example generating a candidate protein sequence and analyze it for certain properties. When
considering the space of possible candidates and the analysis runtime, this effectively tends toward an infinite search
space. It also doesn't particularly depend on the state of the driver application or other tasks.

As such, the default inputs for pipeline execution is simply a single-row batch of monotonic IDs. These simply serve as
a catalyst for execution of the source tasks and subsequently control the flow throughout the pipeline.

All executors should support such a input feed (or simulate it), but some may also support a generator that yields input
dataframes as a way of enabling an input feed that passes some desired state or data from the driver to the source
task(s).

Specifically, the :py:meth:`RayStreamGraphExecutor<dplutils.pipeline.ray.RayStreamGraphExecutor>` takes an optional
callable that returns a generator which yields dataframes. For example, to replicate an infinite stream of monotonically
increasing batch ids::

  def my_generator():
      bid = 1
      while True:
         yield pd.DataFrame({'id': [bid]})
         bid += 1

  pl = RayStreamGraphExecutor(graph, generator=my_generator)


Note the distinction: the argument does not take a generator, but a callable that returns one - if pipeline reruns
should share state, this can be handled in the generator or an objects method could be passed where the object maintains
state.
