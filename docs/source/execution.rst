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

Specifically, derivatives of the :py:meth:`StreamingGraphExecutor<dplutils.pipeline.stream.StreamingGraphExecutor>` take
an optional callable that returns a generator which yields dataframes. For example, to replicate an infinite stream of
monotonically increasing batch ids using the ray implementation of the streaming graph executor
(:py:meth:`RayStreamGraphExecutor<dplutils.pipeline.ray.RayStreamGraphExecutor>`)::

  def my_generator():
      bid = 1
      while True:
         yield pd.DataFrame({'id': [bid]})
         bid += 1

  pl = RayStreamGraphExecutor(graph, generator=my_generator)


Note the distinction: the argument does not take a generator, but a callable that returns one - if pipeline reruns
should share state, this can be handled in the generator or an objects method could be passed where the object maintains
state.


Outputs
-------

As mentioned above, the `run()` method yields :py:meth:`OutputBatch<dplutils.pipeline.OutputBatch>` objects to the
caller, which contain the `data` attribute with the output dataframe. As pipeline graphs can contain more than a single
sink task, the output batch also has a `task` attribute with the name of the output task, enable custom routing for
dataframes yielded from different parts of the pipeline.

The `write()` method executes `run()` and writes out resultant dataframes from sink tasks to a parquet table at the
specified directory (specified by `outdir`). It writes a file per output batch yielded from sinks marked with the run ID
and a monotonic increasing ID in order generated. It will by default write batches to hive-style partitions for each
sink task (e.g. `{outdir}/task={task_name}/`), in the case that there are multiple sink tasks in the graph, or write
parquet files directly under output if there is only a single source task. This behavior can be controlled via the
`partition_by_task` argument.

A typical parquet reader will understand how to read a multi-file partitioned dataset as generated above. For example,
to read in using pandas::

  import pandas as pd
  pd.read_parquet('{outdir}/')


Or using duckdb::

  import duckdb as ddb
  ddb.query('''SELECT * FROM '{outdir}/*/*.parquet' ''')

assuming the output is partitioned one level deep. Both of these will generate a table that has a column for the
hive-partitioned data, which in this case is a string column named task.
