Observing
=========

This package includes some utilities for observing the progress and performance of running pipelines. In a similar
manner to the python logging framework, observers are meant to be used as a global facility whose handling can be
transparently swapped out by the user to send results to different backends.

Observers provide a mechanism to record and send metrics about pipeline behavior and performance. For example to count
the number of times a particular conditional statement in a task got run, or to time a critical section of code. Adding
observers can help a user understand the progress of a running pipeline as well as elucidate places in code that could
be optimized.

Example Usage
-------------

For example, a task writer can add observers to critical parts of code::

  from dplutils import observer

  def my_task(indf):
      rows_in = len(indf)
      with observer.timer('critical_section'):
          ...
      rows_out = len(indf)
      observer.observe('filter_percent', rows_out / rows_in * 100)


By default the above would do nothing when that task is executed in a pipeline, since we do not know what the user would
want and what facilities are available. To record observations, an observer must be configured and set prior to running
the pipeline (see below).

Setting an Observer
-------------------

A user can set the observer to handles those prior to running a pipeline, for example to send all metrics to an aim
run::

  from dplutils import observer
  from dplutils.observer.aim import AimObserver

  observer.set_observer(AimObserver())

  my_pipeline.writeto('out_path')


Assuming that pipeline contained task code similar to above, this would route the observations through the
``AimObserver`` which records in its database as an experiment, which can be visualized live during the pipeline run
using the aim UI (see https://github.com/aimhubio/aim)


Executor Compatibility
----------------------

It is important to note that this package aims to make running *distributed* pipelines simple, and as such it is common
that the task code be running in a process or machine that is separate from the process that set the observer and
started the pipeline.

An observer implementation may work over the network (or utilize the filesystem, or otherwise be insensitive to this)
and be fully compatible with distributed processing, while others may not. The user should understand specific observer
behavior with respect to executor environment. For example, the ``AimObserver`` should run on a single machine and
aggregate results, while in a ray-based executor this would result in multiple copies. For this case, there is an
observer that wraps another one to handle the network communication via ray actors::

  from dplutils import observer
  from dplutils.observer.ray import RayActorWrappedObserver

  observer.set_observer(RayActorWrappedObserver(AimObserver))

  my_pipeline.writeto('out_path')


.. note::
   Also note that an executor implementation might have to add explicit handling of module setup in order for the
   observer api to work as expected, and as such a given executor may or may not be fully compatible with the observer
   module. In most cases this would mean observations in a task would take the no-op default.

Custom observers
----------------

A custom observer should implement the :py:meth:`Observer<dplutils.observer.Observer>` interface and all abstract
methods thereof. Generally it is not necessary to implement ``timer``, which utilizes the other methods to operate.
