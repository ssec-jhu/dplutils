# SSEC-JHU dplutils

[![CI Status](https://github.com/ssec-jhu/dplutils/actions/workflows/ci.yml/badge.svg)](https://github.com/ssec-jhu/dplutils/actions)
[![Documentation Status](https://readthedocs.org/projects/dplutils/badge/?version=latest)](https://dplutils.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/ssec-jhu/dplutils/branch/main/graph/badge.svg?token=0KPNKHRC2V)](https://codecov.io/gh/ssec-jhu/dplutils)
[![Security Status](https://github.com/ssec-jhu/dplutils/actions/workflows/security.yml/badge.svg)](https://github.com/ssec-jhu/dplutils/actions)
[![PyPI](https://img.shields.io/pypi/v/dplutils)](https://pypi.org/project/dplutils)
<!---[![DOI](https://zenodo.org/badge/<insert_ID_number>.svg)](https://zenodo.org/badge/latestdoi/<insert_ID_number>) --->


![SSEC-JHU Logo](docs/_static/SSEC_logo_horiz_blue_1152x263.png)

* [About](#About)
* [Quick Start](#quick-start)
* [Scaling Out](#scaling-out)
* [Resource Specification](#resource-specifications)
* [Building and Development](#building-and-development)

# About

This package provides python utilities to define and execute graphs of tasks
that operate on and produce dataframes in a batched-streaming manner. The
primary aims are as follows:

* Operate on an indefinite stream of batches of input data.
* Execute tasks in a distributed fashion using configurable execution backends
  (e.g. Ray).
* Schedule resources on a per-task basis.
* Configurable batching: enable co-location for high-overhead tasks or maximal
  spread for resource intensive tasks.
* Provide a simple interface to create tasks and insert them into a pipeline.
* Provide validations to help ensure tasks are correctly configured prior to
  execution (potentially with high latency).

Discovery pipelines that generate input samples on-the-fly are particularly well
suited for this package, though input can be taken from any source, including
on-disk tables.

This package also includes utilities for observing pipelines using metrics tools
such as mlflow or aim, and provides functionality for making simple CLI
interfaces from pipeline definitions.

# Quick Start

Getting up and running is easy: simply install the package:

```sh
pip install dplutils
```

Then define the tasks, connect them into a pipeline, and then iterate over the
output dataframes:

```py
import numpy as np
from dplutils.pipeline import PipelineGraph, PipelineTask

# Definitions of task code - note all take dataframe as first argument and return a dataframe
def generate_sample(df):
  return df.assign(sample = np.random.random(len(df)))

def round_sample(df, decimals=1):
  return df.assign(rounded = df['sample'].round(decimals))

def calc_residual(df):
  return df.assign(residual = df['rounded'] - df['sample'])

# Connect them together in an execution graph (along with execution metadata)
pipeline = PipelineGraph([
  PipelineTask('generate', generate_sample),
  PipelineTask('round', round_sample, batch_size=10),
  PipelineTask('resid', calc_residual, num_cpus=2),
])

# Run the tasks and iterate over the outputs, here using the Ray execution framework
from dplutils.pipeline.ray import RayStreamGraphExecutor

executor = RayStreamGraphExecutor(pipeline).set_config('round.kwargs.decimals', 2)
for result_batch in executor.run():
  print(result_batch)
  break  # otherwise it will - by design - run indefinitely!
```

As an alternative to iterating over batches directly as above, we can use the
CLI utilities to run the given executor as a tool. The helper arranges for all
options so we just need to define the desired executor:

```py
executor = RayStreamGraphExecutor(pipeline)

if __name__ == '__main__':
  cli_run(executor)
```

then run our module with parameters as needed. The CLI based run will write the
output to a parquet table at a specified location (below assumes code is in
`ourmodule.py`):

```sh
python -m ourmodule -o /path/to/outdir --set-config round.batch_size=5
```

The above is of course a trivial example to demonstrate the structure and
simplicity of defining pipelines and how they operate on configurable-sized
batches represented as dataframes. For more information on defining tasks, their
inputs, seeding the input tasks, more complex graph structures and distributed
execution, among other topics, see the documentation at:
https://dplutils.readthedocs.io/en/stable/.

# Scaling out

One of the goals of this project simplify the scaling out of connected tasks on
a variety of systems. PipelineExecutors
[PipelineExecutor](https://dplutils.readthedocs.io/en/stable/generated/dplutils.pipeline.PipelineExecutor.html#dplutils.pipeline.PipelineExecutor)
are responsible for this - this package provides a framework for adding
executors based on appropriate underlying scheduling/execution systems and
provides some implementations, for example using Ray
[RayStreamGraphExecutor](https://dplutils.readthedocs.io/en/stable/generated/dplutils.pipeline.ray.RayStreamGraphExecutor.html).
Setup required to properly configure an executor for scaling depends on the
backend used, for example ray relies on having a cluster previously bootstrapped
(see https://docs.ray.io/en/latest/cluster/getting-started.html), though can
operate locally without any prior setup.

# Resource specifications

Another primary goal is to arrange for resource dependencies to be met by the
execution environment for a particular task. Resources such as number of CPUs or
GPUs are natural targets and supported by many systems. We also want to support
arbitrary resource requests, for example if a task requires a large local
database on fast disks, this might be available only on one node in a cluster. A
custom resource can be used to ensure that batches of a particular task always
execute on the environment that has that resource.

This ability depends on the executor backend to support it, so executors
implementations typically only make sense for such systems - of which Ray is
one.


For instance, if a task required a database as described above, the task might
be defined in the following manner:

```py
PipelineTask('bigdboperation', function_needs_big_db, resources={'bigdb': 1})
```

And if using the Ray executor, then at least one worker in the cluster that has
local fast disks with the big database resident would be started similar to:

```sh
ray start --address {head-ip} --resources '{"bigdb": 1}'
```

In other execution systems, the worker might be started in a different manner,
but the task definition could remain as-is, enabling easy swapping of the
execution environment depending on the situation.

# Building and Development

If you need to make modifications to the source code, follow the steps below to
get the source and run tests. The process is simple and we use Tox to manage
test and build environments. We welcome any contributions, please open a pull
request!

## Setup

Clone this repository locally:

```sh
git clone https://github.com/ssec-jhu/dplutils.git
cd dplutils
```

Install dependencies:

```sh
pip install -r requirements/dev.txt
```

## Tests

Run tox:

* ``tox -e test``, to run just the tests
* ``tox``, to run linting, tests and build. This should be run without errors prior to commit

## Docker

From the repo directory, run

``docker build -f docker/Dockerfile --tag dplutils .``


