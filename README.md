# SSEC-JHU dplutils

[![CI Status](https://github.com/ssec-jhu/dplutils/actions/workflows/ci.yml/badge.svg)](https://github.com/ssec-jhu/dplutils/actions)
[![Documentation Status](https://readthedocs.org/projects/dplutils/badge/?version=latest)](https://dplutils.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/ssec-jhu/dplutils/branch/main/graph/badge.svg?token=0KPNKHRC2V)](https://codecov.io/gh/ssec-jhu/dplutils)
[![Security Status](https://github.com/ssec-jhu/dplutils/actions/workflows/security.yml/badge.svg)](https://github.com/ssec-jhu/dplutils/actions)
[![PyPI](https://img.shields.io/pypi/v/dplutils)](https://pypi.org/project/dplutils)
<!---[![DOI](https://zenodo.org/badge/<insert_ID_number>.svg)](https://zenodo.org/badge/latestdoi/<insert_ID_number>) --->


![SSEC-JHU Logo](docs/_static/SSEC_logo_horiz_blue_1152x263.png)

Distributed Data Pipeline Utilities

# Usage:

Check out the docs at https://dplutils.readthedocs.io/en/stable/

## Setup locally

```
pip install dplutils
```

You can create pipelines and run in a local ray cluster as in the example below. Either in client mode running directly
in an interactive python session, or by submission to a locally running ray cluster.

Further details on running a pipeline are listed below in the using the docker container environment.

## Setup Using Docker

Get (or build, see below) docker image

```
docker pull ghcr.io/ssec-jhu/dplutils:latest
```

### Start cluster

To start a cluster, start one ray head node and any number of worker nodes on network-connected hosts. To start the head
node, running the container using the docker engine, this can be used:

```
docker run -d -n rayhead -v /path/to/data:/data --net host \
  dplutils /opt/startray.sh --head --block
```

which will start the head node (blocking in order that the container stay up). The ``--net host`` option is given to
expose all open ports on the host as ray requires several bi-directional connections to workers. The `-v ...` option is
an example of mounted a local path into the container so it can access files (for example directory containing source
data, and output directory). It also exposes a dashboard at 8265 that can be viewed using a web browser.

On the workers, similarly start using the command:

```
docker run -d -v /path/to/data:/data --net host \
  dplutils /opt/startray.sh --block --address={head-node}:6379
```

For hosts with custome resources (e.g. other than those that get auto-detected such as CPUs and GPUs), you can pass
resources to the start command:

```
docker run -d --net host -v /path/to/data:/data \
  dplutils /opt/startray.sh --block --address={head-node}:6379 \
  --resources '{"mycustomresource": 1}'
```

In the dashboard you should see the workers listed in the clusters tab.

## Start pipeline

Pipelines can be run via interactive python sessions or asynchronously. In an interactive session one would import or
define a pipeline within the session and then call ``run`` or ``writeto`` method to kick off execution. For longer
running or production jobs it is generally advisable to submit a job to ray. dplutils contains helpers for making it
easy to run configurable pipelines via the command line. For example, assuming a script like:

```
from dplutils.pipeline import PipelineTask
from dplutils.pipeline.ray import RayDataPipelineExecutor
from dplutils.cli import cli_run

if __name__ == '__main__':
   pl = RayDataPipelineExecutor([PipelineTask('task1', lambda x: x.assign(newcol=1))])
   cli_run(pl)
```

We can submit the job in the following way, assuming a container is already running and has had the ray head node
started (here named `rayhead`; see above):

```
docker exec -it rayhead ray job submit -- python /path/to/script.py -o outdir
```

Note that as this is run within the container environment, the paths are what is exposed within and not necessarily the
same as in the host environment

The progress and log files can be viewed on the ray dashboard, and generated data will be available as a parquet table
written to ``/outdir`` (one file per batch, as they are completed)


# Installation, Build, & Run instructions

An "official" docker image is provided based on the latest release, but for development or those needing a custom build
or running outside of a containerized environment, below are instructions for installing the code from the source
repository.

## Setup

Install dependencies:

* ``pip install -r requirements/dev.txt``

## Tests

Run tox:

* ``tox -e test``, to run just the tests
* ``tox``, to run linting, tests and build. This should be run without errors prior to commit

## Docker

From the repo directory, run

``docker build -f docker/Dockerfile --tag dplutils .``


