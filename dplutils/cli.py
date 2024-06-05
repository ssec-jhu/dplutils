import json
from argparse import ArgumentParser, Namespace

from dplutils.pipeline import PipelineExecutor


def add_generic_args(argparser):
    """Add Pipeline generic args to parser

    The generic set of CLI arguments are as follows:

        - ``-c`` (``--set-context``): Set pipline context item.
        - ``-s`` (``--set-config``): Set pipline config item.
        - ``-o`` (``--out-dir``): Directory to write output files to.

    Args:
        argparser: The :class:`ArgumentParser<argparse.ArgumentParser>` instance
          to add args to.
    """
    argparser.add_argument("-c", "--set-context", action="append", default=[], help="set context parameter")
    argparser.add_argument("-s", "--set-config", action="append", default=[], help="set configuration parameter")
    argparser.add_argument("-o", "--out-dir", default=".", help="write results to directory")


def get_argparser(**kwargs):
    """Get an extensible argparser with default options built-in

    This is a thin wrapper over just instantiating an :class:`ArgumentParser`,
    but adds the default pipeline options as given in :meth:`add_generic_args`
    (see its documentation for list of arguments). See also
    :meth:`set_config_from_args` for details of how the ``set-context`` and
    ``set-config`` parmeters are handled.

    Args:
        kwargs: keyword arguments to pass to constructor of
            :class:`ArgumentParser`
    """
    ap = ArgumentParser(**kwargs)
    add_generic_args(ap)
    return ap


def parse_config_element(conf):
    k, v = conf.split("=", 1)
    try:
        v = json.loads(v)
    except json.decoder.JSONDecodeError:
        pass
    return k, v


def set_config_from_args(pipeline: PipelineExecutor, args: Namespace):
    """Configure pipeline using config from arguments

    Set context from the ``set-context`` argument and config from
    ``set-config``. Each will be parset as ``name=value`` pair, where the value
    is parsed as a JSON object, falling back to string.

    For ``set-config``, the ``name`` is of the form
    ``task.param[.subparam[...]]`` where ``task`` is the taskname, ``param`` is
    a parameter of :class:`PipelineTask<dplutils.pipeline.PipelineTask>`
    and ``subparam`` is a key within a dictionary of such a parameter (where
    applicable).
    """
    for ctx in args.set_context:
        pipeline.set_context(*parse_config_element(ctx))
    for conf in args.set_config:
        pipeline.set_config(*parse_config_element(conf))


def cli_run(pipeline: PipelineExecutor, args: Namespace | None = None, **argparse_kwargs):
    """Run pipeline from cli args

    If ``args`` is None, this function runs the pipeline for the standard set of
    workload-independent arguments (e.g. those that are generic to pipleines in
    general; see :meth:`add_generic_args`). If ``args`` is supplied, it must be an
    :class:`argparse.Namespace` object with command line arguments to be used to
    configure pipline (see also :meth:`set_config_grom_args`).

    The pipeline is run using the
    :meth:`PipelineExecutor.writeto<dplutils.pipeline.PipelineExecutor.writeto>`
    method to the ``out-dir`` cli argument.

    Args:
        pipeline: The pipeline to configure and run
        args: None or and argparse.Namespace object of parsed args
        argparse_kwargs: kwargs to be passed to
            :class:`ArgumentParser<argparse.ArgumentParser>` on instantiation
    """
    if args is None:
        args = get_argparser(**argparse_kwargs).parse_args()
    set_config_from_args(pipeline, args)
    pipeline.writeto(args.out_dir)
