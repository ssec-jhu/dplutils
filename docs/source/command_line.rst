Executing via CLI
=================

The package provides some basic utilities to simplify configuration and execution of pipelines via the command line. In
the most basic case, one could use::


  from dplutils.cli import cli_run

  pipeline = ...instantiate executor from graph...

  if __name__ == "__main__":
      cli_run(pipeline)


This will setup an argument parser and add generic context and config setting arguments and call the `writeto` method
writing to directory specified on the command line. It also has a help method::


  python -m my_pipeline.main -h

  usage: main.py [-h] [-c SET_CONTEXT] [-s SET_CONFIG] [-o OUT_DIR]

  options:
  -h, --help            show this help message and exit
  -c SET_CONTEXT, --set-context SET_CONTEXT
                        set context parameter
  -s SET_CONFIG, --set-config SET_CONFIG
                        set configuration parameter
  -o OUT_DIR, --out-dir OUT_DIR
                        write results to directory

in the above code, it is assumed that the `my_pipeline.main` module (`main.py`) contains a main section with the
`cli_run` command executed, typically within a conditional block as in the previous example above. The above help
message serves also as an example of the default arguments that `cli_run` sets up and passes to the pipeline
configuration.

In cases where we need to extend the arguments, we can get the default set, add to it and then pass those to the
`cli_run` method::

  from dplutils.cli import cli_run, get_argparser

  ap = get_argparser()
  ap.add_argument("--myarg", ...)
  args = ap.parse_args()
  ... do something with args ...
  cli_run(pipeline, args)

This ensures we get the consistent set of defaults and run behavior while also supporting custom arguments.
