# -*- coding: utf-8 -*-

"""Console script for dao."""

import click

from acirc.cli.init_pipeline import init_pipeline
from acirc.cli.init_task import init_task, list_tasks
from acirc.cli.init_io import init_io, list_ios
from acirc.cli.module import generate_tasks, module_config
from acirc.utils import setup_logging


@click.group()
@click.option('-v', '--verbose', is_flag=True, default=False, help='Turn on debug logging')
@click.pass_context
def cli(context, verbose):
    """acirc's CLI. With it, you can perform pretty much all operations you desire
        Shown below are all the possible commands you can use.

        Run ::

            $ acirc --help

        To get an overview of the possibilities.
    """
    setup_logging(verbose)


cli.add_command(init_pipeline)
cli.add_command(init_task)
cli.add_command(list_tasks)
cli.add_command(init_io)
cli.add_command(list_ios)
cli.add_command(init_io)
cli.add_command(generate_tasks)
cli.add_command(module_config)
