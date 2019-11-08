# -*- coding: utf-8 -*-

"""Console script for dao."""

import click

from acirc.cli.ls import ls
from acirc.cli.mv import mv
from acirc.cli.head import head
from acirc.cli.cat import cat
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


cli.add_command(ls)
cli.add_command(mv)
cli.add_command(head)
cli.add_command(cat)
