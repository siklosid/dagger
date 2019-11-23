from acirc.pipeline.io_factory import IOFactory
from acirc.utils import Printer

import sys
import click

io_factory = IOFactory()
valid_tasks = io_factory.factory.keys()


@click.command()
@click.option('--type', '-t', help='Type of task')
def init_io(type: str) -> None:
    """
    Printing io template config

    Parameters
    ----------
    type: str

    Returns
    -------
    Nonedd

    """

    Printer.print_success(io_factory.factory[type].sample())


@click.command()
def list_ios() -> None:
    """
    Printing valid io types

    Returns
    -------
    """

    Printer.print_success("\n".join(valid_tasks))


if __name__ == "__main__":
    print(io_factory.factory['redshift'].sample())


