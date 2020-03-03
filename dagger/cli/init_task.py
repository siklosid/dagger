from dagger.pipeline.task_factory import TaskFactory
from dagger.utils import Printer

import sys
import click

task_factory = TaskFactory()
valid_tasks = task_factory.factory.keys()


@click.command()
@click.option('--type', '-t', help='Type of task')
def init_task(type: str) -> None:
    """
    Printing task template config
    """

    Printer.print_success(task_factory.factory[type].sample())


@click.command()
def list_tasks() -> None:
    """
    Printing valid task types
    """

    Printer.print_success("\n".join(valid_tasks))


if __name__ == "__main__":
    task_factory.factory['batch'].sample()


