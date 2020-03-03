from dagger.utils import Printer
from dagger.utilities.module import Module

import click


@click.command()
@click.option('--module', '-m', help='Path to module directory')
@click.option('--module_config', '-c', help='Path to module_config_file')
def generate_tasks(module: str, module_config: str) -> None:
    """
    Generating tasks for a module based on config
    """

    module = Module(module, module_config)
    module.generate_task_configs()

    Printer.print_success("Tasks are successfully generated")


@click.command()
def module_config() -> None:
    """
    Printing module config file
    """
    Printer.print_success(Module.module_config_template())

