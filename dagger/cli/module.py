import click
from dagger.utilities.module import Module
from dagger.utils import Printer


@click.command()
@click.option("--config_file", "-c", help="Path to module config file")
@click.option("--target_dir", "-t", help="Path to directory to generate the task configs to")
def generate_tasks(config_file: str, target_dir: str) -> None:
    """
    Generating tasks for a module based on config
    """

    module = Module(config_file, target_dir)
    module.generate_task_configs()

    Printer.print_success("Tasks are successfully generated")


@click.command()
def module_config() -> None:
    """
    Printing module config file
    """
    Printer.print_success(Module.module_config_template())
