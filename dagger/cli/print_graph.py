import click

from dagger.config_finder.config_finder import ConfigFinder
from dagger.config_finder.config_processor import ConfigProcessor
from dagger.graph.task_graph import TaskGraph


def _print_graph(root_dir: str):
    cf = ConfigFinder(root_dir)
    cp = ConfigProcessor(cf)

    pipelines = cp.process_pipeline_configs()

    g = TaskGraph()
    for pipeline in pipelines:
        g.add_pipeline(pipeline)

    g.print_graph()


@click.command()
@click.option("--root", "-r", help="Root directory")
def print_graph(root: str) -> None:
    """
    Printing task template config
    """
    _print_graph(root)


if __name__ == "__main__":
    _print_graph("./")
