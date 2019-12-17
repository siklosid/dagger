from acirc.pipeline.pipeline import Pipeline
from acirc.utils import Printer


import click


@click.command()
def init_pipeline() -> None:
    """
    Printing pipeline template config
    """

    Printer.print_success(Pipeline.sample())


if __name__ == "__main__":
    main()
