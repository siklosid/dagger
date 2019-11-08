import logging

import click

from acirc.utilities.head import main

_logger = logging.getLogger('root')


@click.command()
@click.option('--path', '-p', help='Path of interest', default='.')
@click.option('--lines', '-n', help='Path of interest', default='.')
def head(path: str, lines: int) -> None:
    """
    Prints the top n lines of a file to terminal

    Parameters
    ----------
    lines: int
    path: str

    Returns
    -------
    None

    """
    _logger.info('Initiating work')
    main(path, rows)


