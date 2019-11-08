import logging

import click

from acirc.utilities.ls import main
_logger = logging.getLogger('root')


@click.command()
@click.option('--path', '-p', help='Path of interest', default='.')
def ls(path: str) -> None:
    """
    List files in a directory

    Parameters
    ----------
    path: str

    Returns
    -------
    None

    """

    _logger.info('Initiating work')
    main(path)
