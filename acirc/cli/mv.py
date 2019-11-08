import logging

import click

from acirc.utils import Printer as p
from acirc.utilities.mv import main
_logger = logging.getLogger('root')


@click.command()
@click.option('--source', '-s', help='Source path', default='.')
@click.option('--dest', '-d', help='Destination path', default='.')
def mv(source: str, dest: str) -> None:
    """
    Moves files from source to destination

    Parameters
    ----------
    source: str
    dest: str

    Returns
    -------
    None

    """
    main(source, dest)
    p.print_success('Successfully moved files')

