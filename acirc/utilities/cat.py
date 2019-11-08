import logging

from acirc.utils import Printer as p

_logger = logging.getLogger('root')


def main(path: str) -> None:
    """
    Cat contents of a file

    Parameters
    ----------
    path: str
        Input path

    Returns
    -------
    None
    """

    with open(path, 'r') as f:
        l = f.readlines()
        _logger.debug('Debugging this run for no reason')
        p.print_success('\n'.join(l))
