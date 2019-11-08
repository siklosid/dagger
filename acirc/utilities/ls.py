import os

from acirc.utils import Printer as p


def main(path: str) -> None:
    """
    List files in a directory

    Parameters
    ----------
    path: str
        Input path

    Returns
    -------
    None
    """

    if not os.path.exists(path):
        raise ValueError('Path specified does not exist')

    if os.path.isfile(path):
        raise ValueError('Path specified is a file')

    f = os.listdir(path)
    p.print_success('\n'.join(f))
