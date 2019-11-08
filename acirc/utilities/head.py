
from acirc.utils import Printer as p


def main(path: str, lines: int = 10) -> None:
    """
    Prints the top n lines of a file to terminal

    Parameters
    ----------
    path: str
        Input path
    lines: int
        Number of lines to print

    Returns
    -------
    None
    """

    with open(path, 'r') as f:
        l = f.readlines()[0:lines]
        p.print_success('\n'.join(l))
