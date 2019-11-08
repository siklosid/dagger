import os
import shutil


def main(source: str, dest: str) -> None:
    """
    Moves files from source to destination

    Parameters
    ----------
    source: str
        Source path

    dest: str
        Destination path

    Returns
    -------
    None

    """

    if not os.path.exists(source) or not os.path.exists(dest):
        raise ValueError('Path specified does not exist')

    shutil.move(source, dest)

