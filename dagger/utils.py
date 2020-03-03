import logging
import sys

import click


def setup_logging(loglevel):
    """
    Setup basic logging

    Parameters
    ----------
    loglevel: bool
        DEBUG if True, else INFO

    Returns
    -------
    None

    """

    level = logging.DEBUG if loglevel else logging.INFO

    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=level, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


class Printer(object):
    @staticmethod
    def print_header(text):
        click.secho("\n{}\n".format(text), fg="yellow")

    @staticmethod
    def print_warning(text):
        click.secho("\n{}\n".format(text), fg="magenta")

    @staticmethod
    def print_success(text):
        click.secho("\n{}\n".format(text), fg="green")

    @staticmethod
    def print_error(text):
        click.secho("\n{}\n".format(text), fg="red")

    @staticmethod
    def add_color(value, color):
        return click.style("{}".format(value), fg=color)
