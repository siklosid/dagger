import sys

import click
from dagger.alerts.alert import AlertFactory
from dagger.utils import Printer

alert_factory = AlertFactory()
valid_alerts = alert_factory.factory.keys()


@click.command()
@click.option("--type", "-t", help="Type of alert")
def init_alert(type: str) -> None:
    """
    Printing io template config
    """

    Printer.print_success(alert_factory.factory[type].sample())


@click.command()
def list_alerts() -> None:
    """
    Printing valid io types
    """

    Printer.print_success("\n".join(valid_alerts))
