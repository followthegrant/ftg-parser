import click

from .extract_pubmed import extract
from .get_fulltext_coi import extract_fulltext_coi as _extract_fulltext_coi


@click.group()
def cli():
    pass


@cli.command('extract_pubmed')
@click.option('--debug/--no-debug', help='Enable debug mode: Raise on errors.', show_default=True, default=False)
def extract_pubmed(debug):
    extract(debug)


@cli.command('extract_fulltext_coi')
def extract_fulltext_coi():
    _extract_fulltext_coi()
