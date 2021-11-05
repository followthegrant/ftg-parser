import json
import csv
import logging
import sys

import click

from . import load
from .schema import ArticleFullOutput
from .parse import parse_article
from .coi import flag_coi
from .ftm import make_entities

log = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command("parse")
@click.argument("collection")
# @click.option(
#     "--debug/--no-debug",
#     help="Enable debug mode: Raise on errors.",
#     show_default=True,
#     default=False,
# )
# @click.option(
#     "--meta-only/--all",
#     help="Extract only article metadata (without authors as ftm entities)",
#     show_default=True,
#     default=False,
# )
# def parse(debug, meta_only):
def parse(collection):
    """
    parse source xml/html files into json representation with metadata, authors,
    institutions and conflict of interest statements

    collection: one of "pubmed", "biorxiv", "medrxiv", "aerzteblatt"
    """
    loader = getattr(load, collection)
    for fpath in sys.stdin:
        fpath = fpath.strip()
        data = loader(fpath)
        if data is not None:
            data = parse_article(data)
            sys.stdout.write(json.dumps(data.dict(), default=lambda x: str(x)) + "\n")


@cli.command("map-ftm")
def ftm():
    """
    parse input json into ftm entities
    """
    for data in sys.stdin:
        data = json.loads(data)
        data = ArticleFullOutput(**data)
        for entity in make_entities(data):
            sys.stdout.write(json.dumps(entity.to_dict()) + "\n")


@cli.command("flag_cois")
def flag_cois():
    """
    Flag COI statements if there is a conflict (1) or not (0)
    Expects CSV from STDIN without header row to be able to process in parallel
    first column must be the coi text, all other columns will be passed through.
    a new column is appended after the coi text column with the 0/1 flag

    example parallel use (omit csv header via tail):

    cat cois.csv | tail -n +2 | parallel --pipe ftgftm flag_cois > cois.flagged.csv
    """
    reader = csv.reader(sys.stdin)
    writer = csv.writer(sys.stdout)
    for row in reader:
        try:
            coi = row[0]
            flag = int(flag_coi(coi))
            writer.writerow((row[0], flag, *row[1:]))
        except Exception as e:
            log.error(f"{e.__class__.__name__}: {e}")
            log.error(str(row))
