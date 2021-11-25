import os
import csv
import json
import logging
import sys

import click

from . import load
from .coi import flag_coi
from .dedupe.authors import explode_triples, dedupe, dedupe_psql
from .ftm import make_entities
from .parse import parse_article
from .schema import ArticleFullOutput
from .psql import insert_many

log = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command("parse")
@click.argument("collection")
@click.option(
    "--store-json",
    help="Store parsed json into given directory (1 file per article)",
    type=click.Path(exists=True),
)
def parse(collection, store_json=None):
    """
    parse source xml/html files into json representation with metadata, authors,
    institutions and conflict of interest statements

    collection: one of
        pubmed
        europepmc
        semanticscholar
        openaire
        cord
    """
    loader = getattr(load, collection)
    for fpath in sys.stdin:
        fpath = fpath.strip()
        try:
            data = loader(fpath)
        except Exception as e:
            log.error(f"Cannot load `{fpath}`: '{e}'")
            data = None
        if data is not None:
            for d in data:
                try:
                    d = parse_article(d)
                    res = json.dumps(d.dict(), default=lambda x: str(x))
                    if store_json is not None:
                        fp = os.path.join(store_json, d.id + ".json")
                        with open(fp, "w") as f:
                            f.write(res)
                    sys.stdout.write(res + "\n")
                except Exception as e:
                    log.error(f"Cannot parse `{fpath}`: '{e}'")


@cli.command("map-ftm")
def ftm():
    """
    parse input json into ftm entities
    """
    for data in sys.stdin.readlines():
        data = json.loads(data)
        data = ArticleFullOutput(**data)
        for entity in make_entities(data):
            sys.stdout.write(json.dumps(entity.to_dict()) + "\n")


@cli.command("author-triples")
@click.option("--source", help="Append source column with this value")
def author_triplets(source=None):
    """
    generate author triples for institutions and co-authors:

    fingerprint,author_id,coauthor_id
    fingerprint,author_id,institution_id

    optionally append `source` value to each row:

    fingerprint,author_id,institution_id,source_name
    ...
    """
    for data in sys.stdin:
        data = json.loads(data)
        data = ArticleFullOutput(**data)
        for triple in explode_triples(data):
            out = ",".join(triple)
            if source is not None:
                out += f",{source}"
            sys.stdout.write(out + "\n")


@cli.command("dedupe")
def _dedupe():
    """
    dedupe data based on triples,
    returns matching id pairs
    """
    triples = csv.reader(sys.stdin)
    for pair in dedupe(triples):
        sys.stdout.write(",".join(pair) + "\n")


@cli.command("flag-cois")
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


@cli.group()
def psql():
    pass


@psql.command("insert")
@click.argument("table")
def psql_insert(table):
    """
    bulk upsert of stdin csv format to psql database defined via
    `FTM_STORE_URI`

    currently a very simple approach: input csv withou header, all columns must
    be present and in order of the existing `table`
    doesn't complain if a row already exists, but will not update it
    """
    rows = csv.reader(sys.stdin)
    insert_many(table, rows)


@psql.command("dedupe-authors")
@click.argument("table")
@click.option("--source", help="Filter for only this source")
def psql_dedupe_authors(table, source=None):
    for pair in dedupe_psql(table, source):
        sys.stdout.write(",".join(pair) + "\n")
