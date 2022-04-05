import csv
import json
import logging
import os

import click
from followthemoney.cli.util import MAX_LINE, write_object
from pathlib import Path

from . import parse as parsers
from .coi import flag_coi
from .db import insert_many
from .dedupe import authors as dedupe
from .ftm import make_entities
from .schema import ArticleFullOutput
from .statements import Statement, statements_from_entity

log = logging.getLogger(__name__)


def readlines(stream):
    while True:
        line = stream.readline(MAX_LINE)
        if not line:
            return
        yield line.strip()


@click.group()
def cli():
    pass


@cli.command("parse")
@click.argument("parser")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option(
    "--store-json",
    help="Store parsed json into given directory (1 file per article)",
    type=click.Path(exists=True),
)
@click.option(
    "--author-triples",
    help="Write author triples to this directory",
    type=click.Path(exists=True)
)
@click.option("-d", "--dataset", help="Append source (dataset) column with this value")
def parse(parser, infile, outfile, store_json=None, author_triples=None, dataset=None):
    """
    parse source xml/html files into json representation with metadata, authors,
    institutions and conflict of interest statements

    parser: one of
        jats (pubmed, *rxiv)
        europepmc
        semanticscholar
        openaire
        cord
    """
    parser = getattr(parsers, parser)
    for fpath in readlines(infile):
        try:
            data = parser(fpath)
        except Exception as e:
            log.error(f"Cannot load `{fpath}`: '{e}'")
            data = None
        if data is not None:
            for d in data:
                try:
                    res = json.dumps(d.dict(), default=lambda x: str(x), sort_keys=True)
                    if store_json is not None:
                        fp = os.path.join(store_json, d.id + ".json")
                        with open(fp, "w") as f:
                            f.write(res)
                    outfile.write(res + "\n")
                except Exception as e:
                    log.error(f"Cannot parse `{fpath}`: '{e}'")

                if author_triples is not None:
                    for triple in dedupe.explode_triples(d):
                        if dataset is not None:
                            triple += (dataset,)
                        path = Path(f'{author_triples}/{",".join(triple)}')
                        path.touch()


@cli.command("map-ftm")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
def map_ftm(infile, outfile):
    """
    parse input json into ftm entities
    """
    for data in readlines(infile):
        data = json.loads(data)
        data = ArticleFullOutput(**data)
        for entity in make_entities(data):
            write_object(outfile, entity)


@cli.command("author-triples")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-d", "--dataset", help="Append source (dataset) column with this value")
def author_triplets(infile, outfile, dataset=None):
    """
    generate author triples for institutions and co-authors:

    fingerprint,author_id,coauthor_id
    fingerprint,author_id,institution_id

    optionally append `dataset` value to each row:

    fingerprint,author_id,institution_id,dataset
    ...
    """
    for data in readlines(infile):
        data = json.loads(data)
        data = ArticleFullOutput(**data)
        for triple in dedupe.explode_triples(data):
            out = ",".join(triple)
            if dataset is not None:
                out += f",{dataset}"
            outfile.write(out + "\n")


@cli.command("dedupe-triples")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-d", "--dataset", help="Append source (dataset) column with this value")
def dedupe_triples(infile, outfile, dataset=None):
    """
    dedupe data based on triples,
    returns matching id pairs
    """
    triples = csv.reader(infile)
    for pair in dedupe.dedupe_triples(triples):
        out = ",".join(pair)
        if dataset is not None:
            out += f",{dataset}"
        outfile.write(out + "\n")


@cli.command("flag-cois")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
def flag_cois(infile, outfile):
    """
    Flag COI statements if there is a conflict (1) or not (0)
    Expects CSV from STDIN without header row to be able to process in parallel
    first column must be the coi text, all other columns will be passed through.
    a new column is appended after the coi text column with the 0/1 flag

    example parallel use (omit csv header via tail):

    cat cois.csv | tail -n +2 | parallel --pipe ftgftm flag_cois > cois.flagged.csv
    """
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    for row in reader:
        try:
            coi = row[0]
            flag = int(flag_coi(coi))
            writer.writerow((row[0], flag, *row[1:]))
        except Exception as e:
            log.error(f"{e.__class__.__name__}: {e}")
            log.error(str(row))


@cli.command("to-statements")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-d", "--dataset", required=True)
def to_statements(infile, outfile, dataset):
    writer = csv.DictWriter(outfile, fieldnames=Statement.__annotations__.keys())
    writer.writeheader()
    for entity in readlines(infile):
        entity = json.loads(entity)
        for statement in statements_from_entity(entity, dataset):
            writer.writerow(statement)


@cli.group()
def db():
    pass


@db.command("insert")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-t", "--table", required=True, help="Database table name to write to")
def db_insert(infile, table):
    """
    bulk upsert of stdin csv format to database defined via
    `FTM_STORE_URI`

    currently a very simple approach: input csv without header, all columns must
    be present and in order of the existing `table`
    doesn't complain if a row already exists, but will not update it

    # FIXME when using with `echo <many lines> | parallel --pipe ftg db insert ..`
    this can cause deadlocks on postgresql!!
    """
    rows = []
    for ix, row in enumerate(csv.reader(infile)):
        rows.append(row)
        if ix % 10000 == 0:
            insert_many(table, rows)
            rows = []
    if rows:
        insert_many(table, rows)


@db.command("dedupe-authors")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option(
    "-t",
    "--table",
    default="author_triples",
    help="Database table to read triples from",
    show_default=True,
)
@click.option("-d", "--dataset", help="Filter triples for this dataset")
def dedupe_authors(infile, outfile, table, dataset=None):
    """
    dedupe authors via triples table `table`
    based on fingerprints coming from infile
    """
    for fingerprint in readlines(infile):
        for pair in dedupe.dedupe_db(table, fingerprint, dataset):
            out = ",".join(pair)
            if dataset is not None:
                out += f",{dataset}"
            outfile.write(out + "\n")


@db.command("rewrite-author-ids")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option(
    "-t",
    "--table",
    default="author_aggregation",
    help="Database table to read aggregated IDs from",
    show_default=True,
)
@click.option("-d", "--dataset", help="Filter IDs for this dataset")
def db_rewrite_authors(infile, outfile, table, dataset=None):
    """
    rewrite author ids from db table with stored (agg_id, author_id) pairs
    """
    for entity in readlines(infile):
        entity = json.loads(entity)
        entity = dedupe.rewrite_entity(table, entity, dataset)
        outfile.write(json.dumps(entity) + "\n")
