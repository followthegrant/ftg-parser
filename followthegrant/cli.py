import csv
import glob
import json
import os
import sys
from datetime import datetime

import click
from followthemoney.cli.util import MAX_LINE, write_object

from . import parse as parsers
from . import settings
from .coi import flag_coi
from .db import insert_many
from .dedupe import authors as dedupe
from .ftm import make_entities
from .logging import configure_logging, get_logger
from .schema import ArticleFullOutput
from .store import get_store
from .util import get_path
from .worker import DELETE_SOURCE, PARSE, QUEUES, STORE_JSON, BatchWorker, Worker

log = get_logger(__name__)


def readlines(stream):
    while True:
        line = stream.readline(MAX_LINE)
        if not line:
            return
        yield line.strip()


@click.group()
@click.option(
    "--log-level",
    default=settings.LOG_LEVEL,
    help="Set logging level",
    show_default=True,
)
def cli(log_level):
    configure_logging(log_level, sys.stderr)


@cli.command("parse")
@click.argument("parser")
@click.option("-f", "--file-path", type=click.Path(exists=True), default=None)
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option(
    "--store-json",
    help="Store parsed json into given directory (1 file per article)",
    type=click.Path(exists=True),
)
@click.option("-d", "--dataset", help="Append source (dataset) column with this value")
def parse(parser, file_path, infile, outfile, store_json=None, dataset=None):
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
    if file_path is not None:
        paths = [file_path]
    else:
        paths = readlines(infile)
    for fpath in paths:
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
    generate author triples (+ property type) for institutions and co-authors:

    fingerprint,author_id,coauthor_id,"coauthor"
    fingerprint,author_id,institution_id,"affiliation"

    optionally append `dataset` value to each row:

    fingerprint,author_id,institution_id,prop_type,dataset
    ...
    """
    writer = csv.writer(outfile)
    for data in readlines(infile):
        data = json.loads(data)
        data = ArticleFullOutput(**data)
        for triple in dedupe.explode_triples(data):
            row = triple
            if dataset is not None:
                row += dataset
            writer.writerow(row)


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


@cli.group()
def db():
    pass


@db.command("init")
@click.option(
    "--recreate/--no-recreate",
    help="Recreate database if it already exists.",
    default=False,
    show_default=True,
)
def db_init(recreate):
    """
    initialize required tables in Clickhouse
    """
    store = get_store()
    store.init(recreate=recreate)


@db.command("insert")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-t", "--table", required=True, help="Database table name to write to")
@click.option("-c", "--columns", required=True, help="Comma separated list of columns")
def db_insert(infile, table, columns):
    """
    bulk insert of stdin csv format to clickhouse database defined via
    `DATABASE_URI`

    currently a very simple approach: input csv without header, all columns must
    be present and in order of the existing `table`
    """
    insert_many(table, columns.split(","), csv.reader(infile))


@db.command("dedupe-authors")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-d", "--dataset", help="Filter triples for this dataset")
def dedupe_authors(outfile, dataset=None):
    """
    dedupe authors via triples and output `canonical_id,author_id` pairs
    to `outfile`
    """
    writer = csv.writer(outfile)
    for pair in dedupe.dedupe_from_db(dataset):
        pair += (dataset,)
        writer.writerow(pair)


@db.command("update-canonical")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-d", "--dataset", help="Restrict to dataset, otherwise for all datasets")
def update_canonical(outfile, dataset=None):
    """
    update canonical ids based on author triples for `dataset` or for all datasets
    and write result as csv to `outfile`
    """
    df = dedupe.update_canonical(dataset)
    df.to_csv(outfile, index=False)


@cli.group(invoke_without_command=True)
@click.option("--queue", "-q", multiple=True, help="Listen to queue(s)")
@click.pass_context
def worker(ctx, queue):
    if ctx.invoked_subcommand is None:
        log.info(f"Using data root: `{settings.DATA_ROOT}`")
        worker = Worker(queues=list(queue))
        worker.run()


@worker.command("batch")
@click.option("--queue", "-q", multiple=True, help="Listen to queue(s)")
@click.option(
    "--heartbeat",
    help="Heartbeat interval in seconds",
    type=int,
    default=5,
    show_default=True,
)
@click.option(
    "--batch_size", type=int, help="Batch size", default=1_000, show_default=True
)
def batch_worker(queue, heartbeat, batch_size):
    worker = BatchWorker(queues=list(queue), heartbeat=heartbeat, batch_size=batch_size)
    worker.run()


@worker.command("crawl")
@click.argument("parser")
@click.argument("pattern")
@click.option("-d", "--dataset", help="name of the dataset", required=True)
@click.option(
    "--delete-source/--no-delete-source",
    help="Delete source files after processing",
    default=False,
    show_default=True,
)
@click.option("--job-id", help="Job ID, will be auto generated if empty")
@click.option(
    "--store-json",
    help="Store parsed json into given directory (1 file per article)",
    type=click.Path(exists=False),
)
def crawl(parser, pattern, dataset, delete_source=False, store_json=None, job_id=None):
    worker = Worker()
    payload = {
        "parser": parser,
        "dataset": dataset,
        "delete_source": delete_source,
        "store_json": store_json,
        "job_id": job_id or datetime.now().isoformat(),
    }
    queues = set(QUEUES.keys())
    if not delete_source:
        queues.discard(DELETE_SOURCE)
    if store_json is not None:
        store_json = get_path(store_json)
    else:
        queues.discard(STORE_JSON)
    payload["allowed_queues"] = list(queues)
    for fp in glob.glob(get_path(pattern)):
        worker.dispatch(PARSE, {**payload, **{"fpath": fp}})
    worker.shutdown()
