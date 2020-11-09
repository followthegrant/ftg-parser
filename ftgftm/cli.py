import click
import csv
import sys
import pandas as pd

from .extract_pubmed import extract
from .get_fulltext_coi import extract_fulltext_coi as _extract_fulltext_coi
from .split_coi import split_coi


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


@cli.command('split_cois')
def split_cois():
    """
    Split coi statements from STDIN.
    Expects CSV format with a header row,
        1st column: deduplication identifier (document id etc.)
        2nd column: Author name
        3rd column: Author identifier
        4nd column: COI statement
    Author name: "FirstName LastName" or "FirstName MiddleName LastName"
    """
    df = pd.read_csv(sys.stdin).applymap(lambda x: x.strip() if isinstance(x, str) else x)
    doc_id, author_name, author_id, coi_statement = [str(df.columns[i]) for i in range(4)]
    df[author_name] = tuple(zip(df[author_name], df[author_id]))
    del df[author_id]
    df = df.groupby(doc_id).agg({
        author_name: tuple,
        coi_statement: lambda x: ' '.join(list(set(x)))
    }).drop_duplicates()
    writer = csv.writer(sys.stdout)
    writer.writerow((doc_id, author_name, author_id, coi_statement))
    for doc_id, row in df.iterrows():
        author_ids = dict(row[author_name])
        authors = list(set([tuple(a[0].split(' ', 1)) if ' ' in a[0] else (None, a[0]) for a in row[author_name]]))
        explicit_authors = []
        all_authors = None
        for a, statement in split_coi(row[coi_statement], authors):
            author = ' '.join([i for i in a if i]) if a != 'all' else 'all'
            author_id = author_ids.get(author)
            if author_id:
                explicit_authors.append(author_id)
                writer.writerow((doc_id, author, author_id, ' '.join(statement)))
            if a == 'all':
                all_authors = True
        if all_authors:
            for author, author_id in author_ids.items():
                if author_id not in explicit_authors:
                    writer.writerow((doc_id, author, author_id, ' '.join(statement)))
