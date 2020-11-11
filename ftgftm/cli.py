import click
import csv
import sys
import pandas as pd

from .extract_pubmed import extract
from .get_fulltext_coi import extract_fulltext_coi as _extract_fulltext_coi
from .split_coi import split_coi
from .flag_cois import flag_coi


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
    All other columns will be passed through
    Author name: "FirstName LastName" or "FirstName MiddleName LastName"
    """
    df = pd.read_csv(sys.stdin).applymap(lambda x: x.strip() if isinstance(x, str) else x).fillna('')
    doc_id, author_name, author_id, coi_statement = [str(df.columns[i]) for i in range(4)]
    df[author_name] = tuple(zip(df[author_name], df[author_id]))
    del df[author_id]
    df = df.groupby(doc_id).agg({**{
        author_name: tuple,
        coi_statement: lambda x: ' '.join(list(set(x)))
    }, **{c: 'first' for c in df.columns[3:]}}).drop_duplicates()
    writer = csv.writer(sys.stdout)
    writer.writerow((doc_id, author_name, author_id, coi_statement, *df.columns[2:]))
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
                writer.writerow((doc_id, author, author_id, ' '.join(statement), *[row[c] for c in df.columns[2:]]))
            if a == 'all':
                all_authors = True
        if all_authors:
            for author, author_id in author_ids.items():
                if author_id not in explicit_authors:
                    writer.writerow((doc_id, author, author_id, ' '.join(statement), *[row[c] for c in df.columns[2:]]))


@cli.command('flag_cois')
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
        coi = row[0]
        flag = int(flag_coi(coi))
        writer.writerow((row[0], flag, *row[1:]))
