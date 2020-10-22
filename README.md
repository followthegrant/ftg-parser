# follow the grant x follow the money

command line utillity for turning [pubmed
metadata](https://www.ncbi.nlm.nih.gov/pmc/tools/ftp/) into a
[followthemoney](https://docs.alephdata.org/developers/followthemoney) model.

## why?

it allows to model the schema in an easy way, and the [followthemoney
cli](http://github.com/alephdata/followthemoney/) together with
[followthemoney-store](https://github.com/alephdata/followthemoney-store)
allows easy data pipelines for input, output and transforming in different
formats, always being agnostic about the underlaying backend & infrastructure.

with this scripts collection it is easy to quickly have a look at the pubmed
dataset, export to csv for further analysis or import into an
[aleph](https://docs.alephdata.org/) database instance – everything in an easy,
reproducible way with existing toolchains.

## model

Articles: `Document` with the abstract text in property `summary`

*Authors*: `Person`

*Institutions*: `Organization`

*Authorship*: `Documentation` with `role` = 'author' between `Person` and `Document`

*Affiliations*: `Membership` with `role` = 'affiliated with' from `Person` in `Organization`

*Conflict of interest statement*: `Documentation` between `Person` and `Document`
with `role` = 'conflict of interest statement' and the statement in the
property `summary`

*Publisher*: `LegalEntity` with `Documentation` from `Document` via `role` = 'publisher'

## data

obtain pubmed source xml data from here: ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/

the `extract_pubmed.py` script expects the pdf csv to be able to set
`sourceUrl` properties to the `Document` entities:

        ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_non_comm_use_pdf.csv

## installation


        pip install -e https://gitlab.com/follow-the-grant/ftg-cli.git#egg=ftgftm

will install the package and all requirements. after it, type

        ftgftm

in your terminal to verify installation.

it uses the [pubmed parser](https://titipata.github.io/pubmed_parser/) with coi-extractment included.

for parallel bash processing, use [GNU parallel](https://www.gnu.org/software/parallel/)

## convert pubmed metadata to ftm

the command `ftgftm extract_pubmed` takes pubmed xml file paths as `stdin`,
creates ftm entities and prints their json strings line per line to `stdout`.
that allows easy piping for different usecases within or extending the ftm
toolchain.

### generate ftm entities

for a small amount of xml files, export them into a json file:

        find ./path_to_pubmed/ -type f -name "*xml" | ftgftm extract_pubmed | ftm aggregate | ftm validate > pubmed_ftm.jsonl

for the full dataset, use `ftm store` (sqlite works as backend but postgres is
faster, as it allows concurrent writing via `parallel` – please see the
documentation of [`followthemoney-store`](https://github.com/alephdata/followthemoney-store)
on how to setup env vars for the sql backend):

        find ./path_to_pubmed/ -type f -name "*xml" | parallel -j8 --pipe ftgftm extract_pubmed | parallel -j8 --pipe ftm store write -d ftg

with `-j8` you set the number of parallel jobs, this should be the numbers of
cores of the machine for best performance.

once the entities are loaded into the ftm store, they can be exported to csv
(into a folder called "csv" in this example):

        ftm store iterate -d ftg | ftm export-csv -o csv

or written into an aleph instance:

        ftm store iterate -d ftg | alephclient write-entities -f ftg


more infos about how to use `ftm` and `alephclient`:

        ftm --help
        alephclient --help

*and their project repos on github...*

