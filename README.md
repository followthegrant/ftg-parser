# followthegrant-parser

Parser for different article collections to extract metadata, authors and their
(individual) conflict of interest statements, if any, into a standarized `json` format.

Extracted data can be transformed into a
[followthemoney](https://followthemoney.readthedocs.io/en/latest/) model that
can be piped into an [aleph](https://docs.alephdata.org/) instance.

## overview

### currently supported collections
- [pubmed open access subset](https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/)
- [biorxiv](https://www.biorxiv.org/)
- in progress: medrxiv
- in progress: Deutsches Ärzteblatt

### features
- author deduplication via their associated institutions
- detect institution countries and assign them to authors
- extract conflict of interest statements
- split sentences from conflict of interest statements and assign them to specific authors
- flag a statement if it describes a conflict or not
- scripts make use of [python typing](https://docs.python.org/3/library/typing.html)
    and [pydantic](https://pydantic-docs.helpmanual.io/) to ensure data validation
- extensive test suite to ensure parsing works as expected

### example output
after parsing, the json result of an article looks like this:

```json
{
  "id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
  "title": "Ectasia risk factors in refractive surgery",
  "published_at": "2016-04-20",
  "abstract": "This review outlines risk factors of post-laser in situ keratomileusis (LASIK) ectasia that can be detected preoperatively and presents a new metric to be considered in the detection of ectasia risk. Relevant factors in refractive surgery screening include the analysis of intrinsic biomechanical properties (information obtained from corneal topography/tomography and patient\u2019s age), as well as the analysis of alterable biomechanical properties (information obtained from the amount of tissue altered by surgery and the remaining load-bearing tissue). Corneal topography patterns of placido disk seem to play a pivotal role as a surrogate of corneal strength, and abnormal corneal topography remains to be the most important identifiable risk factor for ectasia. Information derived from tomography, such as pachymetric and epithelial maps as well as computational strategies, to help in the detection of keratoconus is additional and relevant. High percentage of tissue altered (PTA) is the most robust risk factor for ectasia after LASIK in patients with normal preoperative corneal topography. Compared to specific residual stromal bed (RSB) or central corneal thickness values, percentage of tissue altered likely provides a more individualized measure of biomechanical alteration because it considers the relationship between thickness, tissue altered through ablation and flap creation, and ultimate RSB thickness. Other recognized risk factors include low RSB, thin cornea, and high myopia. Age is also a very important risk factor and still remains as one of the most overlooked ones. A comprehensive screening approach with the Ectasia Risk Score System, which evaluates multiple risk factors simultaneously, is also a helpful tool in the screening strategy.",
  "journal": {
    "id": "9d03ad6a37bbf4dec4878d21ab6f2f9afc3e66ae",
    "name": "Clinical Ophthalmology (Auckland, N.Z.)"
  },
  "authors": [
    {
      "id": "5ee2c50b04ddd72555db5410f6fe362036f03512",
      "name": "Marcony R Santhiago",
      "first_name": "Marcony R",
      "last_name": "Santhiago",
      "institutions": [
        {
          "id": "4007e685cb687256b78361bec82a243a14bf037a",
          "name": "Department of Ophthalmology, Federal University of S\u00e3o Paulo, S\u00e3o Paulo, Brazil",
          "country": "br"
        }
      ],
      "countries": [
        "br"
      ]
    },
    {
      "id": "d847b3a2e8b32dc1ef76a24d07d6cc89cb82ead2",
      "name": "Natalia T Giacomin",
      "first_name": "Natalia T",
      "last_name": "Giacomin",
      "institutions": [
        {
          "id": "4007e685cb687256b78361bec82a243a14bf037a",
          "name": "Department of Ophthalmology, Federal University of S\u00e3o Paulo, S\u00e3o Paulo, Brazil",
          "country": "br"
        }
      ],
      "countries": [
        "br"
      ]
    },
    {
      "id": "b197e07356885d0c2ecc6fd2011de74293404099",
      "name": "David Smadja",
      "first_name": "David",
      "last_name": "Smadja",
      "institutions": [
        {
          "id": "62845aa73dbc15653d3bbde47678bb13831c7e31",
          "name": "Ophthalmology Department, Tel Aviv Sourasky Medical Center, Tel Aviv, Israel",
          "country": "il"
        }
      ],
      "countries": [
        "il"
      ]
    },
    {
      "id": "8cf2a4a342e41e586f4fec7e23b0bd672e5df73d",
      "name": "Samir J Bechara",
      "first_name": "Samir J",
      "last_name": "Bechara",
      "institutions": [
        {
          "id": "4007e685cb687256b78361bec82a243a14bf037a",
          "name": "Department of Ophthalmology, Federal University of S\u00e3o Paulo, S\u00e3o Paulo, Brazil",
          "country": "br"
        }
      ],
      "countries": [
        "br"
      ]
    }
  ],
  "index_text": "pmid:27143849\ndoi:10.2147/OPTH.S51313\npmcid:4844427",
  "identifiers": [
    {
      "id": "40b29ed479b216666eb545ada1d6dfb684147841",
      "key": "pmid",
      "label": "PubMed ID",
      "value": "27143849"
    },
    {
      "id": "b5a8bfad1fc6879397c8c4d49f305bfe013e1b94",
      "key": "doi",
      "label": "Digital Object Identifier",
      "value": "10.2147/OPTH.S51313"
    },
    {
      "id": "8be8e69834c8d792d914770ef100063ae40825af",
      "key": "pmcid",
      "label": "PubMed Central ID",
      "value": "4844427"
    }
  ],
  "coi_statement": {
    "id": "7d2e27fbae1eb8e64860af1483f0cc31794b3848",
    "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
    "article_title": "Ectasia risk factors in refractive surgery",
    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
    "title": "conflict of interest statement (article)",
    "text": "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work.",
    "published_at": "2016-04-20",
    "flag": true,
    "index_text": "flag:1",
    "role": "conflict of interest statement (article)"
  },
  "individual_coi_statements": [
    {
      "id": "72e9487468b06df78555e2ddf6ef0fdd60f1f06e",
      "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
      "article_title": "Ectasia risk factors in refractive surgery",
      "author_id": "5ee2c50b04ddd72555db5410f6fe362036f03512",
      "author_name": "Marcony R Santhiago",
      "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
      "title": "individual conflict of interest statement (Marcony R Santhiago)",
      "text": "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). The authors report no other conflicts of interest in this work.",
      "published_at": "2016-04-20",
      "flag": true,
      "index_text": "flag:1",
      "role": "individual conflict of interest statement"
    },
    {
      "id": "366dce3f5a8fab389a9a62d1c71a39be5b79f530",
      "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
      "article_title": "Ectasia risk factors in refractive surgery",
      "author_id": "b197e07356885d0c2ecc6fd2011de74293404099",
      "author_name": "David Smadja",
      "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
      "title": "individual conflict of interest statement (David Smadja)",
      "text": "Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work.",
      "published_at": "2016-04-20",
      "flag": true,
      "index_text": "flag:1",
      "role": "individual conflict of interest statement"
    }
  ]
}

```

## installation

        pip install git+https://gitlab.com/follow-the-grant/ftg-parser.git#egg=followthegrant-parser

will install the package and all requirements. after it, type

        ftg --help

in your terminal to verify installation.

for parallel bash processing, install [GNU parallel](https://www.gnu.org/software/parallel/) (optional)

## usage

### parse articles into json

the parse function takes a file path as an input (typically to a `xml` or
`html` file of an article) and produces `json` output

via command line:

        find ./data/pubmed/ -type f -name "*xml" | ftg parse pubmed > pubmed.jsonl

or in python:

```python
from ftg import load, parse

fpath = "/pubmed/PMC4844427/opth-10-713.nxml"
data = load.pubmed(fpath)
data = parse.parse_article(data)  # json dict as described above
```

### create ftm entities

just pipe the generated `json` (see above) to `ftg map-ftm`

via command line (see below for better performance in huge datasets):

        find ./data/pubmed/ -type f -name "*xml" | ftg parse pubmed | ftg map-ftm | ftm aggregate > entities.jsonl

or in python:

```python
from ftg import load, parse, ftm

fpath = "/pubmed/PMC4844427/opth-10-713.nxml"
data = load.pubmed(fpath)
data = parse.parse_article(data)
for entity in ftm.make_entities(data):
    yield entity
```

### bulk datasets processing

for a lot of articles, use parallelization and `ftm store` (sqlite works as
backend but postgres is faster, as it allows concurrent writing via `parallel`
– please see the documentation of
[`followthemoney-store`](https://github.com/alephdata/followthemoney-store) on
how to setup env vars for the sql backend):

        find ./path_to_pubmed/ -type f -name "*xml" | parallel -j8 --pipe ftg parse pubmed | parallel -j8 --pipe ftg map-ftm | parallel -j8 --pipe ftm store write -d ftg_pubmed

with `-j8` you set the number of parallel jobs, this should be the numbers of
cores of the machine for best performance.

once the entities are loaded into the ftm store, they can be exported to csv
(into a folder called "csv" in this example):

        ftm store iterate -d ftg_pubmed | ftm export-csv -o csv

or written into an aleph instance:

        ftm store iterate -d ftg_pubmed | alephclient write-entities -f ftg

more infos about how to use `ftm` and `alephclient`:

        ftm --help
        alephclient --help

and their documentations:
- https://docs.alephdata.org/developers/followthemoney
- https://docs.alephdata.org/developers/alephclient

## testing

check out this repo locally to make sure to have testdata available.

then to install dev dependencies and run tests:

    make install
    make test

## more info regarding ftm

### why followthemoney model?

it allows to model the schema in an easy way, and the [followthemoney
cli](http://github.com/alephdata/followthemoney/) together with
[followthemoney-store](https://github.com/alephdata/followthemoney-store)
allows easy data pipelines for input, output and transforming in different
formats, always being agnostic about the underlaying backend & infrastructure.

with this scripts collection it is easy to quickly have a look at the pubmed
dataset and other collections, export to csv for further analysis or import into an
[aleph](https://docs.alephdata.org/) database instance – everything in an easy,
reproducible way with existing toolchains.

### ftm model

*Publishers*: `LegalEntity`

*Articles*: `Article` with the abstract text in property `summary`

*Authors*: `Person`

*Institutions*: `Organization`

*Authorship*: `Documentation` with `role` = 'author' between `Person` and `Document`

*Affiliations*: `Membership` with `role` = 'affiliated with' from `Person` in `Organization`

*Conflict of interest statement*: `PlainText` with the statement in the property `summary`,
connected to both `Author` and `Article` via `Documentation`
with `role` = 'conflict of interest statement'

### mapping

find the complete FTM mapping here: [mapping.yml](./ftg/mapping.yml)

