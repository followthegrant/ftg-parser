DATA_ROOT ?= data
FTM_STORE_URI ?= postgresql:///ftg

# PUBMED CENTRAL
pubmed.download:
	mkdir -p $(DATA_ROOT)/pubmed/src
	wget -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.xml.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/

pubmed.parse: src = extracted
pubmed.parse: pat = *.xml
pubmed.parse: parser = pubmed

# EUROPEPMC
europepmc.download:
	mkdir -p $(DATA_ROOT)/europepmc/src
	wget -P $(DATA_ROOT)/europepmc/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/oa/

europepmc.parse: src = src
europepmc.parse: pat = *.xml.gz
europepmc.parse: parser = europepmc

# BIORXIV
biorxiv.parse: src = src
biorxiv.parse: pat = *.xml
biorxiv.parse: parser = pubmed

# MEDRXIV
medrxiv.download:
	mkdir -p $(DATA_ROOT)/medrxiv/src
	# ca. 1.5 yrs back
	aws s3 sync s3://medrxiv-src-monthly/Current_Content/ $(DATA_ROOT)/src/ --request-payer requester
	# for all data:
	aws s3 sync s3://medrxiv-src-monthly/Back_Content/ $(DATA_ROOT)/src/ --request-payer requester


medrxiv.parse: src = src
medrxiv.parse: pat = *.xml
medrxiv.parse: parser = pubmed


%.parse:
	mkdir -p $(DATA_ROOT)/$*/json
	find $(DATA_ROOT)/$*/$(src)/ -type f -name "$(pat)" | parallel -N100 --pipe ftg parse $(parser) --store-json $(DATA_ROOT)/$*/json | parallel -N10000 --pipe ftg map-ftm | parallel -N10000 --pipe ftm store write -d ftg_$*


# wrangling

%.aggregate:
	ftm store iterate -d ftg_$* | parallel --pipe -N10000 ftm store write -d ftg_$*_aggregated -o aggregated
	ftm store delete -d ftg_$*

%.db:
	sed 's/@dataset/$*/g; s/@collection/ftm_ftg_$*_aggregated/g' ./psql/ftg_procedure.tmpl.sql | psql $(FTM_STORE_URI)

%.authors:
	psql $(FTM_STORE_URI) < ./psql/author_triples.sql
	find $(DATA_ROOT)/$*/json/ -type f -name "*.json" -exec cat {} \; | jq -c | parallel -N 1000 --pipe ftg author-triples --source $* | parallel -N1000 --pipe ftg psql insert author_triples
	ftg psql dedupe-authors author_triples --source biorxiv > $(DATA_ROOT)/$*/authors_deduped.csv

%.export:
	rm -rf $(DATA_ROOT)/$*/export/pg_dump
	mkdir -p $(DATA_ROOT)/$*/export/pg_dump
	pg_dump $(FTM_STORE_URI) -t $*_* -Fd -Z9 -O -j48 -f $(DATA_ROOT)/$*/export/pg_dump/data
	pg_dump $(FTM_STORE_URI) -t ftm_ftg_$*_aggregated -Fd -Z9 -O -j48 -f $(DATA_ROOT)/$*/export/pg_dump/ftm
	# tar cf - $(FTM_STORE_URI)/$*/export/pg_dump/data | parallel --pipe --recend '' --keep-order --block-size 1M "xz -9" > data.tar.xz
	# the above decreases size only .01 % as pg_dump compression is already very high
	tar cf - $(DATA_ROOT)/$*/json parallel --pipe --recend '' --keep-order --block-size 1M "xz -9" > $(DATA_ROOT)/$*/export

# psql docker
psql:
	docker run -p 5432:5432 -e POSTGRES_USER=ftg -e POSTGRES_PASSWORD=ftg -d postgres:13

# package

install:
	pip install -e .

install.dev:
	pip install coverage nose moto pytest pytest-cov black flake8 isort ipdb

test:
	pytest -s --cov=ftg --cov-report term-missing
