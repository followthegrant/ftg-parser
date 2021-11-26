DATA_ROOT ?= data
FTM_STORE_URI ?= postgresql:///ftg

# PUBMED CENTRAL
pubmed: pubmed.parse pubmed.authors pubmed.aggregate pubmed.db pubmed.export pubmed.upload
pubmed.download:
	mkdir -p $(DATA_ROOT)/pubmed/src
	wget -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.xml.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/
pubmed.parse: src = extracted
pubmed.parse: pat = *xml  # xml/nxml
pubmed.parse: parser = pubmed

# EUROPEPMC
europepmc: europepmc.parse europepmc.authors europepmc.aggregate europepmc.db europepmc.export europepmc.upload
europepmc.download:
	mkdir -p $(DATA_ROOT)/europepmc/src
	wget -P $(DATA_ROOT)/europepmc/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/oa/
europepmc.parse: src = src
europepmc.parse: pat = *.xml
europepmc.parse: parser = europepmc

# EUROPEPMC PREPRINTS
europepmc_ppr: europepmc_ppr.download europepmc_ppr.parse europepmc_ppr.authors europepmc_ppr.aggregate europepmc_ppr.db europepmc_ppr.export europepmc_ppr.upload
europepmc_ppr.download:
	mkdir -p $(DATA_ROOT)/europepmc_ppr/src
	wget -P $(DATA_ROOT)/europepmc_ppr/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/preprint_fulltext
europepmc_ppr.parse: src = src
europepmc_ppr.parse: pat = *.xml
europepmc_ppr.parse: parser = europepmc

# BIORXIV
biorxiv: biorxiv.parse biorxiv.authors biorxiv.aggregate biorxiv.db biorxiv.export biorxiv.upload
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
	find $(DATA_ROOT)/$*/$(src)/ -type f -name "$(pat)" | parallel -N1 --pipe ftg parse $(parser) --store-json $(DATA_ROOT)/$*/json | parallel -N10000 --pipe ftg map-ftm | parallel -N10000 --pipe ftm store write -d $*


# wrangling
%.authors:
	psql $(FTM_STORE_URI) < ./psql/author_triples.sql
	find $(DATA_ROOT)/$*/json/ -type f -name "*.json" -exec cat {} \; | jq -c | parallel -N 100 --pipe ftg author-triples --source $* | parallel -j1 --pipe -N10000 ftg db insert author_triples
	ftg db dedupe-authors | ftg db insert author_aggregation

%.aggregate:
	ftm store delete -d $*_aggregated
	ftm store iterate -d $* | parallel --pipe -N10000 ftg db rewrite-author-ids | parallel --pipe -N10000 ftm store write -d $*_aggregated -o aggregated
	ftm store delete -d $*

%.db:
	sed 's/@dataset/$*/g; s/@collection/ftm_$*_aggregated/g' ./psql/ftg_procedure.tmpl.sql | psql $(FTM_STORE_URI)

%.export:
	rm -rf $(DATA_ROOT)/$*/export/pg_dump
	mkdir -p $(DATA_ROOT)/$*/export/pg_dump
	pg_dump $(FTM_STORE_URI) -t $*_* -Fd -Z9 -O -j48 -f $(DATA_ROOT)/$*/export/pg_dump/data
	pg_dump $(FTM_STORE_URI) -t ftm_$*_aggregated -Fd -Z9 -O -j48 -f $(DATA_ROOT)/$*/export/pg_dump/ftm
	# tar cf - $(FTM_STORE_URI)/$*/export/pg_dump/data | parallel --pipe --recend '' --keep-order --block-size 1M "xz -9" > data.tar.xz
	# the above decreases size only .01 % as pg_dump compression is already very high
	tar cf - $(DATA_ROOT)/$*/json | parallel --pipe --recend '' --keep-order --block-size 1M "xz -9" > $(DATA_ROOT)/$*/export/json.tar.xz

%.upload:
	rsync -avz -e "ssh -p $(RSYNC_PORT)" --progress $(DATA_ROOT)/$*/export $(RSYNC_DEST)/followthegrant/$*/

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
