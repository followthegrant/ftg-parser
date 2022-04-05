export DATA_ROOT ?= `pwd`/data
export FTM_STORE_URI ?= postgresql:///ftg
export PSQL_PORT ?= 5432
export PSQL_SHM ?= 1g
export INGESTORS_LID_MODEL_PATH=./models/lid.176.ftz

# PUBMED CENTRAL
pubmed: pubmed.parse pubmed.authors pubmed.aggregate pubmed.db pubmed.export # pubmed.upload
pubmed.reparse: pubmed.download_json pubmed.parse_json pubmed.authors pubmed.aggregate pubmed.db pubmed.export pubmed.upload
pubmed.download:
	mkdir -p $(DATA_ROOT)/pubmed/src
	wget -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/
	wget -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/
	wget -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_other/xml/
pubmed.extract:
	mkdir -p $(DATA_ROOT)/pubmed/extracted
	parallel tar -C $(DATA_ROOT)/pubmed/extracted -xvf ::: $(DATA_ROOT)/pubmed/src/*.tar.gz
pubmed.parse: src = extracted
pubmed.parse: pat = *xml
pubmed.parse: parser = jats
pubmed.parse: chunksize = 1000

# EUROPEPMC
europepmc: europepmc.parse europepmc.authors europepmc.aggregate europepmc.db europepmc.export europepmc.upload
europepmc.reparse: europepmc.download_json europepmc.parse_json europepmc.authors europepmc.aggregate europepmc.db europepmc.export europepmc.upload
europepmc.download:
	mkdir -p $(DATA_ROOT)/europepmc/src
	wget -P $(DATA_ROOT)/europepmc/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/oa/
europepmc.parse: src = src
europepmc.parse: pat = *.xml.gz
europepmc.parse: parser = europepmc
europepmc.parse: chunksize = 1

# EUROPEPMC PREPRINTS
europepmc_ppr: europepmc_ppr.download europepmc_ppr.parse europepmc_ppr.authors europepmc_ppr.aggregate europepmc_ppr.db europepmc_ppr.export europepmc_ppr.upload
europepmc_ppr.reparse: europepmc_ppr.download_json europepmc_ppr.parse_json europepmc_ppr.authors europepmc_ppr.aggregate europepmc_ppr.db europepmc_ppr.export europepmc_ppr.upload
europepmc_ppr.download:
	mkdir -p $(DATA_ROOT)/europepmc_ppr/src
	wget -P $(DATA_ROOT)/europepmc_ppr/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/preprint_fulltext
europepmc_ppr.parse: src = src
europepmc_ppr.parse: pat = *.xml.gz
europepmc_ppr.parse: parser = europepmc
europepmc_ppr.parse: chunksize = 1

# BIORXIV
biorxiv: biorxiv.parse biorxiv.authors biorxiv.aggregate biorxiv.db biorxiv.export biorxiv.upload
biorxiv.reparse: biorxiv.download_json biorxiv.parse_json biorxiv.authors biorxiv.aggregate biorxiv.db biorxiv.export biorxiv.upload
biorxiv.parse: src = src
biorxiv.parse: pat = *.xml
biorxiv.parse: parser = jats
biorxiv.parse: chunksize = 1000

# MEDRXIV
medrxiv: medrxiv.parse medrxiv.authors medrxiv.aggregate medrxiv.db medrxiv.export medrxiv.upload
medrxiv.reparse: medrxiv.download_json medrxiv.parse_json medrxiv.authors medrxiv.aggregate medrxiv.db medrxiv.export medrxiv.upload
medrxiv.download:
	mkdir -p $(DATA_ROOT)/medrxiv/src
	# ca. 1.5 yrs back
	aws s3 sync s3://medrxiv-src-monthly/Current_Content/ $(DATA_ROOT)/medrxiv/src/ --request-payer requester
	# for all data:
	# aws s3 sync s3://medrxiv-src-monthly/Back_Content/ $(DATA_ROOT)/src/ --request-payer requester
medrxiv.parse: src = src
medrxiv.parse: pat = *.meca
medrxiv.parse: parser = medrxiv
medrxiv.parse: chunksize = 1000

# SEMANTICSCHOLAR
semanticscholar: semanticscholar.download semanticscholar.parse semanticscholar.authors semanticscholar.aggregate semanticscholar.db semanticscholar.export semanticscholar.upload
semanticscholar.download:
	mkdir -p $(DATA_ROOT)/semanticscholar/src
	aws s3 cp --no-sign-request --recursive s3://ai2-s2-research-public/open-corpus/2021-12-01/ $(DATA_ROOT)/semanticscholar/src
semanticscholar.parse: src = src
semanticscholar.parse: pat = s2-corpus-*.gz
semanticscholar.parse: parser = semanticscholar
semanticscholar.parse: chunksize = 1

# OPENAIRE
openaire: openaire.parse openaire.authors openaire.aggregate openaire.db openaire.export openaire.upload
openaire.parse: src = src
openaire.parse: pat = part-*.json.gz
openaire.parse: parser = openaire
openaire.parse: chunksize = 1

# OPENAIRE COVID SUBSET
openaire_covid: openaire_covid.download openaire_covid.parse openaire_covid.authors openaire_covid.aggregate openaire_covid.db openaire_covid.export openaire_covid.upload
openaire_covid.download:
	mkdir -p $(DATA_ROOT)/openaire_covid/src
	mkdir -p $(DATA_ROOT)/openaire_covid/extracted
	wget -P $(DATA_ROOT)/openaire_covid/src https://zenodo.org/record/4736827/files/COVID-19.tar
	tar -xvf $(DATA_ROOT)/openaire_covid/src/COVID-19.tar -C $(DATA_ROOT)/openaire_covid/extracted
openaire_covid.parse: src = extracted
openaire_covid.parse: pat = part-*.json.gz
openaire_covid.parse: parser = openaire
openaire_covid.parse: chunksize = 1

# parse
%.parse:
	ftm store delete -d $*
	mkdir -p $(DATA_ROOT)/$*/author_triples
	mkdir -p $(DATA_ROOT)/$*/json
	psql $(FTM_STORE_URI) < ./psql/author_dedupe.sql
	find $(DATA_ROOT)/$*/$(src)/ -type f -name "$(pat)" | parallel -N$(chunksize) --pipe ftg parse $(parser) --store-json $(DATA_ROOT)/$*/json --author-triples $(DATA_ROOT)/$*/author_triples | parallel -N10000 --pipe ftg map-ftm | parallel -N10000 --pipe ftm store write -d $*

%.download_json:
	mkdir -p $(DATA_ROOT)/$*/json
	aws --endpoint-url $(S3_ENDPOINT) s3 cp s3://followthegrant/$*/export/json.tar.xz $(DATA_ROOT)/$*/json/
	tar -xvf $(DATA_ROOT)/$*/json/json.tar.xz -C $(DATA_ROOT)/$*/json/ --strip-components=5

%.parse_json:
	ftm store delete -d $*
	find $(DATA_ROOT)/$*/json/ -type f -name "*.json" -exec cat {} \; | jq -c | parallel -N1000 --pipe ftg map-ftm | parallel -N10000 --pipe ftm store write -d $*

# wrangling
%.authors:
	# psql $(FTM_STORE_URI) < ./psql/author_dedupe.sql
	# find $(DATA_ROOT)/$*/json/ -type f -name "*.json" -exec cat {} \; | jq -c | parallel -N 10000 --pipe ftg author-triples -d $* | parallel --pipe -N10000 ftg db insert -t author_triples
	# find $(DATA_ROOT)/author_triples -type f -exec sort -u {} \; | parallel --pipe -N10000 ftg db insert -t author_triples
	# psql $(FTM_STORE_URI) -c "copy (select a.fingerprint from (select fingerprint, count(author_id) from author_triples where dataset = '$*' group by fingerprint) a where a.count > 1) to stdout" | parallel -N1000 --pipe ftg db dedupe-authors -d $* | parallel --pipe -N10000 ftg db insert -t author_aggregation
	find $(DATA_ROOT)/$*/author_triples -type f -exec sort -u {} \; | parallel --pipe -N10000 ftg dedupe-triples -d $* | parallel --pipe -N10000 ftg db insert -t author_aggregation


%.aggregate:
	ftm store delete -d $*_aggregated
	ftm store iterate -d $* | parallel --pipe -N10000 ftg db rewrite-author-ids | parallel --pipe -N10000 ftm store write -d $*_aggregated -o aggregated
	ftm store delete -d $*
	# FIXME ? re-aggregate aufter author dedupe
	ftm store iterate -d $*_aggregated | parallel --pipe -N10000 ftm store write -d $* -o aggregated

%.db:
	sed 's/@dataset/$*/g; s/@collection/ftm_$*/g' ./psql/ftg_procedure.tmpl.sql | psql $(FTM_STORE_URI)

%.export:
	rm -rf $(DATA_ROOT)/$*/export/pg_dump
	mkdir -p $(DATA_ROOT)/$*/export/pg_dump
	pg_dump $(FTM_STORE_URI) -t $*_* -Fd -Z9 -O -j48 -f $(DATA_ROOT)/$*/export/pg_dump/data
	pg_dump $(FTM_STORE_URI) -t ftm_$* -Fd -Z9 -O -j48 -f $(DATA_ROOT)/$*/export/pg_dump/ftm
	tar cf - $(DATA_ROOT)/$*/json | parallel --pipe --recend '' --keep-order --block-size 1M "xz -9" > $(DATA_ROOT)/$*/export/json.tar.xz

%.upload:
	aws --endpoint-url $(S3_ENDPOINT) s3 sync $(DATA_ROOT)/$*/export s3://followthegrant/$*/export

%.sync:
	aws --endpoint-url $(S3_ENDPOINT) s3 sync s3://followthegrant/$*/export $(DATA_ROOT)/$*/export

%.pg_restore:
	pg_restore -d $(FTM_STORE_URI) $(DATA_ROOT)/$*/export/pg_dump/data

# psql docker
.PHONY: psql
psql:
	mkdir -p $(DATA_ROOT)/psql/data
	docker run --shm-size=$(PSQL_SHM) -p $(PSQL_PORT):5432 -v $(DATA_ROOT)/psql/data:/var/lib/postgresql/data -e POSTGRES_USER=ftg -e POSTGRES_PASSWORD=ftg -d postgres:latest > ./psql/docker_id
	sleep 5
	psql $(FTM_STORE_URI) < ./psql/alter_system.sql
	docker restart `cat ./psql/docker_id`

psql.%:
	docker $* `cat ./psql/docker_id`

psql.start_local:
	docker run --shm-size=$(PSQL_SHM) -p $(PSQL_PORT):5432 -v $(DATA_ROOT)/psql/data:/var/lib/postgresql/data -e POSTGRES_USER=ftg -e POSTGRES_PASSWORD=ftg -d postgres:latest > ./psql/docker_id
	sleep 5
	psql $(FTM_STORE_URI) < ./psql/alter_system_local.sql
	docker restart `cat ./psql/docker_id`

# spacy dependencies
spacy:
	python3 -m spacy download en_core_web_sm
	python3 -m spacy download de_core_news_sm
	python3 -m spacy download fr_core_news_sm
	python3 -m spacy download es_core_news_sm
	python3 -m spacy download ru_core_news_sm
	python3 -m spacy download pt_core_news_sm
	python3 -m spacy download ro_core_news_sm
	python3 -m spacy download mk_core_news_sm
	python3 -m spacy download el_core_news_sm
	python3 -m spacy download pl_core_news_sm
	python3 -m spacy download it_core_news_sm
	python3 -m spacy download lt_core_news_sm
	python3 -m spacy download nl_core_news_sm
	python3 -m spacy download nb_core_news_sm
	python3 -m spacy download da_core_news_sm


# package

install:
	pip install -e .

install.dev:
	pip install coverage nose moto pytest pytest-cov black flake8 isort ipdb

test:
	pytest -s --cov=ftg --cov-report term-missing
