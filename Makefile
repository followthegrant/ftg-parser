export DATA_ROOT ?= `pwd`/data
export FTM_STORE_URI ?= postgresql:///ftg
export PSQL_PORT ?= 5432
export PSQL_SHM ?= 1g
export INGESTORS_LID_MODEL_PATH=./models/lid.176.ftz
export LOG_LEVEL ?= info

# PUBMED CENTRAL
pubmed.parse: pubmed.download pubmed.extract pubmed.crawl
pubmed.wrangle: pubmed.authors pubmed.tables
pubmed.export: pubmed.export_json pubmed.export_db
pubmed.download:
	mkdir -p $(DATA_ROOT)/pubmed/src
	wget --inet4-only -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/
	wget --inet4-only -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/
	wget --inet4-only -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_other/xml/
pubmed.extract:
	mkdir -p $(DATA_ROOT)/pubmed/extracted
	parallel tar -C $(DATA_ROOT)/pubmed/extracted -xvf ::: $(DATA_ROOT)/pubmed/src/*.tar.gz
pubmed.crawl: pat = extracted/*/*xml
pubmed.crawl: parser = jats

# EUROPEPMC
europepmc.parse: europepmc.download europepmc.extract europepmc.crawl
europepmc.wrangle: europepmc.authors europepmc.tables
europepmc.export: europepmc.export_json europepmc.export_db
europepmc.download:
	mkdir -p $(DATA_ROOT)/europepmc/src
	wget -P $(DATA_ROOT)/europepmc/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/oa/
europepmc.crawl: pat = src/*.xml.gz
europepmc.crawl: parser = europepmc

# EUROPEPMC PREPRINTS
europepmc_ppr.parse: europepmc_ppr.download europepmc_ppr.extract europepmc_ppr.crawl
europepmc_ppr.wrangle: europepmc_ppr.authors europepmc_ppr.tables
europepmc_ppr.export: europepmc_ppr.export_json europepmc_ppr.export_db
europepmc_ppr.download:
	mkdir -p $(DATA_ROOT)/europepmc_ppr/src
	wget -P $(DATA_ROOT)/europepmc_ppr/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/preprint_fulltext
europepmc_ppr.crawl: pat = src/*.xml.gz
europepmc_ppr.crawl: parser = europepmc

# BIORXIV
biorxiv: biorxiv.parse biorxiv.authors biorxiv.aggregate biorxiv.tables biorxiv.export biorxiv.upload
biorxiv.parse: src = src
biorxiv.parse: pat = *.xml
biorxiv.parse: parser = jats
biorxiv.parse: chunksize = 1000

# MEDRXIV
medrxiv.parse: medrxiv.crawl
medrxiv.wrangle: medrxiv.authors medrxiv.aggregate medrxiv.tables medrxiv.export
medrxiv.download:
	mkdir -p $(DATA_ROOT)/medrxiv/src
	# ca. 250 GB ~ 20$ ? FIXME
	aws --profile aws s3 sync s3://medrxiv-src-monthly/ $(DATA_ROOT)/medrxiv/src/ --request-payer requester
medrxiv.crawl: pat = src/*/*/*.meca
medrxiv.crawl: parser = medrxiv

# SEMANTICSCHOLAR
semanticscholar: semanticscholar.download semanticscholar.parse semanticscholar.authors semanticscholar.aggregate semanticscholar.tables semanticscholar.export semanticscholar.upload
semanticscholar.download:
	mkdir -p $(DATA_ROOT)/semanticscholar/src
	aws s3 cp --no-sign-request --recursive s3://ai2-s2-research-public/open-corpus/2021-12-01/ $(DATA_ROOT)/semanticscholar/src
semanticscholar.parse: src = src
semanticscholar.parse: pat = s2-corpus-*.gz
semanticscholar.parse: parser = semanticscholar
semanticscholar.parse: chunksize = 1

# OPENAIRE
openaire: openaire.parse openaire.authors openaire.aggregate openaire.tables openaire.export openaire.upload
openaire.parse: src = src
openaire.parse: pat = part-*.json.gz
openaire.parse: parser = openaire
openaire.parse: chunksize = 1

# OPENAIRE COVID SUBSET
openaire_covid: openaire_covid.download openaire_covid.parse openaire_covid.authors openaire_covid.aggregate openaire_covid.tables openaire_covid.export openaire_covid.upload
openaire_covid.download:
	mkdir -p $(DATA_ROOT)/openaire_covid/src
	mkdir -p $(DATA_ROOT)/openaire_covid/extracted
	wget -P $(DATA_ROOT)/openaire_covid/src https://zenodo.org/record/4736827/files/COVID-19.tar
	tar -xvf $(DATA_ROOT)/openaire_covid/src/COVID-19.tar -C $(DATA_ROOT)/openaire_covid/extracted
openaire_covid.parse: src = extracted
openaire_covid.parse: pat = part-*.json.gz
openaire_covid.parse: parser = openaire
openaire_covid.parse: chunksize = 1

init:
	docker-compose up -d
	psql $(FTM_STORE_URI) < ./psql/author_dedupe.sql
	touch ./init

%.crawl:
	ftm store delete -d $*
	mkdir -p $(DATA_ROOT)/$*/json
	docker-compose run --rm worker ftg worker crawl $(parser) "$*/$(pat)" -d $* --store-json "$*/json" --delete-source

%.download_json:
	mkdir -p $(DATA_ROOT)/$*/json
	aws --profile ftg --endpoint-url $(S3_ENDPOINT) s3 cp s3://followthegrant/$*/export/json.tar.xz $(DATA_ROOT)/$*/json/
	tar -xvf $(DATA_ROOT)/$*/json/json.tar.xz -C $(DATA_ROOT)/$*/json/ --strip-components=5

%.parse_json:
	ftm store delete -d $*
	find $(DATA_ROOT)/$*/json/ -type f -name "*.json" -exec cat {} \; | jq -c | parallel -N1000 --pipe ftg map-ftm | parallel -N10000 --pipe ftm store write -d $*

# author dedupe
%.authors:
	psql $(FTM_STORE_URI) -c "copy (select a.fingerprint from (select fingerprint, count(author_id) from author_triples where dataset = '$*' group by fingerprint) a where a.count > 1) to stdout" | parallel -N1000 --pipe ftg db dedupe-authors -d $* | parallel --pipe -N10000 ftg db insert -t author_aggregation
	ftg db yield-dedupe-entities -d $* | parallel -N1000 --pipe ftg db rewrite-inplace -d $*

%.tables:
	sed 's/@dataset/$*/g; s/@collection/ftm_$*/g' ./psql/ftg_tables.tmpl.sql | psql $(FTM_STORE_URI)

%.index:
	sed 's/@dataset/$*/g; s/@collection/ftm_$*/g' ./psql/ftg_index.tmpl.sql | psql $(FTM_STORE_URI)

%.export_json:
	mkdir -p $(DATA_ROOT)/$*/export
	tar cf - $(DATA_ROOT)/$*/json | parallel --pipe --recend '' --keep-order --block-size 1M "xz -9" > $(DATA_ROOT)/$*/export/json.tar.xz

%.export_db:
	rm -rf $(DATA_ROOT)/$*/export/pg_dump
	mkdir -p $(DATA_ROOT)/$*/export/pg_dump
	pg_dump $(FTM_STORE_URI) -t $*_* -Fd -Z9 -O -j48 -f $(DATA_ROOT)/$*/export/pg_dump/data
	pg_dump $(FTM_STORE_URI) -t ftm_$* -Fd -Z9 -O -j48 -f $(DATA_ROOT)/$*/export/pg_dump/ftm

%.upload:
	aws --profile ftg --endpoint-url $(S3_ENDPOINT) s3 sync $(DATA_ROOT)/$*/export s3://followthegrant/$*/export

%.sync:
	aws --profile ftg --endpoint-url $(S3_ENDPOINT) s3 sync --delete s3://followthegrant/$*/export $(DATA_ROOT)/$*/export

%.pg_restore:
	pg_restore -d $(FTM_STORE_URI) $(DATA_ROOT)/$*/export/pg_dump/data
	sed 's/@dataset/$*/g; s/@collection/ftm_$*/g' ./psql/ftg_index.tmpl.sql | psql $(FTM_STORE_URI)


# STANDALONE SERVICES (rabbit psql)

.PHONY: psql
psql:
	mkdir -p $(DATA_ROOT)/psql/data
	docker run --shm-size=$(PSQL_SHM) -p $(PSQL_PORT):5432 -v $(DATA_ROOT)/psql/data:/var/lib/postgresql/data -e POSTGRES_USER=ftg -e POSTGRES_PASSWORD=ftg -d postgres:latest > ./psql/docker_id
	sleep 5
	psql $(FTM_STORE_URI) < ./psql/alter_system.sql
	docker restart `cat ./psql/docker_id`

psql.dev:
	mkdir -p $(DATA_ROOT)/psql/data
	docker run --shm-size=$(PSQL_SHM) -p $(PSQL_PORT):5432 -v $(DATA_ROOT)/psql/data:/var/lib/postgresql/data -e POSTGRES_USER=ftg -e POSTGRES_PASSWORD=ftg -d postgres:latest > ./psql/docker_id
	sleep 5
	psql $(FTM_STORE_URI) < ./psql/alter_system_local.sql
	docker restart `cat ./psql/docker_id`

psql.%:
	docker $* `cat ./psql/docker_id`

rabbitmq:
	docker run -p 5672:5672 -p 8080:15672 --hostname ftg-rabbit rabbitmq:management-alpine


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

install: spacy
	pip install -e .

install.dev:
	pip install coverage nose moto pytest pytest-cov black flake8 isort ipdb

test:
	pytest -s --cov=ftg --cov-report term-missing
