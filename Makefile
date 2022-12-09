export DATA_ROOT ?= `pwd`/data
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
	aws s3 cp --no-sign-request --recursive s3://ai2-s2-research-public/open-corpus/2022-05-01/ $(DATA_ROOT)/semanticscholar/src
semanticscholar.parse: src = src
semanticscholar.parse: pat = s2-corpus-*.gz
semanticscholar.parse: parser = semanticscholar

# OPENAIRE
openaire: openaire.parse openaire.authors openaire.aggregate openaire.tables openaire.export openaire.upload
openaire.parse: src = src
openaire.parse: pat = part-*.json.gz
openaire.parse: parser = openaire

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

init:
	docker-compose up -d
	sleep 10
	docker-compose run --rm worker ftg db init

%.crawl:
	mkdir -p $(DATA_ROOT)/$*/json
	docker-compose run --rm worker ftg worker crawl $(parser) "$*/$(pat)" -d $* --store-json "$*/json" --delete-source

%.download_json:
	mkdir -p $(DATA_ROOT)/$*/json
	aws --profile ftg --endpoint-url $(S3_ENDPOINT) s3 cp s3://followthegrant/$*/export/json.tar.xz $(DATA_ROOT)/$*/json/
	tar -xvf $(DATA_ROOT)/$*/json/json.tar.xz -C $(DATA_ROOT)/$*/json/ --strip-components=5

# generate canonical ids (deduping)
%.canonical:
	ftg db update-canonical -d $*

%.export_json:
	mkdir -p $(DATA_ROOT)/$*/export
	tar cf - $(DATA_ROOT)/$*/json | parallel --pipe --recend '' --keep-order --block-size 1M "xz -9" > $(DATA_ROOT)/$*/export/json.tar.xz

%.export_ftm:
	mkdir -p $(DATA_ROOT)/$*/export
	ftm cstore iterate -d $* | xz -9 -c > $(DATA_ROOT)/$*/export/$*.ftm.ijson.xz

%.upload:
	aws --profile ftg --endpoint-url $(S3_ENDPOINT) s3 sync $(DATA_ROOT)/$*/export s3://followthegrant/$*/export

%.sync:
	aws --profile ftg --endpoint-url $(S3_ENDPOINT) s3 sync --delete s3://followthegrant/$*/export $(DATA_ROOT)/$*/export


# import testdata
sampledata:
	ftg worker crawl jats "biorxiv/*xml" -d biorxiv
	ftg worker crawl jats "pubmed/*xml"  -d pubmed
	ftg worker crawl medrxiv "medrxiv/*/*meca" -d medrxiv
	ftg worker crawl openaire "openaire/*" -d openaire
	ftg worker crawl cord "cord/*json" -d cord
	ftg worker crawl semanticscholar "semanticscholar/*" -d semanticscholar
	ftg worker crawl crossref "crossref/*" -d crossref
	ftg worker crawl europepmc "europepmc/*" -d europepmc


# services for dev purposes (rabbit, clickhouse)
rabbitmq:
	docker run -p 5672:5672 -p 15672:15672 --hostname ftg-rabbit rabbitmq:management-alpine

clickhouse:
	docker run -p 9000:9000 -p 8123:8123 --ulimit nofile=262144:262144 clickhouse/clickhouse-server

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
