export DATA_ROOT ?= `pwd`/data
export INGESTORS_LID_MODEL_PATH=./models/lid.176.ftz
export LOG_LEVEL ?= info

# PUBMED CENTRAL
pubmed.download:
	mkdir -p $(DATA_ROOT)/pubmed/src
	wget -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/
	wget -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/
	wget -P $(DATA_ROOT)/pubmed/src/ -r -l1 -H -nd -N -np -A "*.tar.gz" -e robots=off ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_other/xml/
pubmed.extract:
	mkdir -p $(DATA_ROOT)/pubmed/extracted
	parallel tar -C $(DATA_ROOT)/pubmed/extracted -xvf ::: $(DATA_ROOT)/pubmed/src/*.tar.gz
pubmed.crawl: pat = extracted/*/*xml
pubmed.crawl: parser = jats

# EUROPEPMC
europepmc.parse: europepmc.download europepmc.extract europepmc.crawl
europepmc.download:
	mkdir -p $(DATA_ROOT)/europepmc/src
	wget -P $(DATA_ROOT)/europepmc/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/oa/
europepmc.crawl: pat = src/*.xml.gz
europepmc.crawl: parser = europepmc

# EUROPEPMC PREPRINTS
europepmc_ppr.download:
	mkdir -p $(DATA_ROOT)/europepmc_ppr/src
	wget -P $(DATA_ROOT)/europepmc_ppr/src/ -r -l1 -H -nd -N -np -A "*.xml.gz" -e robots=off https://europepmc.org/ftp/preprint_fulltext
europepmc_ppr.crawl: pat = src/*.xml.gz
europepmc_ppr.crawl: parser = europepmc

# BIORXIV
biorxiv.parse: pat = src/*.xml
biorxiv.parse: parser = jats

# MEDRXIV
medrxiv.download:
	mkdir -p $(DATA_ROOT)/medrxiv/src
	# ca. 250 GB ~ 20$ ? FIXME
	aws --profile aws s3 sync s3://medrxiv-src-monthly/ $(DATA_ROOT)/medrxiv/src/ --request-payer requester
medrxiv.crawl: pat = src/*/*/*.meca
medrxiv.crawl: parser = medrxiv

# SEMANTICSCHOLAR
semanticscholar.download:
	mkdir -p $(DATA_ROOT)/semanticscholar/src
	aws s3 cp --no-sign-request --recursive s3://ai2-s2-research-public/open-corpus/2022-05-01/ $(DATA_ROOT)/semanticscholar/src
semanticscholar.parse: pat = src/s2-corpus-*.gz
semanticscholar.parse: parser = semanticscholar

# OPENAIRE
openaire.parse: pat = src/part-*.json.gz
openaire.parse: parser = openaire

# OPENAIRE COVID SUBSET
openaire_covid.download:
	mkdir -p $(DATA_ROOT)/openaire_covid/src
	mkdir -p $(DATA_ROOT)/openaire_covid/extracted
	wget -P $(DATA_ROOT)/openaire_covid/src https://zenodo.org/record/4736827/files/COVID-19.tar
	tar -xvf $(DATA_ROOT)/openaire_covid/src/COVID-19.tar -C $(DATA_ROOT)/openaire_covid/extracted
openaire_covid.parse: pat = extracted/part-*.json.gz
openaire_covid.parse: parser = openaire

%.crawl:
	docker-compose run --rm worker ftg worker crawl $(parser) "$*/$(pat)" -d $* --delete-source

%.export:
	mkdir -p $(DATA_ROOT)/$*/export
	ftm cstore iterate -d $* | xz -9 -c > $(DATA_ROOT)/$*/export/entities.ftm.ijson.xz

%.upload:
	aws --profile ftg --endpoint-url $(S3_ENDPOINT) s3 sync $(DATA_ROOT)/$*/export s3://data.followthegrant.org/$*


# import testdata
sampledata:
	ftg worker crawl jats "biorxiv/*xml" -d biorxiv
	ftg worker crawl jats "pubmed/*xml"  -d pubmed
	ftg worker crawl jats "medrxiv/*/*meca" -d medrxiv
	ftg worker crawl europepmc "europepmc/*" -d europepmc
	ftg worker crawl openalex "openalex/**/*.gz" -d openalex
	# ftg worker crawl openaire "openaire/*" -d openaire
	# ftg worker crawl s2orc "semanticscholar/**/*.gz" -d semanticscholar
	# ftg worker crawl cord "cord/*json" -d cord
	# ftg worker crawl crossref "crossref/*" -d crossref


# services for dev purposes (rabbit, clickhouse)
rabbitmq:
	docker run -p 5672:5672 -p 15672:15672 --hostname ftg-rabbit rabbitmq:management

clickhouse:
	docker run -p 9000:9000 -p 8123:8123 --ulimit nofile=262144:262144 clickhouse/clickhouse-server

# spacy dependencies
spacy:
	pip install spacy
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

install.dev: install
	pip install coverage nose moto pytest pytest-cov black flake8 isort ipdb

test:
	pytest -s --cov=ftg --cov-report term-missing
