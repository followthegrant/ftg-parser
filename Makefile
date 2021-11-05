all: clean install test

install:
	pip install -e .
	pip install coverage nose moto pytest pytest-cov black flake8 isort ipdb

test:
	pytest -s --cov=ftg --cov-report term-missing

testdata: testdata_pubmed

testdata_pubmed:
	mkdir -p testdata/pubmed
	cut -d, -f1 testdata/oa_file_list.csv | shuf | head -1000 > testdata/pubmed_subset
	while read path ; do \
		curl -L https://ftp.ncbi.nlm.nih.gov/pub/pmc/$$path | tar -xz -C testdata/pubmed --wildcards "*xml" ; \
	done < testdata/pubmed_subset

