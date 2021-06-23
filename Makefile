ALEPH_COLLECTION=ftg_full_rev2

all: init pull diff download extract import push clean
test: init pull diff download.test extract import

init:
	mkdir -p ./state/current
	mkdir -p ./data/download
	mkdir -p ./data/extracted

pull:
	rclone --config ./rclone.conf sync aws:followthegrant/state/current ./state/current

diff:
	rclone --config ./rclone.conf sync pubmed:pub/pmc/oa_file_list.txt ./data/
	csvcut -H -K 1 -t -c 1 ./data/oa_file_list.txt | sort > ./state/current/files.pubmed
	sort -o ./state/current/files.imported ./state/current/files.imported
	comm -23 ./state/current/files.pubmed ./state/current/files.imported > ./state/current/files.diff
	wc -l ./state/current/files.diff

download:
	rclone --config ./rclone.conf --no-traverse --files-from ./state/current/files.diff copy pubmed:pub/pmc/ ./data/download/

download.test:
	shuf ./state/current/files.diff | head -100 > ./data/testfiles
	rclone --config ./rclone.conf --no-traverse --files-from ./data/testfiles copy pubmed:pub/pmc/ ./data/download/

extract:
	find ./data/download/ -type f -name "*.tar.gz" | parallel tar --wildcards -C ./data/extracted/ "*xml" -xf {}

import:
	find ./data/extracted/ -type f -name "*xml" | parallel --pipe ftgftm extract_pubmed | parallel --pipe ftm store write -d ftg_update_`date '+%Y-%m-%d'`
	ftm store iterate -d ftg_update_`date '+%Y-%m-%d'` | alephclient write-entities -f $(ALEPH_COLLECTION)
	cat ./state/current/files.diff >> ./state/current/files.imported

push:
	rclone --config ./rclone.conf copy ./state/ aws:followthegrant/state/

clean:
	rm -rf ./state
	rm -rf ./data
