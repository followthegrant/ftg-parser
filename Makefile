all: init check download extract ftgftm upload

init:
	mkdir -p ./data/local

check:
	-rclone --config ./rclone.conf check pubmed-http:pub/pmc/oa_package/$(PREFIX) aws:pubmed-archive/$(PREFIX) --one-way --size-only --combined ./data/rclone.diff
	grep "^[+|*] " data/rclone.diff | sed "s/^..//g" > data/rclone.download
	wc -l data/rclone.download

download:
	rclone --config ./rclone.conf --files-from ./data/rclone.download sync -P pubmed-http:pub/pmc/oa_package/$(PREFIX) ./data/local

upload:
	rclone --config ./rclone.conf --files-from ./data/rclone.download sync -P pubmed-http:pub/pmc/oa_package/$(PREFIX) aws:pubmed-archive/$(PREFIX)

extract:
	find data/local -type f -name "*.tar.gz" -exec tar -xvf {} -C ./data/local/  \;

ftgftm:
	find ./data/local -type f -name "*xml" | ftgftm extract_pubmed | ftm store aggregate > ./data/entities.jsonl

clean:
	rm -rf ./data
