.PHONY: wordcount test lab5 clean lab6 lab6-inspect

wordcount:
	docker compose -f compose.lab5.yml run --rm wordcount

test:
	docker compose -f compose.lab5.yml run --rm tests 

lab5:
	docker compose -f compose.lab5.yml run --rm model
	docker compose -f compose.lab5.yml run --rm distribution

lab6:
	docker compose -f compose.lab6.yml up --build \
		--abort-on-container-exit --exit-code-from model-lab6 model-lab6

lab6-inspect:
	docker compose -f compose.lab6.yml run --rm --no-deps \
		--entrypoint python3 hbase-seed -m openfoodfacts_cluster.jobs.inspect_hbase

clean:
	docker compose -f compose.lab5.yml down --remove-orphans
	docker compose -f compose.lab6.yml down --remove-orphans