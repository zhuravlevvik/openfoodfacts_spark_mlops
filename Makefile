.PHONY: wordcount test lab5 clean

wordcount:
	docker compose -f compose.lab5.yml run --rm wordcount

test:
	docker compose -f compose.lab5.yml run --rm tests 

lab5:
	docker compose -f compose.lab5.yml run --rm model
	docker compose -f compose.lab5.yml run --rm distribution

clean:
	docker compose -f compose.lab5.yml down --remove-orphans