.PHONY: wordcount test lab5 clean lab6 lab6-inspect lab7 lab7-inspect lab8-build lab8-deploy lab8-status lab8-logs lab8-delete

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

lab7:
	docker compose -f compose.lab7.yml up --build \
		--abort-on-container-exit --exit-code-from model-lab7 model-lab7

lab7-inspect:
	docker compose -f compose.lab7.yml run --rm --no-deps \
		--entrypoint python3 hbase-seed -m openfoodfacts_cluster.jobs.inspect_hbase

lab8-build:
	./scripts/k8s-build-images.sh

lab8-deploy:
	./scripts/k8s-kind-deploy.sh

lab8-status:
	kubectl get pods,jobs,statefulsets,deployments,services,pvc,pdb \
		-n openfoodfacts -o wide

lab8-logs:
	kubectl logs -n openfoodfacts -l spark-role=driver --tail=-1

lab8-delete:
	kind delete cluster --name openfoodfacts-lab8

clean:
	docker compose -f compose.lab5.yml down --remove-orphans
	docker compose -f compose.lab6.yml down --remove-orphans
	docker compose -f compose.lab7.yml down --remove-orphans
