#!/usr/bin/env bash
set -euo pipefail

docker build --target runtime -t openfoodfacts-model:lab8 -f Dockerfile.model .
docker build -t openfoodfacts-data-mart:lab8 -f Dockerfile.datamart .
docker build -t openfoodfacts-hbase:lab8 -f Dockerfile.hbase .