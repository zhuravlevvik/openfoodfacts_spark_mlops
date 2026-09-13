"""Minimal Spark action used to verify that the execution environment works."""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from openfoodfacts_cluster.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session("wordcount")
    try:
        words = (
            spark.read.text(args.input)
            .select(F.explode(F.split(F.lower("value"), r"\W+")).alias("word"))
            .filter(F.length("word") > 0)
        )
        for row in words.groupBy("word").count().orderBy(F.desc("count"), "word").collect():
            print(f"{row['word']}\t{row['count']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
