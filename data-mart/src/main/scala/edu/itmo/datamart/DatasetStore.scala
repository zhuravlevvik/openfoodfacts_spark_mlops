package edu.itmo.datamart

import java.time.Instant
import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.shaded.org.checkerframework.checker.units.qual.m

final class DatasetStore(
    config: ServiceConfig,
    repository: HBaseSparkRepository
) {
    def hasRawProducts(): Boolean = repository.hasRawProducts()

    def refresh(): DatasetMetadata = synchronized {
        val prepared = ProductPreprocessor.prepare(repository.rawProducts()).cache()
        try {
            val metadata = DatasetMetadata(
                version = UUID.randomUUID().toString,
                count = prepared.count().toInt,
                createdAt = Instant.now().toString
            )
            repository.writePrepared(metadata, prepared)
            repository.writeMetadata(metadata)
            metadata
        } finally {
            prepared.unpersist()
        }
    }

    def currentMetadata(): DatasetMetadata = repository.currentMetadata()

    def currentPage(offset: Int, requestedLimit: Int): DatasetPage = {
        require(offset >= 0, "offset must not be negative")
        require(requestedLimit > 0, "limit must be positive")
        val limit = math.min(requestedLimit, config.pageLimit)
        val metadata = currentMetadata()
        val products = repository.preparedForVersion(metadata.version)
        val numbered = products.withColumn(
            "_row_number",
            row_number().over(Window.orderBy(col("code")))
        )
        val rows = numbered
            .filter(col("_row_number") > offset && col("_row_number") <= offset + limit)
            .drop("_row_number")
            .orderBy("code")
            .collect()
            .toVector
        val items = rows.map { row =>
            PreparedProduct(
                code = row.getAs[String]("code"),
                productName = Option(row.getAs[String]("product_name")).getOrElse(""),
                categories = Option(row.getAs[String]("categories")).getOrElse(""),
                features = ProductPreprocessor.features.indices.map(index =>
                    row.getAs[Double](s"$index")
                ).toVector
            )
        }
        DatasetPage(metadata.version, metadata.count, offset, limit, items)
    }

    def publishResults(request: PublishResultsRequest): PublishResultsResponse = {
        require(request.runId.nonEmpty, "run_id must not be empty")
        val metadata = currentMetadata()
        val published = repository.writeResults(
            request,
            repository.preparedForVersion(metadata.version)
        )
        PublishResultsResponse(request.runId, published, "complete")
    }
}