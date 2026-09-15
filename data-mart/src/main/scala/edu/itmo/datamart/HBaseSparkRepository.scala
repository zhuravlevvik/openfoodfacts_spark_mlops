package edu.itmo.datamart

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

final class HBaseSparkRepository(config: ServiceConfig, spark: SparkSession) {
    private val connectorFormat = "org.apache.hadoop.hbase.spark"
    private val connectorOptions = Map(
        "hbase.spark.use.hbasecontext" -> "false",
        "hbase.spark.pushdown.columnfilter" -> "false",
        "hbase.spark.query.cacheblocks" -> "false",
        "hbase.spark.query.cachedrows" -> "500"
    )

    private def catalog(
        table: String,
        rowKey: String,
        columns: Vector[(String, String, String)]
    ): String = {
    val mappedColumns = (
        Vector(s""""$rowKey":{"cf":"rowkey","col":"$rowKey","type":"string"}""") ++
            columns.map { case (name, family, qualifier) =>
                s""""$name":{"cf":"$family","col":"$qualifier","type":"string"}"""
            }
        ).mkString(",")
        s"""{"table":{"namespace":"default","name":"$table"},"rowkey":"$rowKey","columns":{$mappedColumns}}"""
    }

    private val rawCatalog = catalog(
        config.rawTable,
        "code",
        Vector(
            ("product_name", "info", "product_name"),
            ("categories", "info", "categories")
        ) ++ ProductPreprocessor.features.map(feature =>
            (feature.name, feature.family, feature.qualifier)
        )
    )

    private val preparedColumns = Vector(
        ("version", "meta", "version"),
        ("code", "meta", "code"),
        ("product_name", "info", "product_name"),
        ("categories", "info", "categories")
    ) ++ ProductPreprocessor.features.indices.map(index =>
        (s"f$index", "feature", s"f$index")
    )
    private val preparedCatalog = catalog(config.preparedTable, "storage_key", preparedColumns)
    private val metadataCatalog = catalog(
        config.metadataTable,
        "metadata_key",
        Vector(
            ("version", "meta", "version"),
            ("count", "meta", "count"),
            ("created_at", "meta", "created_at")
        )
    )
    private val resultsCatalog = catalog(
        config.resultsTable,
        "result_key",
        Vector(
            ("run_id", "model", "run_id"),
            ("cluster", "model", "cluster"),
            ("created_at", "model", "created_at"),
            ("product_name", "info", "product_name"),
            ("categories", "info", "categories")
        )
    )
    private val runsCatalog = catalog(
        config.runsTable,
        "run_id",
        Vector(
            ("status", "run", "status"),
            ("completed_at", "run", "completed_at"),
            ("published_rows", "run", "published_rows"),
            ("silhouette", "run", "silhouette"),
            ("source", "run", "source")
        )
    )

    private def read(catalogJson: String): DataFrame =
        spark.read
            .options(connectorOptions + ("catalog" -> catalogJson))
            .format(connectorFormat)
            .load()

    
    private def write(frame: DataFrame, catalogJson: String): Unit =
        frame.write
            .options(connectorOptions + ("catalog" -> catalogJson))
            .format(connectorFormat)
            .save()
        
    def healthcheck(): Unit = {
        read(rawCatalog).limit(1).take(1)
        ()
    }

    def hasRawProducts(): Boolean = read(rawCatalog).limit(1).take(1).nonEmpty

    def rawProducts(): DataFrame = read(rawCatalog)

    def writePrepared(metadata: DatasetMetadata, prepared: DataFrame): Unit = {
        val featureColumns = ProductPreprocessor.features.indices.map(index => 
            col(s"f$index").cast("string").as(s"f$index")
        )
        val rows = prepared
            .withColumn("version", lit(metadata.version))
            .withColumn("storage_key", concat_ws(":", col("version"), col("code")))
        val outputColumns = Seq(
            col("storage_key"),
            col("version"),
            col("code"),
            col("product_name"),
            col("categories")
        ) ++ featureColumns
        write(rows.select(outputColumns: _*), preparedCatalog)
    }

    def writeMetadata(metadata: DatasetMetadata): Unit = {
        import spark.implicits._
        val row = Seq(("current", metadata.version, metadata.count.toString, metadata.createdAt))
            .toDF("metadata_key", "version", "count", "created_at")
        write(row, metadataCatalog)
    }

    def currentMetadata(): DatasetMetadata = {
        val rows = read(metadataCatalog)
            .filter(col("metadata_key") === "current")
            .select("version", "count", "created_at")
            .take(1)
        if (rows.isEmpty) throw new IllegalStateException("dataset has not been refreshed yet")
        DatasetMetadata(rows(0).getString(0), rows(0).getString(1).toInt, rows(0).getString(2))
    }

    def preparedForVersion(version: String): DataFrame = {
        val numericFeatures = ProductPreprocessor.features.indices.map(index =>
            col(s"f$index").cast("double").as(s"f$index")
        )
        val identityColumns = Seq(
            col("code"),
            col("product_name"),
            col("categories")
        )
        read(preparedCatalog)
            .filter(col("version") === version)
            .select((identityColumns ++ numericFeatures): _*)
    }

    def writeResults(request: PublishResultsRequest, products: DataFrame): Int = {
        require(request.predictions.nonEmpty, "predictions must not be empty")
        val predictionSchema = StructType(
            Vector(
                StructField("code", StringType, nullable = false),
                StructField("cluster", IntegerType, nullable = false)
            )
        )
        val predictionRows = request.predictions.map(prediction => Row(prediction.code, prediction.cluster))
        val predictions = spark.createDataFrame(
            spark.sparkContext.parallelize(predictionRows),
            predictionSchema
        )
        require(
            predictions.select("code").distinct().count() == request.predictions.size,
            "predictions contain duplicate product codes"
        )

        val joined = predictions.join(products, Seq("code"), "inner").cache()
        try {
            val joinedCount = joined.count().toInt
            require(
                joinedCount == request.predictions.size,
                "predictions contain unknown product codes"
            )
            val resultRows = joined
                .withColumn("run_id", lit(request.runId))
                .withColumn("created_at", lit(request.createdAt))
                .withColumn("result_key", concat_ws(":", col("run_id"), col("code")))
                .select(
                    col("result_key"),
                    col("run_id"),
                    col("cluster").cast("string").as("cluster"),
                    col("created_at"),
                    col("product_name"),
                    col("categories")
                )
            write(resultRows, resultsCatalog)

            import spark.implicits._
            val runRow = Seq(
                (
                    request.runId,
                    "complete",
                    request.createdAt,
                    joinedCount.toString,
                    request.silhouette.toString,
                    "data-mart-spark-connector"
                )
            ).toDF("run_id", "status", "completed_at", "published_rows", "silhouette", "source")
            write(runRow, runsCatalog)
            joinedCount
        } finally {
            joined.unpersist()
        }
    }
}
