package edu.itmo.datamart

final case class ServiceConfig(
    host: String,
    port: Int,
    zookeeperQuorum: String,
    zookeeperPort: Int,
    rawTable: String,
    preparedTable: String,
    metadataTable: String,
    resultsTable: String,
    runsTable: String,
    pageLimit: Int
)

object ServiceConfig {
    private def positiveInt(name: String, default: Int): Int = {
        val value = sys.env.get(name).fold(default)(_.toInt)
        require(value > 0, s"$name must be positive, got $value")
        value
    }

    def fromEnvironment(): ServiceConfig =
        ServiceConfig(
            host = sys.env.getOrElse("DATA_MART_HOST", "0.0.0.0"),
            port = positiveInt("DATA_MART_PORT", 8081),
            zookeeperQuorum = sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", "hbase"),
            zookeeperPort = positiveInt("HBASE_ZOOKEEPER_PORT", 2181),
            rawTable = sys.env.getOrElse("HBASE_RAW_TABLE", "off_products_raw"),
            preparedTable = sys.env.getOrElse("HBASE_PREPARED_TABLE", "off_products_prepared"),
            metadataTable = sys.env.getOrElse("HBASE_METADATA_TABLE", "off_datamart_meta"),
            resultsTable = sys.env.getOrElse("HBASE_RESULTS_TABLE", "off_cluster_results"),
            runsTable = sys.env.getOrElse("HBASE_RUNS_TABLE", "off_model_runs"),
            pageLimit = positiveInt("DATA_MART_PAGE_LIMIT", 1000)
        )
}