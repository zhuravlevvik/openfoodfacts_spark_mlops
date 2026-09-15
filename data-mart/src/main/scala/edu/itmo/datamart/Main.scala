package edu.itmo.datamart

import org.apache.spark.sql.SparkSession

object Main {
    def main(args: Array[String]): Unit = {
        val config = ServiceConfig.fromEnvironment()
        val spark = SparkSession.builder()
            .appName("openfoodfacts-data-mart")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        
        spark.sparkContext.hadoopConfiguration.set("hbase.zookeper.quorum", config.zookeperQuorum)
        spark.sparkContext.hadoopConfiguration.set(
            "hbase.zookeper.property.clientPort",
            config.zookeeperPort.toString
        )

        val repository = new HBaseSparkRepository(config, spark)
        repository.healthcheck()

        val store = new DatasetStore(config, repository)
        val service = new DataMartHttpService(config, store)
        service.runUntilShutdown()
        spark.stop()
    }
}