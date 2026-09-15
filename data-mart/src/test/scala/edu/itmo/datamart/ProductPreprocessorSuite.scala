package edu.itmo.datamart

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ProductPreprocessorSuite extends munit.FunSuite {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("data-mart-tests")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = Option(spark).foreach(_.stop())

  test("preprocessor keeps transformations inside a DataFrame") {
    val rows = Vector(
      row("1", "100", "10", "2", "20", "5", "3", "7", "0.5"),
      row("2", "200", "20", "4", "40", "10", "6", "14", "1.0"),
      row("3", "300", "30", "6", "60", "15", "9", "21", null),
      row("3", "999", "99", "9", "90", "90", "9", "9", "9")
    )
    val schema = StructType(
      Vector(
        StructField("code", StringType),
        StructField("product_name", StringType),
        StructField("categories", StringType)
      ) ++ ProductPreprocessor.features.map(feature => StructField(feature.name, StringType))
    )
    val raw = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val prepared = ProductPreprocessor.prepare(raw).collect().toVector

    assertEquals(prepared.map(_.getAs[String]("code")).toSet, Set("1", "2", "3"))
    assert(prepared.forall(row => ProductPreprocessor.features.indices.forall { index =>
      val value = row.getAs[Double](s"f$index")
      !value.isNaN && !value.isInfinity
    }))
  }

  private def row(code: String, values: String*): Row =
    Row.fromSeq(Vector(code, s"Product $code", "Test") ++ values)
}