package edu.itmo.datamart

import org.apache.spark.ml.feature.{Imputer, StandardScaler, VectorAssembler}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.catalyst.dsl.ExpressionConversions

object ProductPreprocessor {
    final case class Feature(name: String, family: String, qualifier: String, maximum: Double)

    val features: Vector[Feature] = Vector(
        Feature("energy_kcal_100g", "nutrition", "energy_kcal_100g", 1000.0),
        Feature("fat_100g", "nutrition", "fat_100g", 100.0),
        Feature("saturated_fat_100g", "nutrition", "saturated_fat_100g", 100.0),
        Feature("carbohydrates_100g", "nutrition", "carbohydrates_100g", 100.0),
        Feature("sugars_100g", "nutrition", "sugars_100g", 100.0),
        Feature("fiber_100g", "nutrition", "fiber_100g", 100.0),
        Feature("proteins_100g", "nutrition", "proteins_100g", 100.0),
        Feature("salt_100g", "nutrition", "salt_100g", 100.0)
    )

    def prepare(raw: DataFrame): DataFrame = {
        require(raw.take(1).nonEmpty, "raw HBase table contains no products")
        var cleaned = raw
            .withColumn("code", trim(col("code")))
            .filter(col("code").isNotNull && length(col("code")) > 0)
        
        features.foreach { feature =>
            val numeric = regexp_replace(trim(col(feature.name)), ",", ".").cast(DoubleType)
            cleaned = cleaned.withColumn(
                feature.name,
                when(numeric.isNotNull && !isnan(numeric) && numeric.between(0.0, feature.maximum), numeric)
            )
        }

        val anyMeasurement = features.map(feature => col(feature.name).isNotNull).reduce(_ || _)
        cleaned = cleaned.filter(anyMeasurement).dropDuplicates("code")

        val inputColumns = features.map(_.name).toArray
        val imputedColumns = features.map(feature => s"${feature.name}__imputed").toArray
        val imputed = new Imputer()
            .setStrategy("median")
            .setInputCols(inputColumns)
            .setOutputCols(imputedColumns)
            .fit(cleaned)
            .transform(cleaned)
        val assembled = new VectorAssembler()
            .setInputCols(imputedColumns)
            .setOutputCol("unscaled_features")
            .transform(imputed)
        val scaled = new StandardScaler()
            .setInputCol("unscaled_features")
            .setOutputCol("features")
            .setWithMean(true)
            .setWithStd(true)
            .fit(assembled)
            .transform(assembled)

        val withFeatureArray = scaled.withColumn("feature_values", vector_to_array(col("features")))
        features.indices.foldLeft(
            withFeatureArray.select(
                col("code"),
                coalesce(col("product_name"), lit("")).as("product_name"),
                coalesce(col("categories"), lit("")).as("categories"),
                col("feature_values")
            )
        ) { case(frame, index) =>
            frame.withColumn(s"f$index", col("feature_values").getItem(index))
        }.drop("feature_values")
    }
}