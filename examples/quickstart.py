from pyspark.sql import SparkSession

from wappy.wappy import DeltaDataPipeline

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("DataPipeline")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )

    source_df = spark.createDataFrame([[1, 30], [2, 2], [3, 7]], ["id", "b"])
    db_name = "default"
    table_name = "test2"
    staging_name = f"{db_name}.{table_name}_staging"
    publish_name = f"{db_name}.{table_name}"

    pipeline = DeltaDataPipeline(spark, source_df, staging_name, publish_name, merge_keys=["id"])
    pipeline.run()
