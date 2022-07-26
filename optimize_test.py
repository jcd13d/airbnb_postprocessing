import pyspark
from delta import *


pathToTable = "s3://airbnb-scraper-bucket-0-0-1/data/testing_review_schema/master"

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
deltaTable = DeltaTable.forPath(spark, pathToTable)  # For path-based tables

deltaTable.optimize().executeCompaction()