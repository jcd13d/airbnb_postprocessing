import pyspark
from delta import *
import pyspark.sql.functions as F


if __name__ == "__main__":
    read_file = "s3://airbnb-scraper-bucket-0-0-1/data/review_beta_20220721/array_0/20220721201641/"
    write_file_delta = "s3://airbnb-scraper-bucket-0-0-1/data/testing_delta/test_1"
    write_file_parqet = "s3://airbnb-scraper-bucket-0-0-1/data/testing_delta/test_parquet"

    out_dict = {}


    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # df = pd.read_parquet(read_file)

    df = spark.read.parquet(read_file)
    print("first show")
    df.show()
    df.write.format("delta").mode("overwrite").save(write_file_delta)
    # df.write.format("delta").mode("append").save(write_file_delta)
    # df.write.mode("overwrite").parquet(write_file_parqet)

    df = spark.read.format("delta").load(write_file_delta)
    df.count()
    df.show()

    df.printSchema()
    df.withColumn("new", F.lit("test")).write.format("delta").mode("append").save(write_file_delta)

    cols = ["index", "date_of_review", "review", "pulled", "id"]
    df.select(*cols).withColumn("rating", F.lit("test")).write.format("delta").mode("append").save(write_file_delta)

    # df.write.format("delta").mode("append").save(write_file_delta)
    # df = spark.read.format("delta").load(write_file_delta)
    # print("#############################\n\n\n\n\n\n\n\n\n\n\n")
    # print(df.count())
    # print("#############################\n\n\n\n\n\n\n\n\n\n\n")
    # df = spark.read.format("delta").load(write_file_delta)
    # print("#############################\n\n\n\n\n\n\n\n\n\n\n")
    # print(df.count())
    # print("#############################\n\n\n\n\n\n\n\n\n\n\n")
    #
    # print("testing append parquet")
    # df.write.mode("append").parquet(write_file_parqet)
    # df = spark.read.parquet(write_file_parqet)
    # print("#############################\n\n\n\n\n\n\n\n\n\n\n")
    # print(df.count())
    # df.printSchema()
    # print("#############################\n\n\n\n\n\n\n\n\n\n\n")
    #

