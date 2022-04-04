from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


if __name__ == "__main__":
    if __name__ == "__main__":

        # Creating the SparkSession object
        spark = SparkSession.builder.appName("processor").getOrCreate()
        print("SparkSession Created successfully")

        # EMR likes this dir but not the datetime ones
        data_loc = "s3://jd-s3-test-bucket9/data/occupancy_beta_full/"
        out_loc = "s3://jd-s3-test-bucket9/data/test_spark_out"

        df = spark.read.parquet(data_loc)

        df.printSchema()

        df.show()
