from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import s3fs


def recursive_list_dir(fs, dirs, levels=1, prepend="s3://"):
    if levels == 0:
        return dirs
    else:
        new_dirs = []
        for d in dirs:
            new_dirs = new_dirs + fs.ls(d)
        new_dirs = [prepend + x for x in new_dirs]
        return recursive_list_dir(fs, new_dirs, levels-1)


if __name__ == "__main__":

    partitions = 20

    data_loc = "s3://jd-s3-test-bucket9/data/occupancy_beta_2/"
    out_loc = "s3://jd-s3-test-bucket9/data/test_spark_out_3"

    fs = s3fs.S3FileSystem()

    dirs = recursive_list_dir(fs, [data_loc], 2)

    # Creating the SparkSession object
    spark = SparkSession.builder.appName("processor").getOrCreate()
    print("SparkSession Created successfully")

    df = spark.read.parquet(*dirs)

    df.printSchema()

    df = df.withColumn("parition_col", F.col("id") % partitions)

    df.show()

    print("\n\n\n", df.count(), "\n\n\n")

    df.write.partitionBy("parition_col").mode("append").parquet(out_loc)

    check = spark.read.parquet(out_loc)
    check.show()

    print("out loc count")
    print("\n\n\n", check.count(), "\n\n\n")
    # check.write.partitionBy("parition_col").mode("overwrite").parquet(out_loc)



