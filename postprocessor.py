import sys

import s3fs
import fastparquet as fp
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class PostProcessor:
    def __init__(self, num_partitions, data_loc, out_path, partition=[]):
        self.data = None
        self.data_loc = data_loc
        self.partition = partition
        self.out_path = out_path
        self.s3 = s3fs.S3FileSystem()
        self.num_partitions = num_partitions

    def apply_partition_col(self):
        self.data['partition_col'] = self.data['id'] % self.num_partitions
        pass

    def postprocess_logic(self):
        pass

    def read_data(self):
        self.data = fp.ParquetFile(self.data_loc, open_with=self.s3.open).to_pandas()

    def write_data(self):
        if self.s3.exists(self.out_path):
            fp.write(self.out_path, self.data, file_scheme='hive', partition_on=self.partition, append=True, open_with=self.s3.open)
        else:
            fp.write(self.out_path, self.data, file_scheme='hive', partition_on=self.partition, append=False, open_with=self.s3.open)

    def run(self):
        self.read_data()
        self.apply_partition_col()
        self.postprocess_logic()
        self.write_data()


class PostProcessorSpark(PostProcessor):
    def __init__(self, num_partitions, data_loc, out_path, partition=[]):
        super(PostProcessorSpark, self).__init__(num_partitions, data_loc, out_path, partition)

    def apply_partition_col(self):
        self.data['partition_col'] = self.data['id'] % self.num_partitions
        pass

    def postprocess_logic(self):
        pass

    def read_data(self):
        self.data = fp.ParquetFile(self.data_loc, open_with=self.s3.open).to_pandas()

    def write_data(self):
        if self.s3.exists(self.out_path):
            fp.write(self.out_path, self.data, file_scheme='hive', partition_on=self.partition, append=True, open_with=self.s3.open)
        else:
            fp.write(self.out_path, self.data, file_scheme='hive', partition_on=self.partition, append=False, open_with=self.s3.open)

    def run(self):
        self.read_data()
        self.apply_partition_col()
        self.postprocess_logic()
        self.write_data()


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage  : spark_s3_integration.py <AWS_ACCESS_KEY_ID> <AWS_SECRET_ACCESS_KEY>", file=sys.stderr)
        print("Example: spark_s3_integration.py ranga_aws_access_key ranga_aws_secret_key", file=sys.stderr)
        exit(-1)

    awsAccessKey = sys.argv[1]
    awsSecretKey = sys.argv[2]

    conf = (
        SparkConf()
            .setAppName("PySpark S3 Integration Example")
            .set("spark.hadoop.fs.s3a.access.key", awsAccessKey)
            .set("spark.hadoop.fs.s3a.secret.key", awsSecretKey)
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .set("spark.speculation", "false")
            .set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
            .set("fs.s3a.experimental.input.fadvise", "random")
            .setIfMissing("spark.master", "local")
    )

    # Creating the SparkSession object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("SparkSession Created successfully")

    data_loc = "s3a://jd-s3-test-bucket9/data/occupancy_beta/array_1/"
    out_loc = "s3://jd-s3-test-bucket9/data/test_spark_out"

    df = spark.read.parquet(data_loc)

    df.printSchema()


