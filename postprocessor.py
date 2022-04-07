import sys

import s3fs
import fastparquet as fp
# from pyspark.sql import SparkSession
# from pyspark.conf import SparkConf


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


# class PostProcessorSpark(PostProcessor):
#     def __init__(self, num_partitions, data_loc, out_path, partition=[]):
#         super(PostProcessorSpark, self).__init__(num_partitions, data_loc, out_path, partition)
#
#     def apply_partition_col(self):
#         self.data['partition_col'] = self.data['id'] % self.num_partitions
#         pass
#
#     def postprocess_logic(self):
#         pass
#
#     def read_data(self):
#         self.data = fp.ParquetFile(self.data_loc, open_with=self.s3.open).to_pandas()
#
#     def write_data(self):
#         if self.s3.exists(self.out_path):
#             fp.write(self.out_path, self.data, file_scheme='hive', partition_on=self.partition, append=True, open_with=self.s3.open)
#         else:
#             fp.write(self.out_path, self.data, file_scheme='hive', partition_on=self.partition, append=False, open_with=self.s3.open)
#
#     def run(self):
#         self.read_data()
#         self.apply_partition_col()
#         self.postprocess_logic()
#         self.write_data()
#
#
# if __name__ == "__main__":
#
#     # Creating the SparkSession object
#     spark = SparkSession.builder.appName("processor").getOrCreate()
#     print("SparkSession Created successfully")
#
#     # EMR likes this dir but not the datetime ones
#     data_loc = "s3://jd-s3-test-bucket9/data/occupancy_beta_full/"
#     out_loc = "s3://jd-s3-test-bucket9/data/test_spark_out"
#
#     df = spark.read.parquet(data_loc)
#
#     df.printSchema()
#
#     df.show()


