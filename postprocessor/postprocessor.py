import s3fs
import fastparquet as fp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def recursive_list_dir(fs, dirs, levels=1, prepend="s3://"):
    if levels == 0:
        return dirs
    else:
        new_dirs = []
        for d in dirs:
            new_dirs = new_dirs + fs.ls(d)
        new_dirs = [prepend + x for x in new_dirs]
        return recursive_list_dir(fs, new_dirs, levels-1)


class PostProcessor:
    def __init__(self, data_loc, out_path, partition=[], num_partitions=20):
        self.data = None
        self.data_loc = data_loc
        self.partition = partition
        self.out_path = out_path
        self.s3 = s3fs.S3FileSystem()
        self.num_partitions = num_partitions

    def apply_partition_col(self):
        pass

    def postprocess_logic(self):
        pass

    def read_data(self):
        raise NotImplemented("Must implement read method")

    def write_data(self):
        raise NotImplemented("Must implement write method")

    def run(self):
        self.read_data()
        self.apply_partition_col()
        self.postprocess_logic()
        self.write_data()


class PandasPostProcessor(PostProcessor):
    def __init__(self, data_loc, out_path, partition=[], num_partitions=20):
       super(PandasPostProcessor, self).__init__(data_loc, out_path, partition, num_partitions)

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


class PysparkPostProcessor(PostProcessor):
    def __init__(self, data_loc, out_path, partition=[], num_partitions=20):
        super(PysparkPostProcessor, self).__init__(data_loc, out_path, partition, num_partitions)
        self.dirs = recursive_list_dir(self.s3, [self.data_loc], 2)
        self.spark = SparkSession.builder.appName("processor").getOrCreate()

    def apply_partition_col(self):
        self.data = self.data.withColumn("parition_col", F.col("id") % self.num_partitions)

    def postprocess_logic(self):
        pass

    def read_data(self):
        self.data = self.spark.read.parquet(*self.dirs)

    def write_data(self):
        self.data.write.partitionBy("parition_col").mode("append").parquet(self.out_loc)

