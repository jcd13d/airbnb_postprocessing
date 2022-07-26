import pyspark.sql.utils
import s3fs
from delta import *
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

    def clean_directories(self):
        pass

    def run(self):
        self.read_data()
        if self.data is None:
            return False
        else:
            self.apply_partition_col()
            self.postprocess_logic()
            self.write_data()
            self.clean_directories()


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
    def __init__(self, data_loc, out_path, partition=[], num_partitions=20, write_type="parquet"):
        super(PysparkPostProcessor, self).__init__(data_loc, out_path, partition, num_partitions)
        self.write_type = write_type
        self.dirs = recursive_list_dir(self.s3, [self.data_loc], 2)
        # self.spark = SparkSession.builder.appName("processor").getOrCreate()
        builder = pyspark.sql.SparkSession.builder.appName("Post Processor") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def apply_partition_col(self):
        self.data = self.data.withColumn("parition_col", F.col("id") % self.num_partitions)

    def postprocess_logic(self):
        self.data.show()
        print("\n\n\n", self.data.count(), "\n\n\n")
        pass

    def read_data(self):
        print(self.dirs)
        if len(self.dirs) != 0:
            self.data = self.spark.read.parquet(*self.dirs)
        else:
            self.data = None

    def write_data(self):
        print("\n\n\n\n", "WRITINGGGGGGG", "\n\n\n\n")
        print(self.out_path)
        if self.write_type == "parquet":
            if self.s3.exists(self.out_path):
                self.data.write.partitionBy("parition_col").mode("append").parquet(self.out_path)
            else:
                self.data.write.partitionBy("parition_col").mode("overwrite").parquet(self.out_path)
        elif self.write_type == "delta":
            self.data.write.partitionBy("parition_col").format("delta").mode("append").save(self.out_path)

    def clean_directories(self):
        [self.s3.rm(p, recursive=True) for p in self.dirs]
        pass

