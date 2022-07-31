from postprocessor.postprocessor import PysparkPostProcessor
from  pyspark.sql.functions import input_file_name
import pyspark.sql.functions as F


class ListingsPostprocessor(PysparkPostProcessor):
    def __init__(self, data_loc, out_path, partition=[], num_partitions=20, write_type="parquet"):
        super(ListingsPostprocessor, self).__init__(data_loc, out_path, partition, num_partitions, write_type)

    def postprocess_logic(self):
        # self.data = self.data.withColumn("filename", ))
        self.data = self.data.withColumn("config_name", F.split(input_file_name(), "/").getItem(5))

        self.data.show()

    # def write_data(self):
    #     pass

    # def clean_directories(self):
    #     pass


if __name__ == "__main__":
    config = {}

    config["index_postprocess"] = {
        "data_loc": "s3://airbnb-scraper-bucket-0-1-1/data/listings/",
        "out_path": "s3://airbnb-scraper-bucket-0-1-1/data/listings_postprocessed/"
    }

    pp = ListingsPostprocessor(**config['index_postprocess'])

    pp.run()



