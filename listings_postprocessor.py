from postprocessor.postprocessor import PysparkPostProcessor
from  pyspark.sql.functions import input_file_name
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime


class ListingsPostprocessor(PysparkPostProcessor):
    def __init__(self, data_loc, out_path, partition=[], num_partitions=20, write_type="parquet", dir_level=2, schema=None):
        super(ListingsPostprocessor, self).__init__(data_loc, out_path, partition, num_partitions, write_type, dir_level, schema)
        self.run_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    def postprocess_logic(self):
        # self.data = self.data.withColumn("filename", ))
        self.data = self.data.withColumn("config_name", F.split(input_file_name(), "/").getItem(5))
        # ex. 20220831083950 with spaces 2022 08 31 08 39 50
        date_format = "yyyyMMddkkmmss"
        self.data = self.data.withColumn("postprocess_date", F.lit(self.run_time))
        self.to_datetime_wrapper("postprocess_date", date_format)

        self.type_conversion("id", T.LongType())
        self.type_conversion("parition_col", T.IntegerType())
        self.type_conversion("avgRating", T.FloatType())
        self.type_conversion("avgRating", T.FloatType())
        # TODO add more if needed?

        self.data.show()

    def write_data(self):
        pass

    def clean_directories(self):
        pass


if __name__ == "__main__":
    config = {}

    config["index_postprocess"] = {
        "data_loc": "s3://airbnb-scraper-bucket-0-1-1/data/listings/",
        "out_path": "s3://airbnb-scraper-bucket-0-1-1/data/prod_data_tables/listings/20220831_postprocessed",
        "schema": [
            "index",
            "id",
            "name",
            "price",
            "displayPrice",
            "monthlyPriceFactor",
            "weeklyPriceFactor",
            "avgRating",
            "reviewsCount",
            "isNewListing",
            "isSuperhost",
            "lat",
            "lng",
            "personCapacity",
            "size",
            "beds",
            "baths",
            "guest_num",
            "property_type",
            "town",
            "property_category",
            "town_state",
            "top_lat",
            "right_long",
            "bottom_lat",
            "left_long",
            "url",
            "zipcode",
            "road",
            "city",
            "county",
            "state",
            "country",
            "parition_col",
            "config_name",
            "postprocess_date"
        ]
    }

    pp = ListingsPostprocessor(**config['index_postprocess'])

    pp.run()



