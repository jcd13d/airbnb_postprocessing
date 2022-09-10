from delta import *
import pyspark
import pyspark.sql.functions as F
import s3fs
import json


class GetUniqueIds:
    def __init__(self, listings_loc, ids_output_loc):
        print(listings_loc)
        self.s3 = s3fs.S3FileSystem()
        builder = pyspark.sql.SparkSession.builder.appName("Post Processor") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.df = self.spark.read.parquet(listings_loc)
        self.ids_output_loc = ids_output_loc
        self.ids = None

    def get_unique_ids(self):
        newest_pull = self.df.agg({"postprocess_date": "max"}).collect()[0]["max(postprocess_date)"]
        # self.df.where(F.col("process_date") == newest_pull).show()
        df = self.df.where(F.col("process_date") == newest_pull).select("id").distinct().toPandas()
        self.ids = list(df['id'].values)
        print(self.ids)

    def write_ids(self):
        # with self.s3.open(self.ids_output_loc, "w") as f:
        #     json.dump({"ids": self.ids}, f, indent=4)
        pass

    def run(self):
        self.get_unique_ids()
        self.write_ids()


if __name__ == "__main__":
    config = {
        # "listings_loc": "s3://airbnb-scraper-bucket-0-1-1/data/listings_postprocessed/",
        "listings_loc": "s3://airbnb-scraper-bucket-0-1-1/data/prod_data_tables/listings/20220831_postprocessed",
        "ids_output_loc": "s3://airbnb-scraper-bucket-0-1-1/master_configs/ids_dynamic/ids.json"
    }
    print(config)
    app = GetUniqueIds(**config)
    app.run()
