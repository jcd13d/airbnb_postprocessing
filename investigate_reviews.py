import pyspark
from delta import *
import s3fs
import os
import json


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
    s3 = s3fs.S3FileSystem()
    data_loc = "s3://airbnb-scraper-bucket-0-0-1/data/review_beta_20220721/"
    out_loc = "s3://airbnb-scraper-bucket-0-0-1/data/testing_review_schema/"
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    dirs = recursive_list_dir(s3, [data_loc], 2)
    for i, dir in enumerate(dirs):
        df = spark.read.parquet(dir)
        try:
            df.write.format("delta").mode('append').save(os.path.join(out_loc, "master"))
        except pyspark.sql.utils.AnalysisException as e:
            df.write.mode("overwrite").parquet(os.path.join(out_loc, f"fail_{i}"))
            with s3.open(os.path.join(out_loc, f"fail_{i}", f"fail_note.json"), "w") as f:
                json.dump({"error": e.__str__()}, f, indent=4)
            print(e)


