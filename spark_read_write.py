from pyspark.sql import SparkSession
from pyspark.sql import functions as F

AppName = "test_app"

spark = SparkSession.builder.appName(AppName).getOrCreate()