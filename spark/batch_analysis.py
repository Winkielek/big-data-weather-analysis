import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, HiveContext
import requests


spark = (SparkSession
    .builder
    .appName("Python Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate())

weather_df = spark.read.parquet("hdfs://localhost:8020/user/projekt/raw/weather")
weather_df

aqi_df = spark.read.parquet("hdfs://localhost:8020/user/projekt/raw/air_pollution")
aqi_df

joined_df = weather_df.join(aqi_df, on = ["longitude", "latitude"], how = "inner")

columns_to_aggregate = ["feelslike_temperature", "temperature", "wind_speed",
                        "cloud", "humidity", "pressure_mb", "precipitation_mm", "visibility_km"]

df_1 = weather_df.groupBy(["country", "city_name", "longitude", "latitude"]).agg(
    *[F.max(F.col(x)).alias("max_" + x) for x in columns_to_aggregate],
    *[F.min(F.col(x)).alias("min_" + x) for x in columns_to_aggregate],
    *[F.avg(F.col(x)).alias("avg_" + x) for x in columns_to_aggregate],
)

df_1_pd = df_1.toPandas()
df_1.coalesce(1).write.mode("overwrite").json("hdfs://localhost:8020/user/projekt/batch_analysis/weather_aggregated")



columns_to_aggregate = ["aqi", "co", "no",
                        "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
df_2 = joined_df.groupBy(["country", "city_name"]).agg(
    *[F.max(F.col(x)).alias("max_" + x) for x in columns_to_aggregate],
    *[F.min(F.col(x)).alias("min_" + x) for x in columns_to_aggregate],
    *[F.avg(F.col(x)).alias("avg_" + x) for x in columns_to_aggregate],
)

df_2_pd = df_2.toPandas()
df_2.coalesce(1).write.mode("overwrite").json("hdfs://localhost:8020/user/projekt/batch_analysis/aqi_aggregated")


df_3 = df_2.join(df_1, on = ["city_name", "country"], how = 'inner')
df_3.coalesce(1).write.mode("overwrite").json("hdfs://localhost:8020/user/projekt/batch_analysis/map")
