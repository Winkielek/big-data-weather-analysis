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


weather_df = spark.read.parquet("hdfs://localhost:8020/user/projekt/tmp/weather")
weather_df

aqi_df = spark.read.parquet("hdfs://localhost:8020/user/projekt/tmp/aqi")
aqi_df

joined_df = weather_df.join(aqi_df, on = ["longitude", "latitude"], how = "inner")
joined_df_pd = joined_df.toPandas()


import plotly.express as px
px.set_mapbox_access_token("pk.eyJ1Ijoid2lua2llbCIsImEiOiJja3lsemg0dmoyZ2piMm9xcGxsdWt3OG9qIn0.8xnKnGAxWMabNbRTL0cQcw")
df = px.data.carshare()
fig = px.scatter_mapbox(joined_df_pd, lat="latitude", lon="longitude", color="city_name", size="aqi",
                  color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=10)

params = (
    ('op', 'CREATE'),
    ('user.name', 'testuser'),
    ('namenoderpcaddress', 'node1:8020'),
    ('overwrite', 'true'),
)

response = requests.put('http://node1:50075/webhdfs/v1/user/projekt/realtime_analysis/map/map.html', params=params, data = fig.to_html())



