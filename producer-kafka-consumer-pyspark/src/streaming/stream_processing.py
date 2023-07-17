from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType
from pymongo import MongoClient

data_schema = ArrayType(StructType([
    StructField('data_clouds', IntegerType()),
    StructField('data_dew_point', DoubleType()),
    StructField('data_dt', IntegerType()),
    StructField('data_feels_like', DoubleType()),
    StructField('data_humidity', IntegerType()),
    StructField('data_pressure', IntegerType()),
    StructField('data_sunrise', IntegerType()),
    StructField('data_sunset', IntegerType()),
    StructField('data_temp', DoubleType()),
    StructField('data_uvi', DoubleType()),
    StructField('data_visibility', IntegerType()),
    StructField('data_weather_description', StringType()),
    StructField('data_weather_icon', StringType()),
    StructField('data_weather_id', IntegerType()),
    StructField('data_weather_main', StringType()),
    StructField('data_wind_deg', IntegerType()),
    StructField('data_wind_gust', DoubleType()),
    StructField('data_wind_speed', DoubleType()),
    StructField('lat', DoubleType()),
    StructField('lon', DoubleType()),
    StructField('region_name', StringType()),
    StructField('timezone', StringType()),
    StructField('timezone_offset', IntegerType())
]))

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "openweathermap"
MONGODB_CONNECTION_STRING = "mongodb+srv://openweathermap:Ept2023@cluster0.fafwdse.mongodb.net?retryWrites=true&w=majority"
API_ENDPOINT = "http://api:9200/predict"

spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()
    
df = df.selectExpr("CAST(value AS STRING)")

# Convertir la colonne "value" en JSON en utilisant le schéma défini
df = df.withColumn("data", from_json("value", data_schema))
df = df.withColumn("exploded", explode("data"))

df = df.select(
    col("exploded.data_dew_point").alias("dew_point"),
    col("exploded.data_dt").alias("dt"),
    col("exploded.data_feels_like").alias("feels_like"),
    col("exploded.data_humidity").alias("humidity"),
    col("exploded.data_pressure").alias("pressure"),
    col("exploded.data_sunrise").alias("sunrise"),
    col("exploded.data_sunset").alias("sunset"),
    col("exploded.data_temp").alias("temp"),
    col("exploded.data_uvi").alias("uvi"),
    col("exploded.data_visibility").alias("visibility"),
    col("exploded.data_weather_description").alias("description"),
    col("exploded.data_weather_main").alias("main"),
    col("exploded.data_wind_deg").alias("wind_deg"),
    col("exploded.data_wind_gust").alias("wind_gust"),
    col("exploded.data_wind_speed").alias("wind_speed"),
    col("exploded.lat").alias("lat"),
    col("exploded.lon").alias("lon"),
    col("exploded.region_name").alias("region"),
    col("exploded.timezone").alias("timezone")
)

# TODO faire le preprocessing ici
df = df.withColumn("timestamp", col("dt").cast(TimestampType()))
df = df.withColumn("sunrise", col("sunrise").cast(TimestampType()))
df = df.withColumn("sunset", col("sunset").cast(TimestampType()))
df = df.drop("dt")

# TODO faire une prédiction ici
def predict_temperature(data):
    # Appel à l'API pour obtenir les prédictions de température
    pass

# TODO récuperer la prédiction et écrire sur la base de données MongoDB
def process_batch(batch_df, batch_id):
    # Faire les predictions et enregistrer les données dans la base
    pass

# Appliquer une transformation pour chaque batch de données
df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
