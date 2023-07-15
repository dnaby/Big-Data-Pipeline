from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, element_at, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType
from pymongo import MongoClient

data_schema = StructType([
    StructField("data", ArrayType(StructType([
        StructField("clouds", IntegerType()),
        StructField("dew_point", DoubleType()),
        StructField("dt", IntegerType()),
        StructField("feels_like", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("pressure", IntegerType()),
        StructField("sunrise", IntegerType()),
        StructField("sunset", IntegerType()),
        StructField("temp", DoubleType()),
        StructField("uvi", DoubleType()),
        StructField("visibility", IntegerType()),
        StructField("weather", ArrayType(StructType([
            StructField("description", StringType()),
            StructField("icon", StringType()),
            StructField("id", IntegerType()),
            StructField("main", StringType())
        ]))),
        StructField("wind_deg", IntegerType()),
        StructField("wind_gust", DoubleType()),
        StructField("wind_speed", DoubleType())
    ]))),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("timezone", StringType()),
    StructField("timezone_offset", IntegerType())
])

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "openweathermap"
MONGODB_CONNECTION_STRING = "mongodb+srv://openweathermap:Ept2023@cluster0.fafwdse.mongodb.net?retryWrites=true&w=majority"
API_ENDPOINT = "http://your-api-endpoint"

spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()
    
df = df.selectExpr("CAST(value AS STRING)")

# Apply the schema to the JSON data
df = df.withColumn("data", from_json("value", data_schema))

# Explode the "data" array column
df = df.withColumn("exploded_data", explode("data.data"))

# Select the desired columns from the exploded data
df = df.select("exploded_data.clouds", "exploded_data.dew_point", "exploded_data.dt", "exploded_data.feels_like",
               "exploded_data.humidity", "exploded_data.pressure", "exploded_data.sunrise", "exploded_data.sunset",
               "exploded_data.temp", "exploded_data.uvi", "exploded_data.visibility",
               element_at("exploded_data.weather.description", 1).alias("description"), 
               element_at("exploded_data.weather.icon", 1).alias("icon"),
               element_at("exploded_data.weather.id", 1).alias("id"),
               element_at("exploded_data.weather.main", 1).alias("main"),
               "data.lat", "data.lon", "data.timezone", "data.timezone_offset", "data.region_name")

# TODO faire le preprocessing ici
def preprocess(data):
    df = df.withColumn("timestamp", col("dt").cast(TimestampType()))

# TODO faire une prédiction ici
def predict_temperature(data):
    # Appel à l'API pour obtenir les prédictions de température
    pass

# TODO récuperer la prédiction et écrire sur la base de données MongoDB
def process_batch(batch_df, batch_id):
    # Prétraitement sur tout le batch
    preprocessed_df = preprocess(batch_df)

    # Répartition des données par région
    regions_df = preprocessed_df.groupBy("region_name")
    regions = regions_df.count().collect()
    
    # Boucle sur chaque région
    for region in regions:
        region_name = region["region_name"]

        # Faire la prédiction sur les données de la région
        region_data = regions_df.filter(regions_df["region_name"] == region_name)
        predictions = region_data.rdd.map(predict_temperature).collect()

        # Écrire les données dans MongoDB
        client = MongoClient(MONGODB_CONNECTION_STRING)
        db = client[region_name]
        weather_collection = db["weather"]
        predictions_collection = db["predictions"]

        # Écrire les données de la région dans la collection "weather"
        #weather_collection.insert_many(region_data.toPandas().to_dict(orient="records"))

        # Écrire les prédictions dans la collection "predictions"
        #predictions_collection.insert_many(predictions)

# Appliquer une transformation pour chaque batch de données
df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
