from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType
from pymongo import MongoClient
import json
import requests
from pprint import pprint

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
MONGODB_CONNECTION_STRING = "mongodb+srv://openweathermap:Ept2023@cluster0.fafwdse.mongodb.net/test?retryWrites=true&w=majority"
API_ENDPOINT = "http://api:8000/predict/"

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

# TODO récuperer la prédiction et écrire sur la base de données MongoDB
def process_batch(batch_df, batch_id):
    # Écrire les données dans MongoDB
    client = MongoClient(MONGODB_CONNECTION_STRING)
    
    # Appel à l'API pour obtenir les prédictions de température
    regions_df = batch_df.groupBy("region")
    regions = regions_df.count().collect()
    
    # Boucle sur chaque région
    for region in regions:
        region_name = region["region"]
        # BD
        db = client[region_name.replace('/', '_')]
        weather_collection = db["weather"]
        predictions_collection = db["predictions"]
        # Faire la prédiction sur les données de la région
        region_data = batch_df.filter(batch_df["region"] == region_name)
        # Récupérer lat et lon pour la région
        lat = region_data.select("lat").first()[0]
        lon = region_data.select("lon").first()[0]
        # Récupérer les colonnes ds et temp
        data_to_send = region_data.select(col("timestamp").alias("ds").cast(StringType()), col("temp").alias("y")).toPandas()
        # Convert DataFrame to a list of dictionaries
        data_to_send = data_to_send.to_dict(orient="records")
        # créer le payload
        payload = {
            "latitude": float(lat),
            "longitude": float(lon),
            "data": data_to_send
        }
        pprint(payload)
        #payload = json.dumps(payload)
        # Make the POST request
        response = requests.get(API_ENDPOINT, json=payload)
        # Check the response status code
        if response.status_code == 200:
            # Successful response
            data_response = response.json()
            predictions = data_response["predictions"]
            for prediction in predictions:
                # Check if there is an existing prediction with the same timestamp
                existing_predictions = predictions_collection.count_documents({"timestamp": prediction["timestamp"]})
                if existing_predictions > 0:
                    # Update the existing prediction with the new temperature value
                    predictions_collection.update_many({"timestamp": prediction["timestamp"]}, {"$set": {"temperature": prediction["temperature"]}})
                else:
                    # Insert the new prediction into the collection
                    predictions_collection.insert_one(prediction)
            print("Temperature Predictions: ", end="")
            pprint(predictions)
        else:
            # Error response
            pprint(response.text)
            
        weather_data = region_data.withColumn("timestamp", col("timestamp").cast(StringType()))
        weather_data = weather_data.withColumn("sunrise", col("sunrise").cast(StringType()))
        weather_data = weather_data.withColumn("sunset", col("sunset").cast(StringType()))
            
        # Écrire les données de la région dans la collection "weather"
        weather_collection.insert_many(weather_data.toPandas().to_dict(orient="records"))

# Appliquer une transformation pour chaque batch de données
df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
