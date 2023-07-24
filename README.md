# Big Data Pipeline

## Présentation

Ce projet contient 5 dossiers:

- un dossier elk: contenant la stack elasticsearch, logstash et kibana pour la gestion des logs depuis kafka et l'envoie d'alertes en cas de retards sur les arrivées des données;
- un dossier producer-kafka-consumer-pyspark: contenant un producer qui lit des données depuis l'API openweathermap, une broker kafka et un consumer Apache Structured Streaming qui lit les données sur Apache Kafka.
- un dossier Temperature-Forecasting: qui fournit un environnement conteneurisé de notre API fait avec FastAPI. Les informations sur le dit dossier sont accessible depuis son propre README.
- un dossier weatherapp: qui présente un peu l'interface front fait avec ReactJS et ExpressJS
- un dossier images: contenant les captures necessaires pour notre README

## Execute

Pour executer ce pipeline, il vous faudra avoir docker et docker-compse installé sur votre machine.

```bash
docker network create pipeline
bash run.sh
```

And in order to execute the pyspark stream processing. Go to the spark-master container and execute this command:

```bash
pip install --no-cache-dir -r /src/requirements.txt --no-warn-script-location

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/read_stream.py
```

Vous pouvez accéder à l'interface graphique de kibana depuis le localhost:5601.

## Producer

```python

from create_producer import producer
from pprint import pprint
import requests
import schedule
import time
import json

# Configuration
topic = 'openweathermap'

# Configuration de la connexion à l'API OpenWeatherMap
api_key = 'f51253f752e292825af2574ea0d1346c'
units = "metric"

def flatten_dict(dictionary, parent_key='', sep='_'):
    items = []
    for k, v in dictionary.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}", sep=sep).items())
                else:
                    items.append((f"{new_key}{sep}", item))
        else:
            items.append((new_key, v))
    return dict(items)

# Fonction pour récupérer les données de l'API OpenWeatherMap et les publier sur Apache Kafka
def fetch_and_publish_weather(regions: list) -> None:
    # Log
    print("Sending request for all regions...")
    # Création d'une liste pour stocker les données de toutes les régions
    all_data = []
    # Récupération des données de l'API OpenWeatherMap pour chaque région
    current = int(time.time())
    ten_minutes_ago = current - 660
    for region in regions:
        # Log
        print(f"Sending request for {region['name']}...")
        current_url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={region['latitude']}&lon={region['longitude']}&units={units}&dt={current}&appid={api_key}"
        ten_minutes_ago_url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={region['latitude']}&lon={region['longitude']}&units={units}&dt={ten_minutes_ago}&appid={api_key}"
        current_response = requests.get(current_url)
        ten_minutes_ago_response = requests.get(ten_minutes_ago_url)
        if current_response.status_code == 200 and ten_minutes_ago_response.status_code == 200:
            current_data = current_response.json()
            ten_minutes_ago_data = ten_minutes_ago_response.json()
            # Ajouter le nom de la région aux données
            current_data['region_name'] = region['name']
            ten_minutes_ago_data['region_name'] = region['name']
            flattened_current_data = flatten_dict(current_data)
            flattened_ten_minutes_ago_data = flatten_dict(ten_minutes_ago_data)
            all_data.append(flattened_current_data)
            all_data.append(flattened_ten_minutes_ago_data)
            # Log
            print(f"Request sent for {region['name']}✅")
    # Log
    print("Request sent for all regions✅")
    print("Response for all regions: ", end=" ")
    pprint(all_data)
    # Conversion des données en bytes
    value_bytes = json.dumps(all_data).encode('utf-8')
    if (len(all_data) == 26):
        # Log
        print("Sending data for all regions...")
        # Envoi des données sur le topic Apache Kafka
        producer.send(topic, value=value_bytes)
        print("Data sent for all regions✅")
    else:
        print("❌❌❌ Data not sended")
        print("❌❌❌ We have not retrieve all data")

# Charger les données des régions depuis le fichier JSON
with open('regions.json') as file:
    regions = json.load(file)
    
# Boucle principale pour exécuter la tâche planifiée pour toutes les régions
schedule.every(15).minutes.do(fetch_and_publish_weather, regions=regions)

# Boucle principale pour exécuter les tâches planifiées
while True:
    schedule.run_pending()
    time.sleep(1)
    
# Fermeture du producteur Kafka (ne sera jamais atteint dans cet exemple)
producer.close()


```

## Consumer

```python

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
    col("exploded.timezone").alias("timezone"),
    col("exploded.data_weather_icon").alias("icon")
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
    db = client["openweathermap"]
    weather_collection = db["weather"]
    predictions_collection = db["predictions"]
    
    # Appel à l'API pour obtenir les prédictions de température
    regions_df = batch_df.groupBy("region")
    regions = regions_df.count().collect()
    
    # Boucle sur chaque région
    for region in regions:
        region_name = region["region"]
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
                # set region
                prediction["region"] = region_name
                # Check if there is an existing prediction with the same timestamp
                existing_predictions = predictions_collection.count_documents({"region": region_name, "timestamp": prediction["timestamp"]})
                if existing_predictions > 0:
                    # Update the existing prediction with the new temperature value
                    predictions_collection.update_many({"region": region_name, "timestamp": prediction["timestamp"]}, {"$set": {"temperature": prediction["temperature"]}})
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

    
```

## Interface Kibana

![Dashboard Kibana](./images/Capture%20d%E2%80%99%C3%A9cran%202023-07-22%20%C3%A0%201.12.14%20AM.png)

## Alertes sur Slack

![Alets](./images/Capture%20d%E2%80%99%C3%A9cran%202023-07-22%20%C3%A0%201.14.21%20AM.png)

## Contributing

Your contributions are welcome.

## School

[EPT](https://ept.sn)
