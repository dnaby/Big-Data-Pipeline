# Big Data Pipeline

## Présentation

Ce projet contient deux dossiers:

- un dossier elk: contenant la stack elasticsearch, logstash et kibana pour la gestion des logs depuis kafka et l'envoie d'alertes en cas de retards sur les arrivées des données;
- un dossier producer-kafka-consumer-pyspark: contenant un producer qui lit des données depuis l'API openweathermap, une broker kafka et un consumer Apache Structured Streaming qui lit les données sur apache kafka.

## Execute

Pour executer ce pipeline, il vous faudra avoir docker et docker-compse installé sur votre machine.

```bash
cd pipeline
docker network create pipeline
bash run.sh
```

And in order to execute the pyspark stream processing. Go to the spark-master container and execute this commande:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/read_stream.py
```

Vous pouvez accéder à l'interface graphique de kibana depuis le localhost:5601.

## Producer

```python

from kafka import KafkaProducer
from create_producer import producer
from pprint import pprint
import requests
import schedule
import time
import json

# Configuration
topic = 'openweathermap'

# Configuration de la connexion à l'API OpenWeatherMap
api_key = '1faffa257e9d684c74c4580c700c64da'
latitude = '14.693425'
longitude = '-17.447938'
lang = 'fr'

# Fonction pour récupérer les données de l'API OpenWeatherMap et les publier sur Apache Kafka
def fetch_and_publish_weather():
    # Log
    print("Sending request...")
    # Récupération des données de l'API OpenWeatherMap
    url = f'https://api.openweathermap.org/data/3.0/onecall?lat={latitude}&lon={longitude}&lang={lang}&appid={api_key}'
    response = requests.get(url)
    data = response.json()
    
    # Log
    print("Request sended✅")
    print("Response: ", end=" ")
    pprint(data)
    
    # Conversion des données en bytes
    value_bytes = json.dumps(data).encode('utf-8')

    # Log
    print("Sending data...")
    # Envoi des données sur le topic Apache Kafka
    producer.send(topic, value=value_bytes)
    print("Data sended✅")

# Planification de la tâche toutes les 15 minutes
schedule.every(1).minutes.do(fetch_and_publish_weather)

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

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "openweathermap"

spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
    
```

## Contributing

Your contributions are welcome.

## School

[EPT](https://ept.sn)
