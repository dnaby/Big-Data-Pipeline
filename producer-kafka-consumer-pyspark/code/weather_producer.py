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
lang = 'fr'
units = "metric"

# Fonction pour récupérer les données de l'API OpenWeatherMap et les publier sur Apache Kafka
def fetch_and_publish_weather(region: dict) -> None:
    # Log
    print(f"Sending request for {region['name']}...")
    # Récupération des données de l'API OpenWeatherMap
    current = int(time.time())
    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={region['latitude']}&lon={region['longitude']}&lang={lang}&units={units}&dt={current}&appid={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Ajouter le nom de la région aux données
        data['region_name'] = region['name']
        # Log
        print(f"Request sended for {region['name']}✅")
        print(f"Response for {region['name']}: ", end=" ")
        pprint(data)
        # Conversion des données en bytes
        value_bytes = json.dumps(data).encode('utf-8')
        # Log
        print(f"Sending data for {region['name']}...")
        # Envoi des données sur le topic Apache Kafka
        producer.send(topic, value=value_bytes)
        print(f"Data sended for {region['name']}✅")

# Charger les données des régions depuis le fichier JSON
with open('regions.json') as file:
    regions = json.load(file)
    
# Boucle principale pour exécuter les tâches planifiées pour chaque région
while True:
    for region in regions:
        schedule.every(5).minutes.do(fetch_and_publish_weather, region=region)
    schedule.run_pending()
    time.sleep(1)
    
# Fermeture du producteur Kafka (ne sera jamais atteint dans cet exemple)
producer.close()
