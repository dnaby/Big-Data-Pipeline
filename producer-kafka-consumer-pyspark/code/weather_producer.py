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
def fetch_and_publish_weather() -> None:
    # Log
    print("Sending request...")
    # Récupération des données de l'API OpenWeatherMap
    current = int(time.time())
    url = f'https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={latitude}&lon={longitude}&lang={lang}&dt={current}&appid={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
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
