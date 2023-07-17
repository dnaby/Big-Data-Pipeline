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
    for region in regions:
        # Log
        print(f"Sending request for {region['name']}...")
        url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={region['latitude']}&lon={region['longitude']}&units={units}&dt={current}&appid={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Ajouter le nom de la région aux données
            data['region_name'] = region['name']
            flattened_data = flatten_dict(data)
            all_data.append(flattened_data)
            # Log
            print(f"Request sended for {region['name']}✅")
    # Log
    print("Request sent for all regions✅")
    print("Response for all regions: ", end=" ")
    pprint(all_data)
    # Conversion des données en bytes
    value_bytes = json.dumps(all_data).encode('utf-8')
    # Log
    print("Sending data for all regions...")
    # Envoi des données sur le topic Apache Kafka
    producer.send(topic, value=value_bytes)
    print("Data sent for all regions✅")

# Charger les données des régions depuis le fichier JSON
with open('regions.json') as file:
    regions = json.load(file)
    
# Boucle principale pour exécuter la tâche planifiée pour toutes les régions
schedule.every(2).minutes.do(fetch_and_publish_weather, regions=regions)

# Boucle principale pour exécuter les tâches planifiées
while True:
    schedule.run_pending()
    time.sleep(1)
    
# Fermeture du producteur Kafka (ne sera jamais atteint dans cet exemple)
producer.close()
