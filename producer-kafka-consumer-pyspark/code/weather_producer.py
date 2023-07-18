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
    if (len(all_data) == 24):
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
