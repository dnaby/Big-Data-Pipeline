
from pprint import pprint
import requests
import schedule
import time


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

# Planification de la tâche toutes les 15 minutes
schedule.every(15).minutes.do(fetch_and_publish_weather)

# Boucle principale pour exécuter les tâches planifiées
while True:
    schedule.run_pending()
    time.sleep(1)
