from kafka import KafkaProducer


# Log
print("Creating producer...")

# Configuration
bootstrap_servers = 'kafka:9092'
topic = 'openweathermap'

# Configuration de la connexion à l'API OpenWeatherMap
api_key = '1faffa257e9d684c74c4580c700c64da'
latitude = '14.693425'
longitude = '-17.447938'
lang = 'fr'

# Création du producteur Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

print("Producer created✅")