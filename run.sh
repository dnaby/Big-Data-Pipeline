#!/bin/bash
# Déplacer vers le dossier elk
cd ./elk

# Exécuter docker-compose dans le dossier elk
docker-compose up -d

# Revenir au dossier parent
cd ..

# Déplacer vers le dossier producer-kafka-consumer-pyspark
cd ./producer-kafka-consumer-pyspark

# Exécuter docker-compose dans le dossier producer-kafka-consumer-pyspark
docker-compose up -d

# Revenir au dossier parent
cd ..

# Déplacer vers le dossier Temperature-Forecasting
cd ./Temperature-Forecasting

# Exécuter docker-compose dans le dossier Temperature-Forecasting
docker-compose up -d
