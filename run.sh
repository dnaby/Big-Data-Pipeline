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

# Déplacer vers le dossier api
cd ./api

# Exécuter docker-compose dans le dossier api
docker-compose up -d
