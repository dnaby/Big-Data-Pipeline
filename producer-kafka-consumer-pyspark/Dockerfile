# Use a Python base image
FROM python:3.10-slim

WORKDIR /app

# Copie des fichiers de l'application
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt