import os
import csv
import json
import time
import shutil
from dotenv import load_dotenv
from confluent_kafka import Producer

import re

# Configuration
load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9094')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'topic_airbnb')
INPUT_DIR = 'raw_data'
ARCHIVE_DIR = 'archive'
SEND_DELAY = 0.2  # Delai entre chaque message (0.3s = ~1000 lignes en 5 min)

# Initialisation du Producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    if err:
        print(f"Echec : {err}")

def natural_sort_key(filename):
    """Extrait le numero du fichier pour un tri naturel (1, 2, ..., 10, 11, 12)."""
    numbers = re.findall(r'\d+', filename)
    return int(numbers[-1]) if numbers else 0

def process_files():
    """Lit les CSV du dossier raw_data et publie chaque ligne dans Kafka."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    files = sorted(
        [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')],
        key=natural_sort_key
    )
    if not files:
        print("Aucun fichier en attente.")
        return

    for file_name in files:
        file_path = os.path.join(INPUT_DIR, file_name)
        print(f"\n--- Traitement : {file_name} ---")

        # Lecture via csv.DictReader (plus rapide que pandas.iterrows)
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # Conversion des types numeriques (csv lit tout en str)
                payload = {k: _cast(v) for k, v in row.items()}

                producer.produce(
                    topic=TOPIC_NAME,
                    value=json.dumps(payload).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                count += 1

                # Affichage de progression toutes les 100 lignes
                if count % 100 == 0:
                    print(f"  {count} messages envoyes...")

                time.sleep(SEND_DELAY)

        producer.flush()

        # Archivage du fichier traite
        shutil.move(file_path, os.path.join(ARCHIVE_DIR, file_name))
        print(f"  Termine : {count} messages. Fichier archive.")

def _cast(value):
    """Convertit les valeurs texte du CSV en types Python natifs."""
    if value == '' or value is None:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value

if __name__ == "__main__":
    try:
        while True:
            process_files()
            print("\nEn attente de nouveaux fichiers (10s)...")
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nArret du producer.")
