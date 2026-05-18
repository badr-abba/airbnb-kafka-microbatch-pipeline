import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer
from airbnb_pipeline import clean_airbnb_batch

# Configuration
load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9094')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'topic_airbnb')
OUTPUT_DIR = 'data_lake/airbnb_clean'
BATCH_SIZE = 500

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialisation du Consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'airbnb-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe([TOPIC_NAME])

def save_batch(df):
    """Sauvegarde un DataFrame nettoye au format JSON Lines."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUTPUT_DIR, f"batch_airbnb_{ts}.jsonl")
    df.to_json(path, orient='records', lines=True, date_format='iso')
    print(f"  Batch sauvegarde : {path} ({len(df)} lignes)")

def process_batch(buffer):
    """Convertit le buffer en DataFrame, nettoie et sauvegarde."""
    df_raw = pd.DataFrame(buffer)
    df_clean = clean_airbnb_batch(df_raw)
    print(f"  Nettoyage : {len(df_raw)} lignes brutes -> {len(df_clean)} lignes propres")
    save_batch(df_clean)
    consumer.commit(asynchronous=False)

def run():
    """Boucle principale de consommation par micro-batch."""
    buffer = []
    print(f"Consumer pret. Accumulation par lots de {BATCH_SIZE}...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # Si le buffer contient un batch residuel apres un silence prolonge
                continue
            if msg.error():
                print(f"Erreur Kafka : {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            buffer.append(data)

            # Affichage de progression toutes les 100 lignes
            if len(buffer) % 100 == 0:
                print(f"  Buffer : {len(buffer)}/{BATCH_SIZE}")

            # Lot complet : declenchement du pipeline
            if len(buffer) >= BATCH_SIZE:
                print(f"\nLot de {BATCH_SIZE} atteint. Traitement du pipeline...")
                process_batch(buffer)
                buffer = []

    except KeyboardInterrupt:
        # Le buffer incomplet est volontairement ignore.
        # Comme l'offset n'a pas ete commite, Kafka rejouera
        # ces messages au prochain demarrage du consumer.
        if buffer:
            print(f"\n{len(buffer)} messages non commites (seront rejoues au redemarrage).")
        print("Arret du consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()
