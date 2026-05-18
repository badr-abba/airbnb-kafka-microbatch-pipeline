import os
import pandas as pd

# Configuration
SOURCE_FILE = 'listings.csv'
OUTPUT_DIR = 'raw_data'
CHUNK_SIZE = 1000

os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"Lecture et decoupage de {SOURCE_FILE} en lots de {CHUNK_SIZE} lignes...")

try:
    reader = pd.read_csv(SOURCE_FILE, chunksize=CHUNK_SIZE)

    for i, chunk in enumerate(reader):
        # Nettoyage des vrais retours a la ligne dans les colonnes texte
        for col in chunk.select_dtypes(include=['object', 'string']).columns:
            chunk[col] = chunk[col].str.replace(r'\r\n|\r|\n', ' ', regex=True)

        output_file = os.path.join(OUTPUT_DIR, f'airbnb_part_{i + 1}.csv')
        chunk.to_csv(output_file, index=False, encoding='utf-8')
        print(f"  Lot {i + 1} : {output_file} ({len(chunk)} lignes)")

    print(f"\nDecoupage termine. Fichiers dans '{OUTPUT_DIR}/'.")

except FileNotFoundError:
    print(f"Erreur : fichier '{SOURCE_FILE}' introuvable.")
except Exception as e:
    print(f"Erreur : {e}")
