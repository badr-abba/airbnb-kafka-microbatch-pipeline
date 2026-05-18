import pandas as pd

def clean_airbnb_batch(df):
    """Nettoyage et transformation d'un batch de donnees AirBnB."""

    # Remplissage des reviews manquantes par 0
    if 'reviews_per_month' in df.columns:
        df['reviews_per_month'] = df['reviews_per_month'].fillna(0)

    # Suppression des lignes sans date de derniere review
    if 'last_review' in df.columns:
        df = df.dropna(subset=['last_review']).copy()
        df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')

    # Suppression des outliers de prix par IQR
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df = df.dropna(subset=['price'])

        q1 = df['price'].quantile(0.25)
        q3 = df['price'].quantile(0.75)
        iqr = q3 - q1
        df = df[df['price'].between(q1 - 1.5 * iqr, q3 + 1.5 * iqr)]

    return df
