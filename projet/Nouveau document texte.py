import pandas as pd
import psycopg2

# Connexion à ta base locale PostgreSQL
conn = psycopg2.connect(
    host="localhost", 
    database="spotify_oltp", 
    user="postgres", 
    password="admin123"
)

# Liste des tables à exporter
tables = ['users', 'tracks', 'artists', 'subscriptions', 'streams']

for table in tables:
    df = pd.read_sql(f"SELECT * FROM public.{table}", conn)
    df.to_csv(f"{table}.csv", index=False)
    print(f"Fichier {table}.csv généré avec succès.")

conn.close()