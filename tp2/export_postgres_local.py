import os
from datetime import datetime
import psycopg2
import pandas as pd

# CONFIGURATION POSTGRESQL
DB_CONFIG = {
    'host': 'localhost',
    'database': 'shopstream',
    'user': 'postgres',
    'password': 'Agourai1984'  # VOTRE mot de passe PostgreSQL
}

# Tables à exporter
TABLES = ['users', 'products', 'orders', 'order_items', 'crm_contacts']

# Dossier local pour enregistrer les fichiers
EXPORT_FOLDER = './csv_exports'
os.makedirs(EXPORT_FOLDER, exist_ok=True)

def get_db_connection():
    """Connexion PostgreSQL"""
    print("Connexion à PostgreSQL...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connexion PostgreSQL réussie")
        return conn
    except Exception as e:
        print(f"Erreur de connexion PostgreSQL : {e}")
        exit(1)

def export_table_to_local(table_name):
    """Exporte une table PostgreSQL en CSV local"""
    print(f"\nExport de la table '{table_name}'...")
    conn = get_db_connection()
    query = f"SELECT * FROM {table_name}"
    try:
        df = pd.read_sql(query, conn)
        print(f"   {len(df)} lignes extraites de PostgreSQL")
    except Exception as e:
        print(f"   Erreur lecture table : {e}")
        conn.close()
        return
    conn.close()
    
    if df.empty:
        print(f"   Table '{table_name}' vide, pas d'export")
        return
    
    file_path = os.path.join(EXPORT_FOLDER, f"{table_name}.csv")
    df.to_csv(file_path, index=False)
    print(f"   {table_name}.csv créé localement dans {file_path}")

def export_events_to_local():
    """Exporte la table events au format JSON local"""
    print(f"\nExport de la table 'events'...")
    conn = get_db_connection()
    query = "SELECT id, user_id, event_type, event_ts, metadata FROM events"
    try:
        df = pd.read_sql(query, conn)
        print(f"   {len(df)} lignes extraites de PostgreSQL")
    except Exception as e:
        print(f"   Erreur lecture events : {e}")
        conn.close()
        return
    conn.close()
    
    if df.empty:
        print(f"   Table 'events' vide, pas d'export")
        return
    
    file_path = os.path.join(EXPORT_FOLDER, "events.json")
    df.to_json(file_path, orient='records', date_format='iso')
    print(f"   events.json créé localement dans {file_path}")

def main():
    print("="*70)
    print("EXPORT POSTGRESQL vers fichiers locaux")
    print("="*70)
    
    # Export des tables relationnelles
    for table in TABLES:
        export_table_to_local(table)
    
    # Export table events
    export_events_to_local()
    
    print("\nEXPORT TERMINÉ. Tous les fichiers sont dans :", EXPORT_FOLDER)

if __name__ == "__main__":
    main()
