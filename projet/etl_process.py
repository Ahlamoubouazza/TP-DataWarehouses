import psycopg2
from datetime import datetime

# Configuration de la connexion
DB_CONFIG = {
    "host": "localhost",
    "database": "spotify_oltp",
    "user": "postgres",
    "password": "admin123" # Vérifie bien ton mot de passe
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("--- Début du Pipeline ETL ---")

    # 1. Remplissage de dim_date
    print("Étape 1 : Alimentation de dim_date...")
    cur.execute("""
        INSERT INTO dwh.dim_date (date_key, day, month, year, quarter, day_name)
        SELECT DISTINCT 
            stream_timestamp::date, 
            EXTRACT(DAY FROM stream_timestamp),
            EXTRACT(MONTH FROM stream_timestamp),
            EXTRACT(YEAR FROM stream_timestamp),
            EXTRACT(QUARTER FROM stream_timestamp),
            TO_CHAR(stream_timestamp, 'Day')
        FROM public.streams
        ON CONFLICT (date_key) DO NOTHING;
    """)

    # 2. Gestion SCD Type 2 pour dim_users
    print("Étape 2 : Mise à jour de dim_users (Logique SCD Type 2)...")
    cur.execute("""
        SELECT u.user_id, u.username, u.country, s.plan_type
        FROM public.users u
        LEFT JOIN public.subscriptions s ON u.user_id = s.user_id;
    """)
    source_users = cur.fetchall()

    for row in source_users:
        u_id, u_name, u_country, u_plan = row
        cur.execute("SELECT plan_type FROM dwh.dim_users WHERE user_id = %s AND is_active = TRUE", (u_id,))
        result = cur.fetchone()
        
        if result is None:
            cur.execute("""
                INSERT INTO dwh.dim_users (user_id, username, country, plan_type, start_date, is_active)
                VALUES (%s, %s, %s, %s, %s, TRUE)
            """, (u_id, u_name, u_country, u_plan, datetime.now()))
        elif result[0] != u_plan:
            cur.execute("""
                UPDATE dwh.dim_users SET end_date = %s, is_active = FALSE 
                WHERE user_id = %s AND is_active = TRUE
            """, (datetime.now(), u_id))
            cur.execute("""
                INSERT INTO dwh.dim_users (user_id, username, country, plan_type, start_date, is_active)
                VALUES (%s, %s, %s, %s, %s, TRUE)
            """, (u_id, u_name, u_country, u_plan, datetime.now()))

    # 3. Remplissage de dim_tracks
    print("Étape 3 : Alimentation de dim_tracks...")
    cur.execute("TRUNCATE dwh.dim_tracks CASCADE;") 
    cur.execute("""
        INSERT INTO dwh.dim_tracks (track_id, title, artist_name, genre)
        SELECT t.track_id, t.title, a.name, a.genre
        FROM public.tracks t
        JOIN public.artists a ON t.artist_id = a.artist_id;
    """)

    # 4. Remplissage de fact_streams
    print("Étape 4 : Alimentation de fact_streams...")
    cur.execute("TRUNCATE dwh.fact_streams CASCADE;")
    cur.execute("""
        INSERT INTO dwh.fact_streams (date_key, user_key, track_key, total_duration_sec)
        SELECT s.stream_timestamp::date, u.user_key, t.track_key, tr.duration_sec
        FROM public.streams s
        JOIN dwh.dim_users u ON s.user_id = u.user_id AND u.is_active = TRUE
        JOIN dwh.dim_tracks t ON s.track_id = t.track_id
        JOIN public.tracks tr ON s.track_id = tr.track_id;
    """)

    # 5. Data Quality Check
    print("--- Vérification de la Qualité des Données ---")
    cur.execute("SELECT COUNT(*) FROM public.tracks WHERE duration_sec <= 0")
    bad_tracks = cur.fetchone()[0]
    print(f"Qualité : {bad_tracks} erreurs de durée détectées.")

    conn.commit()
    print("--- ETL terminé avec succès ! ---")

except Exception as e:
    print(f"Erreur durant l'ETL : {e}")
    conn.rollback()
finally:
    cur.close()
    conn.close()