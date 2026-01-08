import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

# Configuration de la connexion
conn = psycopg2.connect(
    host="localhost",
    database="spotify_oltp",
    user="postgres",
    password="admin123" 
)
cur = conn.cursor()
fake = Faker()

print("Début de la génération des données...")

# 1. Générer des Artistes
genres = ['Rock', 'Pop', 'Hip-Hop', 'Jazz', 'Electro', 'Classical']
for _ in range(50):
    cur.execute("INSERT INTO artists (name, genre, country) VALUES (%s, %s, %s)",
                (fake.name(), random.choice(genres), fake.country()))

# 2. Générer des Morceaux (Tracks)
cur.execute("SELECT artist_id FROM artists")
artist_ids = [row[0] for row in cur.fetchall()]
for _ in range(200):
    cur.execute("INSERT INTO tracks (title, artist_id, album, duration_sec) VALUES (%s, %s, %s, %s)",
                (fake.sentence(nb_words=3), random.choice(artist_ids), fake.word().capitalize(), random.randint(120, 300)))

# 3. Générer des Utilisateurs
for _ in range(100):
    cur.execute("INSERT INTO users (username, email, country, registration_date) VALUES (%s, %s, %s, %s)",
                (fake.user_name(), fake.email(), fake.country(), fake.date_between(start_date='-1y', end_date='today')))

# --- ÉTAPE AJOUTÉE : 3.5 Générer des Abonnements ---
print("Attribution des abonnements aux utilisateurs...")
cur.execute("SELECT user_id FROM users")
user_ids = [row[0] for row in cur.fetchall()]

for u_id in user_ids:
    plan = random.choice(['Free', 'Premium'])
    cur.execute("INSERT INTO subscriptions (user_id, plan_type, start_date) VALUES (%s, %s, %s)",
                (u_id, plan, fake.date_between(start_date='-1y', end_date='today')))

# 4. Générer des Écoutes (Streams)
cur.execute("SELECT track_id FROM tracks")
track_ids = [row[0] for row in cur.fetchall()]

print("Simulation des écoutes...")
for _ in range(2000):
    cur.execute("INSERT INTO streams (user_id, track_id, stream_timestamp) VALUES (%s, %s, %s)",
                (random.choice(user_ids), random.choice(track_ids), fake.date_time_between(start_date='-90d', end_date='now')))

conn.commit()
cur.close()
conn.close()
print("Succès ! Toutes les tables, y compris 'subscriptions', sont remplies.")