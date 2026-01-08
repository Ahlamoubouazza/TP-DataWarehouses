-- 1. Dimension Temps (Crucial pour les analyses mensuelles/hebdomadaires)
CREATE TABLE dwh.dim_date (
    date_key DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    quarter INT,
    day_name VARCHAR(20)
);

-- 2. Dimension Utilisateurs (On copie les infos de la source)
CREATE TABLE dwh.dim_users (
    user_key SERIAL PRIMARY KEY,
    user_id INT, 
    username VARCHAR(100),
    country VARCHAR(100),
    plan_type VARCHAR(50)
);

-- 3. Dimension Morceaux (Dénormalisée : contient le nom de l'artiste directement)
CREATE TABLE dwh.dim_tracks (
    track_key SERIAL PRIMARY KEY,
    track_id INT,
    title VARCHAR(255),
    artist_name VARCHAR(255),
    genre VARCHAR(100)
);

-- 4. Table de Faits : Écoutes (La table centrale pour les KPIs)
CREATE TABLE dwh.fact_streams (
    fact_id SERIAL PRIMARY KEY,
    date_key DATE REFERENCES dwh.dim_date(date_key),
    user_key INT REFERENCES dwh.dim_users(user_key),
    track_key INT REFERENCES dwh.dim_tracks(track_key),
    stream_count INT DEFAULT 1,
    total_duration_sec INT
);