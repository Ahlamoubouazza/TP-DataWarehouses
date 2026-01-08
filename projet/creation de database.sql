-- Création de la base de données
CREATE DATABASE spotify_oltp;
\c spotify_oltp;

-- Table des Artistes
CREATE TABLE artists (
    artist_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    genre VARCHAR(100),
    country VARCHAR(100)
);

-- Table des Morceaux
CREATE TABLE tracks (
    track_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    artist_id INT REFERENCES artists(artist_id),
    album VARCHAR(255),
    duration_sec INT CHECK (duration_sec > 0)
);

-- Table des Utilisateurs
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    country VARCHAR(100),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des Abonnements (Pour gérer le passage Free -> Premium)
CREATE TABLE subscriptions (
    sub_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    plan_type VARCHAR(50) CHECK (plan_type IN ('Free', 'Premium')),
    price DECIMAL(10,2),
    start_date TIMESTAMP,
    end_date TIMESTAMP
);

-- Table des Écoutes (Logs de streaming)
CREATE TABLE streams (
    stream_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    track_id INT REFERENCES tracks(track_id),
    stream_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);