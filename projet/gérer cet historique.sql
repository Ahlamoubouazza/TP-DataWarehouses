-- Ajout des colonnes pour la gestion de l'historique (SCD Type 2)
ALTER TABLE dwh.dim_users 
ADD COLUMN start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN end_date TIMESTAMP,
ADD COLUMN is_active BOOLEAN DEFAULT TRUE;

-- On met à jour les données existantes pour dire qu'elles sont actives
UPDATE dwh.dim_users 
SET start_date = '2024-01-01 00:00:00', 
    is_active = TRUE 
WHERE start_date IS NULL;