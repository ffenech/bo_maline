-- Migration: Ajouter la colonne nombre_logements
-- Cette colonne contient le nombre de logements de l'agence depuis l'API Territory

-- Ajouter la colonne si elle n'existe pas
ALTER TABLE agency_stats ADD COLUMN IF NOT EXISTS nombre_logements INTEGER DEFAULT NULL;

-- Commentaire
COMMENT ON COLUMN agency_stats.nombre_logements IS 'Nombre de logements de l''agence (depuis API Territory)';
