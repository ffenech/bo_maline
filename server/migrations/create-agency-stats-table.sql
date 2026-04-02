-- Table pour stocker les statistiques des agences
-- Cette table est synchronisée depuis PostgreSQL V3 (stats) et API Territory V2 (codes postaux, tarifs)
-- via le script sync-agency-stats.ts
-- Elle permet aux Netlify Functions d'accéder aux données sans tunnel SSH

CREATE TABLE IF NOT EXISTS agency_stats (
  id_client UUID PRIMARY KEY,
  client_name TEXT NOT NULL,
  id_gocardless TEXT,
  nb_leads_total INTEGER DEFAULT 0,
  nb_leads INTEGER DEFAULT 0,
  nb_leads_zone_total INTEGER DEFAULT 0,
  nb_leads_zone INTEGER DEFAULT 0,
  nb_leads_zone_phone_valid INTEGER DEFAULT 0,
  sector_postal_codes TEXT,
  tarifs TEXT,
  leads_contacted INTEGER DEFAULT 0,
  leads_with_reminder INTEGER DEFAULT 0,
  avg_reminders_done DECIMAL(5,2) DEFAULT 0,
  mandats_signed INTEGER DEFAULT 0,
  pct_lead_contacte DECIMAL(5,2) DEFAULT 0,
  pct_relance_prevu DECIMAL(5,2) DEFAULT 0,
  nombre_logements INTEGER DEFAULT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Migration: ajouter la colonne tarifs si elle n'existe pas
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'agency_stats' AND column_name = 'tarifs') THEN
    ALTER TABLE agency_stats ADD COLUMN tarifs TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'agency_stats' AND column_name = 'nb_leads_total') THEN
    ALTER TABLE agency_stats ADD COLUMN nb_leads_total INTEGER DEFAULT 0;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'agency_stats' AND column_name = 'nb_leads_zone_total') THEN
    ALTER TABLE agency_stats ADD COLUMN nb_leads_zone_total INTEGER DEFAULT 0;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'agency_stats' AND column_name = 'nb_leads_zone') THEN
    ALTER TABLE agency_stats ADD COLUMN nb_leads_zone INTEGER DEFAULT 0;
  END IF;
END $$;

-- Index pour la recherche par nom
CREATE INDEX IF NOT EXISTS idx_agency_stats_client_name
  ON agency_stats USING gin (to_tsvector('french', client_name));

-- Index pour la recherche par GoCardLess ID
CREATE INDEX IF NOT EXISTS idx_agency_stats_gocardless
  ON agency_stats (id_gocardless)
  WHERE id_gocardless IS NOT NULL;

-- Politique RLS pour permettre la lecture publique (API)
ALTER TABLE agency_stats ENABLE ROW LEVEL SECURITY;

-- Politique de lecture pour tous (API publique)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'agency_stats' AND policyname = 'Allow public read access to agency_stats') THEN
    CREATE POLICY "Allow public read access to agency_stats"
      ON agency_stats
      FOR SELECT
      TO anon, authenticated
      USING (true);
  END IF;
END $$;

-- Politique d'écriture pour le service role uniquement (script de sync)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'agency_stats' AND policyname = 'Allow service role to insert/update agency_stats') THEN
    CREATE POLICY "Allow service role to insert/update agency_stats"
      ON agency_stats
      FOR ALL
      TO service_role
      USING (true)
      WITH CHECK (true);
  END IF;
END $$;

-- Commentaires
COMMENT ON TABLE agency_stats IS 'Statistiques des agences synchronisées depuis PostgreSQL V3 (stats) et API Territory V2 (codes postaux, tarifs)';
COMMENT ON COLUMN agency_stats.id_client IS 'UUID du client (clé primaire)';
COMMENT ON COLUMN agency_stats.id_gocardless IS 'Identifiant GoCardLess pour la recherche externe et matching V2';
COMMENT ON COLUMN agency_stats.nb_leads_total IS 'Nombre total de leads';
COMMENT ON COLUMN agency_stats.nb_leads IS 'Nombre total de leads avec téléphone';
COMMENT ON COLUMN agency_stats.nb_leads_zone_total IS 'Nombre de leads dans la zone (codes postaux V2)';
COMMENT ON COLUMN agency_stats.nb_leads_zone IS 'Nombre de leads dans la zone avec téléphone';
COMMENT ON COLUMN agency_stats.nb_leads_zone_phone_valid IS 'Nombre de leads dans la zone avec téléphone validé';
COMMENT ON COLUMN agency_stats.sector_postal_codes IS 'Codes postaux ciblés depuis API V2 (JSON array sous forme de texte)';
COMMENT ON COLUMN agency_stats.tarifs IS 'Tarifs par code postal depuis API V2 (JSON array: [{code_postal, tarif}])';
COMMENT ON COLUMN agency_stats.pct_lead_contacte IS 'Pourcentage de leads contactés';
COMMENT ON COLUMN agency_stats.pct_relance_prevu IS 'Pourcentage de leads avec relance prévue';
COMMENT ON COLUMN agency_stats.avg_reminders_done IS 'Nombre moyen de relances effectuées';
COMMENT ON COLUMN agency_stats.mandats_signed IS 'Nombre de mandats signés';
COMMENT ON COLUMN agency_stats.nombre_logements IS 'Nombre de logements de l''agence (depuis API Territory)';
