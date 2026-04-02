-- Table pour stocker le cache persistant des stats publicitaires (Meta + Google)
-- Les données sont stockées au niveau journalier pour permettre l'agrégation par période.
-- La clé unique (source, ad_name_raw, stat_date) permet l'upsert incrémental.
-- Quand un compte pub est supprimé/recréé, les données historiques restent préservées.

CREATE TABLE IF NOT EXISTS ads_stats_daily (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN ('meta', 'google')),
  ad_name_raw TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  stat_date DATE NOT NULL,
  spend DECIMAL(12,2) DEFAULT 0,
  impressions INTEGER DEFAULT 0,
  clicks INTEGER DEFAULT 0,
  leads DECIMAL(10,2) DEFAULT 0,
  account_id TEXT,
  synced_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(source, ad_name_raw, stat_date)
);

-- Index composite pour les requêtes par période + source
CREATE INDEX IF NOT EXISTS idx_ads_stats_daily_source_date
  ON ads_stats_daily (source, stat_date);

-- Index sur normalized_name pour l'agrégation par client
CREATE INDEX IF NOT EXISTS idx_ads_stats_daily_normalized
  ON ads_stats_daily (normalized_name);

-- Index pour la date seule (requêtes par plage de dates)
CREATE INDEX IF NOT EXISTS idx_ads_stats_daily_date
  ON ads_stats_daily (stat_date);

-- Table de métadonnées pour suivre l'état de la synchronisation
CREATE TABLE IF NOT EXISTS ads_stats_sync_metadata (
  id SERIAL PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN ('meta', 'google')),
  last_full_sync TIMESTAMPTZ,
  last_incremental_sync TIMESTAMPTZ,
  last_sync_status TEXT DEFAULT 'pending',
  last_sync_error TEXT,
  rows_synced INTEGER DEFAULT 0,
  UNIQUE(source)
);

-- Insérer les entrées de métadonnées initiales
INSERT INTO ads_stats_sync_metadata (source, last_sync_status)
VALUES ('meta', 'pending'), ('google', 'pending')
ON CONFLICT (source) DO NOTHING;

-- RLS policies
ALTER TABLE ads_stats_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE ads_stats_sync_metadata ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'ads_stats_daily' AND policyname = 'Allow public read ads_stats_daily') THEN
    CREATE POLICY "Allow public read ads_stats_daily"
      ON ads_stats_daily FOR SELECT TO anon, authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'ads_stats_daily' AND policyname = 'Allow service role full access ads_stats_daily') THEN
    CREATE POLICY "Allow service role full access ads_stats_daily"
      ON ads_stats_daily FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'ads_stats_sync_metadata' AND policyname = 'Allow public read ads_stats_sync_metadata') THEN
    CREATE POLICY "Allow public read ads_stats_sync_metadata"
      ON ads_stats_sync_metadata FOR SELECT TO anon, authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'ads_stats_sync_metadata' AND policyname = 'Allow service role full access ads_stats_sync_metadata') THEN
    CREATE POLICY "Allow service role full access ads_stats_sync_metadata"
      ON ads_stats_sync_metadata FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END $$;

COMMENT ON TABLE ads_stats_daily IS 'Cache persistant des stats publicitaires Meta/Google par jour et par campagne/adset';
COMMENT ON TABLE ads_stats_sync_metadata IS 'Métadonnées de synchronisation pour le cache ads stats';

-- Fonction RPC pour agréger les stats par source et normalized_name
-- Retourne ~400 lignes au lieu de milliers (évite la limite de 1000 lignes de PostgREST)
-- first_stat_date = date absolue la plus ancienne (pas filtrée par période) pour montrer l'historique complet
CREATE OR REPLACE FUNCTION get_ads_stats_aggregated(since_date DATE)
RETURNS TABLE (
  source TEXT,
  normalized_name TEXT,
  total_spend DECIMAL,
  total_impressions BIGINT,
  total_clicks BIGINT,
  total_leads DECIMAL,
  account_ids TEXT[],
  first_stat_date DATE
) LANGUAGE sql STABLE AS $$
  WITH period_stats AS (
    SELECT source, normalized_name,
      SUM(spend) AS total_spend,
      SUM(impressions)::BIGINT AS total_impressions,
      SUM(clicks)::BIGINT AS total_clicks,
      SUM(leads) AS total_leads,
      array_agg(DISTINCT account_id) FILTER (WHERE account_id IS NOT NULL AND account_id LIKE '%|||%') AS account_ids
    FROM ads_stats_daily WHERE stat_date >= since_date
    GROUP BY source, normalized_name
  ),
  first_dates AS (
    SELECT source, normalized_name, MIN(stat_date) AS first_stat_date
    FROM ads_stats_daily GROUP BY source, normalized_name
  )
  SELECT p.source, p.normalized_name, p.total_spend, p.total_impressions,
         p.total_clicks, p.total_leads, p.account_ids, f.first_stat_date
  FROM period_stats p LEFT JOIN first_dates f USING (source, normalized_name);
$$;
