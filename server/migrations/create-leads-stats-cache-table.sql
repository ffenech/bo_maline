-- Table pour stocker le cache persistant des stats leads (V1, V2, ES)
-- Les données sont stockées au niveau journalier.
-- La clé unique (source, stat_date) permet l'upsert incrémental.

CREATE TABLE IF NOT EXISTS leads_stats_daily (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN ('v1', 'v2', 'es')),
  stat_date DATE NOT NULL,
  total_leads INTEGER DEFAULT 0,
  leads_with_phone INTEGER DEFAULT 0,
  leads_with_validated_phone INTEGER DEFAULT 0,
  ga4_visitors INTEGER DEFAULT 0,
  synced_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(source, stat_date)
);

-- Index composite pour les requêtes par source + période
CREATE INDEX IF NOT EXISTS idx_leads_stats_daily_source_date
  ON leads_stats_daily (source, stat_date);

-- Index pour la date seule (requêtes par plage de dates)
CREATE INDEX IF NOT EXISTS idx_leads_stats_daily_date
  ON leads_stats_daily (stat_date);

-- Table de métadonnées pour suivre l'état de la synchronisation
CREATE TABLE IF NOT EXISTS leads_stats_sync_metadata (
  id SERIAL PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN ('v1', 'v2', 'es')),
  last_sync TIMESTAMPTZ,
  last_sync_status TEXT DEFAULT 'pending',
  last_sync_error TEXT,
  rows_synced INTEGER DEFAULT 0,
  UNIQUE(source)
);

-- Insérer les entrées de métadonnées initiales
INSERT INTO leads_stats_sync_metadata (source, last_sync_status)
VALUES ('v1', 'pending'), ('v2', 'pending'), ('es', 'pending')
ON CONFLICT (source) DO NOTHING;

-- RLS policies
ALTER TABLE leads_stats_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE leads_stats_sync_metadata ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'leads_stats_daily' AND policyname = 'Allow public read leads_stats_daily') THEN
    CREATE POLICY "Allow public read leads_stats_daily"
      ON leads_stats_daily FOR SELECT TO anon, authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'leads_stats_daily' AND policyname = 'Allow service role full access leads_stats_daily') THEN
    CREATE POLICY "Allow service role full access leads_stats_daily"
      ON leads_stats_daily FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'leads_stats_sync_metadata' AND policyname = 'Allow public read leads_stats_sync_metadata') THEN
    CREATE POLICY "Allow public read leads_stats_sync_metadata"
      ON leads_stats_sync_metadata FOR SELECT TO anon, authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'leads_stats_sync_metadata' AND policyname = 'Allow service role full access leads_stats_sync_metadata') THEN
    CREATE POLICY "Allow service role full access leads_stats_sync_metadata"
      ON leads_stats_sync_metadata FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END $$;

COMMENT ON TABLE leads_stats_daily IS 'Cache persistant des stats leads V1/V2/ES par jour';
COMMENT ON TABLE leads_stats_sync_metadata IS 'Métadonnées de synchronisation pour le cache leads stats';
