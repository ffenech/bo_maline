-- Table pour stocker le cache persistant des stats pub (endpoint /api/pub-stats)
-- Les données sont stockées comme JSONB complet par période (4 lignes max).
-- La clé unique (period) permet l'upsert par période.

CREATE TABLE IF NOT EXISTS pub_stats_cache (
  id SERIAL PRIMARY KEY,
  period TEXT NOT NULL CHECK (period IN ('all', 'month', '30d', '90d')),
  response_json JSONB NOT NULL,
  synced_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(period)
);

-- Index sur period pour les lookups directs
CREATE INDEX IF NOT EXISTS idx_pub_stats_cache_period
  ON pub_stats_cache (period);

-- Table de métadonnées pour suivre l'état de la synchronisation
CREATE TABLE IF NOT EXISTS pub_stats_sync_metadata (
  id SERIAL PRIMARY KEY,
  period TEXT NOT NULL CHECK (period IN ('all', 'month', '30d', '90d')),
  last_sync TIMESTAMPTZ,
  last_sync_status TEXT DEFAULT 'pending',
  last_sync_error TEXT,
  sync_duration_ms INTEGER DEFAULT 0,
  UNIQUE(period)
);

-- Insérer les entrées de métadonnées initiales
INSERT INTO pub_stats_sync_metadata (period, last_sync_status)
VALUES ('all', 'pending'), ('month', 'pending'), ('30d', 'pending'), ('90d', 'pending')
ON CONFLICT (period) DO NOTHING;

-- RLS policies
ALTER TABLE pub_stats_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE pub_stats_sync_metadata ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'pub_stats_cache' AND policyname = 'Allow public read pub_stats_cache') THEN
    CREATE POLICY "Allow public read pub_stats_cache"
      ON pub_stats_cache FOR SELECT TO anon, authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'pub_stats_cache' AND policyname = 'Allow service role full access pub_stats_cache') THEN
    CREATE POLICY "Allow service role full access pub_stats_cache"
      ON pub_stats_cache FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'pub_stats_sync_metadata' AND policyname = 'Allow public read pub_stats_sync_metadata') THEN
    CREATE POLICY "Allow public read pub_stats_sync_metadata"
      ON pub_stats_sync_metadata FOR SELECT TO anon, authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'pub_stats_sync_metadata' AND policyname = 'Allow service role full access pub_stats_sync_metadata') THEN
    CREATE POLICY "Allow service role full access pub_stats_sync_metadata"
      ON pub_stats_sync_metadata FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END $$;

COMMENT ON TABLE pub_stats_cache IS 'Cache persistant des stats pub (response JSONB complète par période)';
COMMENT ON TABLE pub_stats_sync_metadata IS 'Métadonnées de synchronisation pour le cache pub stats';
