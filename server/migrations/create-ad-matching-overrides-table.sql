-- Table pour les overrides manuels de matching pub → client
CREATE TABLE IF NOT EXISTS ad_matching_overrides (
  id SERIAL PRIMARY KEY,
  normalized_name TEXT NOT NULL UNIQUE,
  id_client TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE ad_matching_overrides ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read ad_matching_overrides"
  ON ad_matching_overrides FOR SELECT TO anon, authenticated USING (true);

CREATE POLICY "Allow service role full access ad_matching_overrides"
  ON ad_matching_overrides FOR ALL TO service_role USING (true) WITH CHECK (true);
