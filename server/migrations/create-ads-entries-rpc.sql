-- RPC pour agréger ads_stats_daily par source + normalized_name + ad_name_raw
-- Retourne ~quelques centaines de lignes au lieu de 50 000 (agrégation côté SQL)
CREATE OR REPLACE FUNCTION get_ads_entries_aggregated(since_date DATE)
RETURNS TABLE (
  source TEXT,
  normalized_name TEXT,
  ad_name_raw TEXT,
  total_spend DECIMAL,
  total_impressions BIGINT,
  total_clicks BIGINT,
  total_leads DECIMAL
) LANGUAGE sql STABLE AS $$
  SELECT source, normalized_name, ad_name_raw,
    SUM(spend) AS total_spend,
    SUM(impressions)::BIGINT AS total_impressions,
    SUM(clicks)::BIGINT AS total_clicks,
    SUM(leads) AS total_leads
  FROM ads_stats_daily
  WHERE stat_date >= since_date
  GROUP BY source, normalized_name, ad_name_raw;
$$;
