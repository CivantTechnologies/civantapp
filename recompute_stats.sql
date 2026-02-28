-- Recompute buyer_category_stats for all merged entities
-- Run with: /opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -f recompute_stats.sql

-- Ireland first (smallest - ~10K pairs)
DO $$
DECLARE
  r RECORD;
  i INT := 0;
BEGIN
  FOR r IN
    SELECT DISTINCT buyer_entity_id, cpv_cluster_id, region
    FROM signals
    WHERE tenant_id = 'civant_default'
      AND buyer_entity_id NOT LIKE 'unresolved:%'
      AND cpv_cluster_id != 'cluster_unknown'
      AND region = 'IE'
  LOOP
    PERFORM public.recompute_buyer_category_stats_v2_1(
      'civant_default',
      r.buyer_entity_id,
      r.cpv_cluster_id,
      r.region,
      interval '15 years'
    );
    i := i + 1;
    IF i % 500 = 0 THEN
      RAISE NOTICE 'IE: % pairs done', i;
    END IF;
  END LOOP;
  RAISE NOTICE 'IE complete: % pairs', i;
END $$;

-- Spain (~19K pairs)
DO $$
DECLARE
  r RECORD;
  i INT := 0;
BEGIN
  FOR r IN
    SELECT DISTINCT buyer_entity_id, cpv_cluster_id, region
    FROM signals
    WHERE tenant_id = 'civant_default'
      AND buyer_entity_id NOT LIKE 'unresolved:%'
      AND cpv_cluster_id != 'cluster_unknown'
      AND region = 'ES'
  LOOP
    PERFORM public.recompute_buyer_category_stats_v2_1(
      'civant_default',
      r.buyer_entity_id,
      r.cpv_cluster_id,
      r.region,
      interval '15 years'
    );
    i := i + 1;
    IF i % 500 = 0 THEN
      RAISE NOTICE 'ES: % pairs done', i;
    END IF;
  END LOOP;
  RAISE NOTICE 'ES complete: % pairs', i;
END $$;

-- France (~38K pairs)
DO $$
DECLARE
  r RECORD;
  i INT := 0;
BEGIN
  FOR r IN
    SELECT DISTINCT buyer_entity_id, cpv_cluster_id, region
    FROM signals
    WHERE tenant_id = 'civant_default'
      AND buyer_entity_id NOT LIKE 'unresolved:%'
      AND cpv_cluster_id != 'cluster_unknown'
      AND region = 'FR'
  LOOP
    PERFORM public.recompute_buyer_category_stats_v2_1(
      'civant_default',
      r.buyer_entity_id,
      r.cpv_cluster_id,
      r.region,
      interval '15 years'
    );
    i := i + 1;
    IF i % 500 = 0 THEN
      RAISE NOTICE 'FR: % pairs done', i;
    END IF;
  END LOOP;
  RAISE NOTICE 'FR complete: % pairs', i;
END $$;
