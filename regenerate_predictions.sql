-- Regenerate predictions after entity merge
-- Run with: /opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -f regenerate_predictions.sql
--
-- Calls compute_prediction_for_pair_v1 directly per pair in a loop,
-- avoiding the single long-running statement that triggers timeout.

-- IE predictions
DO $$
DECLARE
  r RECORD;
  i INT := 0;
  v_result RECORD;
BEGIN
  FOR r IN
    SELECT DISTINCT s.buyer_entity_id, 
      coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id, 
      coalesce(public.normalize_prediction_region(s.region, null), 'IE') as region
    FROM signals s
    WHERE s.tenant_id = 'civant_default'
      AND s.buyer_entity_id NOT LIKE 'unresolved:%'
      AND coalesce(s.cpv_cluster_id, 'cluster_unknown') != 'cluster_unknown'
      AND coalesce(public.normalize_prediction_region(s.region, null), 'IE') = 'IE'
  LOOP
    BEGIN
      SELECT * INTO v_result FROM public.compute_prediction_for_pair_v1(
        'civant_default', r.buyer_entity_id, r.cpv_cluster_id, r.region, 'v1.0.0', now()
      );
    EXCEPTION WHEN OTHERS THEN
      -- skip failures silently
    END;
    i := i + 1;
    IF i % 500 = 0 THEN
      RAISE NOTICE 'IE: % pairs done', i;
    END IF;
  END LOOP;
  RAISE NOTICE 'IE complete: % pairs', i;
END $$;

-- ES predictions
DO $$
DECLARE
  r RECORD;
  i INT := 0;
  v_result RECORD;
BEGIN
  FOR r IN
    SELECT DISTINCT s.buyer_entity_id, 
      coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id, 
      coalesce(public.normalize_prediction_region(s.region, null), 'IE') as region
    FROM signals s
    WHERE s.tenant_id = 'civant_default'
      AND s.buyer_entity_id NOT LIKE 'unresolved:%'
      AND coalesce(s.cpv_cluster_id, 'cluster_unknown') != 'cluster_unknown'
      AND coalesce(public.normalize_prediction_region(s.region, null), 'IE') = 'ES'
  LOOP
    BEGIN
      SELECT * INTO v_result FROM public.compute_prediction_for_pair_v1(
        'civant_default', r.buyer_entity_id, r.cpv_cluster_id, r.region, 'v1.0.0', now()
      );
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
    i := i + 1;
    IF i % 500 = 0 THEN
      RAISE NOTICE 'ES: % pairs done', i;
    END IF;
  END LOOP;
  RAISE NOTICE 'ES complete: % pairs', i;
END $$;

-- FR predictions
DO $$
DECLARE
  r RECORD;
  i INT := 0;
  v_result RECORD;
BEGIN
  FOR r IN
    SELECT DISTINCT s.buyer_entity_id, 
      coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id, 
      coalesce(public.normalize_prediction_region(s.region, null), 'IE') as region
    FROM signals s
    WHERE s.tenant_id = 'civant_default'
      AND s.buyer_entity_id NOT LIKE 'unresolved:%'
      AND coalesce(s.cpv_cluster_id, 'cluster_unknown') != 'cluster_unknown'
      AND coalesce(public.normalize_prediction_region(s.region, null), 'IE') = 'FR'
  LOOP
    BEGIN
      SELECT * INTO v_result FROM public.compute_prediction_for_pair_v1(
        'civant_default', r.buyer_entity_id, r.cpv_cluster_id, r.region, 'v1.0.0', now()
      );
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
    i := i + 1;
    IF i % 500 = 0 THEN
      RAISE NOTICE 'FR: % pairs done', i;
    END IF;
  END LOOP;
  RAISE NOTICE 'FR complete: % pairs', i;
END $$;

-- Final results
SELECT region, confidence_band, COUNT(*) as cnt
FROM predictions_current
WHERE tenant_id = 'civant_default'
GROUP BY region, confidence_band
ORDER BY region, confidence_band;
