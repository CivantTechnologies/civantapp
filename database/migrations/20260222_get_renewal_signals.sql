DROP FUNCTION IF EXISTS get_renewal_signals(int, numeric);

CREATE OR REPLACE FUNCTION get_renewal_signals(
    p_months_ahead int DEFAULT 18,
    p_min_value_eur numeric DEFAULT 0
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '60s'
AS $fn$
DECLARE
    v_result jsonb;
    v_cutoff_date date := current_date + (p_months_ahead || ' months')::interval;
    v_now date := current_date;
BEGIN

WITH all_awards AS (
    SELECT supplier_name_raw, buyer_id, buyer_name_raw, cpv_cluster,
           'IE' as country, award_value_eur, start_date, end_date,
           duration_months, COALESCE(framework_flag, false) as framework_flag
    FROM award_fact_ie WHERE end_date IS NOT NULL
    UNION ALL
    SELECT supplier_name_raw, buyer_id, buyer_name_raw, cpv_cluster,
           'ES', award_value_eur, start_date, end_date,
           duration_months, COALESCE(framework_flag, false)
    FROM award_fact_es WHERE end_date IS NOT NULL
),
expiring AS (
    SELECT * FROM all_awards
    WHERE end_date BETWEEN v_now AND v_cutoff_date
      AND (p_min_value_eur = 0 OR COALESCE(award_value_eur, 0) >= p_min_value_eur)
),
bcc AS (
    SELECT
        buyer_id,
        COALESCE(buyer_name_raw, buyer_id) as buyer_name,
        cpv_cluster,
        country,
        COUNT(*) as total_contracts,
        COUNT(*) FILTER (WHERE end_date BETWEEN v_now AND v_cutoff_date) as expiring_count,
        AVG(duration_months) FILTER (WHERE duration_months > 0) as avg_dur,
        STDDEV(duration_months) FILTER (WHERE duration_months > 0) as std_dur,
        MAX(end_date) FILTER (WHERE end_date BETWEEN v_now AND v_cutoff_date) as latest_end_date,
        SUM(award_value_eur) as total_val,
        AVG(award_value_eur) FILTER (WHERE award_value_eur > 0) as avg_val,
        COUNT(DISTINCT supplier_name_raw) as dist_suppliers,
        array_agg(DISTINCT supplier_name_raw ORDER BY supplier_name_raw)
            FILTER (WHERE supplier_name_raw IS NOT NULL) as suppliers,
        bool_or(framework_flag) as has_fw
    FROM all_awards
    WHERE buyer_id IS NOT NULL AND cpv_cluster IS NOT NULL
    GROUP BY buyer_id, buyer_name_raw, cpv_cluster, country
    HAVING COUNT(*) FILTER (WHERE end_date BETWEEN v_now AND v_cutoff_date) > 0
),
rs AS (
    SELECT *,
        CASE WHEN has_fw THEN latest_end_date - interval '9 months'
             ELSE latest_end_date - interval '4 months' END as pred_date,
        CASE WHEN has_fw THEN 'framework_expiry'
             WHEN total_contracts >= 3 AND std_dur IS NOT NULL AND std_dur < 6 THEN 'repeat_buyer_cycle'
             ELSE 'contract_renewal' END as sig_type,
        CASE WHEN has_fw AND total_contracts >= 2 THEN 0.92
             WHEN total_contracts >= 3 AND std_dur IS NOT NULL AND std_dur < 6 THEN 0.88
             WHEN total_contracts >= 2 THEN 0.75
             ELSE 0.62 END as confidence,
        (latest_end_date - v_now) as days_exp,
        CASE WHEN (latest_end_date - v_now) < 90 THEN 'imminent'
             WHEN (latest_end_date - v_now) < 180 THEN 'upcoming'
             WHEN (latest_end_date - v_now) < 365 THEN 'horizon'
             ELSE 'distant' END as urgency
    FROM bcc
),
ed AS (
    SELECT e.buyer_id, e.cpv_cluster, e.country,
        jsonb_agg(jsonb_build_object(
            'supplier', e.supplier_name_raw,
            'value_eur', e.award_value_eur,
            'start_date', e.start_date,
            'end_date', e.end_date,
            'duration_months', e.duration_months,
            'is_framework', e.framework_flag
        ) ORDER BY e.end_date) as contracts
    FROM expiring e
    GROUP BY e.buyer_id, e.cpv_cluster, e.country
)
SELECT jsonb_build_object(
    'success', true,
    'generated_at', now(),
    'window_months', p_months_ahead,
    'total_signals', (SELECT COUNT(*) FROM rs),
    'signals', COALESCE((
        SELECT jsonb_agg(s ORDER BY (s->>'confidence')::numeric DESC, (s->>'days_until_expiry')::int ASC)
        FROM (
            SELECT jsonb_build_object(
                'signal_type', r.sig_type,
                'buyer_id', r.buyer_id,
                'buyer_name', r.buyer_name,
                'cpv_cluster', r.cpv_cluster,
                'country', r.country,
                'confidence', r.confidence,
                'urgency', r.urgency,
                'days_until_expiry', r.days_exp,
                'predicted_tender_date', r.pred_date,
                'latest_contract_end', r.latest_end_date,
                'total_contracts', r.total_contracts,
                'expiring_count', r.expiring_count,
                'avg_duration_months', ROUND(COALESCE(r.avg_dur, 0)::numeric, 1),
                'total_value_eur', ROUND(COALESCE(r.total_val, 0)::numeric, 2),
                'avg_value_eur', ROUND(COALESCE(r.avg_val, 0)::numeric, 2),
                'distinct_suppliers', r.dist_suppliers,
                'incumbent_suppliers', r.suppliers,
                'has_frameworks', r.has_fw,
                'expiring_contracts', e.contracts
            ) as s
            FROM rs r
            LEFT JOIN ed e ON e.buyer_id = r.buyer_id
                AND e.cpv_cluster = r.cpv_cluster
                AND e.country = r.country
            LIMIT 5000
        ) sub
    ), '[]'::jsonb),
    'summary', jsonb_build_object(
        'imminent', (SELECT COUNT(*) FROM rs WHERE urgency = 'imminent'),
        'upcoming', (SELECT COUNT(*) FROM rs WHERE urgency = 'upcoming'),
        'horizon', (SELECT COUNT(*) FROM rs WHERE urgency = 'horizon'),
        'distant', (SELECT COUNT(*) FROM rs WHERE urgency = 'distant'),
        'framework_expiries', (SELECT COUNT(*) FROM rs WHERE sig_type = 'framework_expiry'),
        'repeat_cycles', (SELECT COUNT(*) FROM rs WHERE sig_type = 'repeat_buyer_cycle'),
        'contract_renewals', (SELECT COUNT(*) FROM rs WHERE sig_type = 'contract_renewal'),
        'total_value_at_risk', (SELECT ROUND(COALESCE(SUM(total_val), 0)::numeric, 2) FROM rs),
        'avg_confidence', (SELECT ROUND(AVG(confidence)::numeric, 3) FROM rs),
        'by_country', jsonb_build_object(
            'IE', (SELECT COUNT(*) FROM rs WHERE country = 'IE'),
            'ES', (SELECT COUNT(*) FROM rs WHERE country = 'ES')
        )
    )
) INTO v_result;

RETURN v_result;
END;
$fn$;
