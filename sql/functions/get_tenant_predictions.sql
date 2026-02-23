CREATE OR REPLACE FUNCTION public.get_tenant_predictions(p_tenant_id text)
 RETURNS TABLE(
   id text, buyer_id text, category text, cpv_family text, time_window text,
   probability numeric, confidence numeric, confidence_breakdown jsonb,
   top_drivers jsonb, evidence jsonb, model_version text, generated_at timestamptz,
   tenant_id text, predicted_tender_date date, signal_type text, renewal_source jsonb,
   urgency text, buyer_name text, country text, total_value_eur numeric
 )
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $$
begin
  return query
  select p.id, p.buyer_id, p.category, p.cpv_family, p.time_window, p.probability, p.confidence,
         p.confidence_breakdown, p.top_drivers, p.evidence, p.model_version, p.generated_at,
         p.tenant_id, p.predicted_tender_date, p.signal_type, p.renewal_source, p.urgency,
         p.buyer_name, p.country, p.total_value_eur
  from public.predictions p
  where p.tenant_id = lower(trim(p_tenant_id))
  order by
    case lower(coalesce(p.urgency, ''))
      when 'overdue' then 0
      when 'upcoming' then 1
      when 'horizon' then 2
      when 'distant' then 3
      else 4
    end,
    p.predicted_tender_date asc nulls last,
    p.probability desc
  limit 5000;
end;
$$;
