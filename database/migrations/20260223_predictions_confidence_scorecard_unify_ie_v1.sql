-- =============================================================================
-- Civant: IE confidence/scorecard unification (dual-write bridge)
-- Migration: 20260223_predictions_confidence_scorecard_unify_ie_v1.sql
-- =============================================================================
-- Goal:
--   Keep existing prediction confidence computation in place, but ensure
--   predictions_current.confidence matches prediction_scorecard.total_score
--   for civant_default + IE during rollout.
--
-- Safety:
--   - Strictly scoped to tenant_id='civant_default' and region='IE'
--   - Idempotent trigger creation
--   - Backfill only touches rows with a matching scorecard row
-- =============================================================================

create or replace function public.sync_ie_confidence_from_scorecard_v1()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_confidence_band text;
begin
  if new.tenant_id <> 'civant_default' then
    return new;
  end if;

  v_confidence_band := case
    when new.total_score <= 39 then 'Low'
    when new.total_score <= 59 then 'Medium'
    when new.total_score <= 79 then 'High'
    else 'Very High'
  end;

  -- Dual-write bridge: run keeps writing legacy confidence first, then this
  -- trigger aligns confidence/band to the persisted scorecard total.
  update public.predictions_current pc
  set
    confidence = new.total_score,
    confidence_band = v_confidence_band
  where pc.tenant_id = new.tenant_id
    and pc.prediction_id = new.prediction_id
    and pc.region = 'IE'
    and (
      pc.confidence is distinct from new.total_score
      or pc.confidence_band is distinct from v_confidence_band
    );

  return new;
end;
$$;

drop trigger if exists trg_sync_ie_confidence_from_scorecard_v1 on public.prediction_scorecard;

create trigger trg_sync_ie_confidence_from_scorecard_v1
after insert or update of total_score
on public.prediction_scorecard
for each row
execute function public.sync_ie_confidence_from_scorecard_v1();

-- Backfill existing IE rows for civant_default.
with scored as (
  select
    ps.tenant_id,
    ps.prediction_id,
    ps.total_score,
    case
      when ps.total_score <= 39 then 'Low'
      when ps.total_score <= 59 then 'Medium'
      when ps.total_score <= 79 then 'High'
      else 'Very High'
    end as confidence_band
  from public.prediction_scorecard ps
  where ps.tenant_id = 'civant_default'
)
update public.predictions_current pc
set
  confidence = scored.total_score,
  confidence_band = scored.confidence_band
from scored
where pc.tenant_id = 'civant_default'
  and pc.region = 'IE'
  and pc.tenant_id = scored.tenant_id
  and pc.prediction_id = scored.prediction_id
  and (
    pc.confidence is distinct from scored.total_score
    or pc.confidence_band is distinct from scored.confidence_band
  );
