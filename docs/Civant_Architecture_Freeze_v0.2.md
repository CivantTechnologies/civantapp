# Civant Architecture Freeze v0.2

Status: `FROZEN`
Effective date: `2026-02-11`
Baseline spec: `docs/Civant_Technical_Spec_v0.2.docx`
Baseline text source: `docs/Civant_Technical_Spec_v0.2.txt`

## Purpose
This document freezes the Civant v0.2 architecture so implementation can proceed without design drift, uncontrolled schema changes, or operational regressions.

## Frozen Scope (Do Not Change Without Approval)
1. Multi-tenant isolation model (`tenant_id` everywhere relevant + backend role checks + RLS policies).
2. Core data layers and flow:
   - Raw (`raw_documents`)
   - Canonical (`canonical_tenders`, `TendersCurrent`, entities)
   - Intelligence (`tender_features_weekly`, `predictions`, `market_signals`)
3. Ingestion operating model:
   - Historical backfill = local-first (download/parse/normalize locally, then upload idempotently).
   - Incremental freshness = source polling with cursor/dedupe.
4. Idempotent write policy for high-volume tables:
   - `raw_documents` conflict-safe by `checksum`
   - `canonical_tenders` upsert by `canonical_id`
   - `TendersCurrent` upsert by `tender_id`
5. Confidence framework:
   - `overall_confidence = data(0-40) + signal(0-30) + model(0-30)`
6. Agent guardrails:
   - Structured outputs only
   - Fail closed on invalid schema
   - Auto-apply threshold >= `0.85`, otherwise review queue
7. Production deployment stack:
   - Vite React frontend on Vercel
   - Node/TS API routes on Vercel
   - Supabase Postgres/Auth/Storage

## Allowed Changes Under Freeze (No Design Review Required)
1. Bug fixes that do not alter frozen contracts.
2. Performance optimizations that preserve behavior and data contracts.
3. UI polish aligned with Civant brand tokens without routing/logic changes.
4. Ingestion reliability tuning within existing controls (timeouts, retries, watchdog thresholds).
5. Additive non-breaking indexes, observability fields, and diagnostics.

## Changes Requiring Architecture Approval
1. Any schema change that breaks existing tables/contracts.
2. Changes to tenant security model, auth model, or RLS posture.
3. Changes to confidence formula or score semantics.
4. Changes to ingestion write semantics that remove idempotency guarantees.
5. New AI-agent classes or relaxed guardrails.

## Change Control Protocol
Use a lightweight architecture change request:

- ID format: `CR-v0.2-XXX`
- Required fields:
  - Problem statement
  - Proposed change
  - Risk assessment
  - Rollback plan
  - Migration impact
  - Cost impact
  - Security impact

If approved, update:
1. `docs/Civant_Technical_Spec_v0.2.txt`
2. `docs/Civant_Technical_Spec_v0.2.docx`
3. This freeze doc status/date/version marker

## Operational Defaults During Freeze
Current ingestion defaults (Spain historical blocks):
1. `batch-size = 120`
2. Stall warning = `3 min` no progress
3. Auto-stop = `10 min` no progress
4. Force-kill grace = `2 min`
5. Health report cadence = `5 min`

## Exit Criteria To Unfreeze (v0.3 Planning Gate)
1. Spain historical backfill complete with reconciliation report.
2. IE/FR/ES incremental freshness jobs stable.
3. End-to-end prediction confidence reporting validated.
4. Security review confirms RLS/role policy coverage.
5. Advisor review completed for v0.3 scope.

## Sign-off
- Founder: Pending
- Technical Advisor: Pending
- Data Advisor: Pending

