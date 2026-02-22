# Pilot Ops Playbook

## Scope
This playbook defines the operational checks and triage flow for pilot customers.

## Automated monitors

### 1) PR Quality Gate
Workflow: `.github/workflows/pr-quality-gate.yml`

Runs on pull requests and enforces:
- `npm run lint`
- `npm run typecheck`
- `npm run typecheck:api`
- `npm run typecheck:deno`
- `npm test`
- `npm run build`

### 2) Pilot SLO Monitor
Workflow: `.github/workflows/pilot-slo-monitor.yml`

Runs every 30 minutes and executes `scripts/pilot-slo-monitor.sql`.

Thresholds:
- National connectors freshness: `<= 180 min`
- TED freshness: `<= 1560 min`
- Connector failures in 24h per connector: `<= 10`
- Predictive `signals_only` freshness: `<= 45 min`
- Predictive `incremental` freshness: `<= 180 min`
- Predictive `full` freshness: `<= 2160 min`
- Search RPC latency (`search_tenders_ranked` 20 rows): `<= 7000 ms` (initial pilot baseline)

Optional Slack alert:
- Set `SLACK_WEBHOOK_URL` GitHub secret to receive failure messages.

## Incident triage

### A. Connector freshness/failure breach
1. Open Actions logs for failing connector prefix and tenant.
2. Re-run failed connector workflow manually with same tenant.
3. If repeated failures, reduce `max_pages` and re-run.
4. Validate `ConnectorRuns` and cursor values in `ConnectorConfig`.

### B. Prediction freshness breach
1. Check `prediction_runs` for failed statuses and error text.
2. Manually dispatch `predictive-signals-v1` then `predictive-engine-incremental-v1`.
3. If still stale, dispatch nightly full compute and capture logs.

### C. Search latency breach
1. Run DB guardrails (`db-guardrails-stop-bleeding-canary.yml`).
2. Verify serving mode from `search_tenders_ranked` engine marker.
3. Confirm no recent schema/index regressions in search-serving path.

## Escalation
- Class A (tenant/data integrity): immediate stop-the-line and rollback.
- Class B (freshness/latency): mitigate within same day.
- Class C (UI-only): queue to next patch window.
