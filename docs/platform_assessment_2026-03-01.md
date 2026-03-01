# Civant Platform Assessment
Date: 2026-03-01
Scope: Security, functionality, bugs, scalability, UX/UI
Assessor: Codex

## 1) Verification Matrix

### Required checks
- `npm test`: **FAIL** (78 passed, 2 failed)
- `npm run lint`: **FAIL** (13 errors)
- `npm run typecheck`: **PASS**
- `npm run build`: **PASS**

### Extended checks
- `npm run typecheck:api`: **FAIL**
- `npm run typecheck:deno`: **PASS**
- `npm run mobile:typecheck`: **PASS**
- `npm audit --json`: **FAIL** (3 vulnerabilities: 2 high, 1 moderate)

## 2) Top Findings (Severity Ordered)

### Critical
1. Canonical governance breach: runtime pipeline writes to `canonical_tenders`.
   - Evidence: `functions/pipeline/steps.ts:317`, `functions/pipeline/steps.ts:348`, `functions/pipeline/steps.ts:350`

### High
1. CI health is red across core quality gates (tests, lint, api typecheck).
2. Token security model relies on `localStorage` + URL token ingestion.
   - Evidence: `src/lib/app-params.js:67`, `src/lib/civant-sdk.js:370`
3. High-severity dependency advisories (`minimatch`, `rollup`).

### Medium
1. Process alerts path has N+1 behavior and sequential side effects.
   - Evidence: `functions/processAlerts.ts:69`, `functions/processAlerts.ts:122`, `functions/processAlerts.ts:128`
2. Connector scheduler loads and iterates all configs in one request, serially.
   - Evidence: `functions/scheduledConnectorSync.ts:100`, `functions/scheduledConnectorSync.ts:104`
3. UX runtime degradation under local dev route execution due missing API handlers/proxy (`404` loops).
   - Evidence: `.playwright-cli/network-2026-03-01T15-27-38-413Z.log`
4. External link safety gap (`window.open` without `noopener,noreferrer`).
   - Evidence: `src/pages/Insights.jsx:207`

### Low
1. Tenant UI select receives null value in some states (React warning).
   - Evidence: `src/Layout.jsx:585`, `src/lib/tenant.jsx:63`
2. Onboarding provider blocks render while loading, contrary to current guard expectations.
   - Evidence: `src/lib/OnboardingGate.jsx:40`, `tests/navigationPerformanceGuards.test.js:19`

## 3) Functionality & Bug Detail

### Failing tests
- `tests/navigationPerformanceGuards.test.js:19`
  - Regex expects `onboardingStatus` pattern and no render blocking; current implementation still blocks with `status === 'loading' ? null : children`.
- `tests/processAlertsTenantGuards.test.js:7`
  - Regex expects tender query shape without `first_seen_at` predicate; function now includes that filter.

### Lint failures
- Hook rule violation (real runtime risk):
  - `src/components/tender/EnrichmentPanel.jsx:16`
- Unused imports (quality/maintainability):
  - `src/pages/Competitors.jsx:7`
  - `src/pages/Home.jsx:15`
  - `src/pages/Reports.jsx` (multiple)
  - `src/pages/Search.jsx:1`

### API typecheck failures
- Excessive type instantiation + insert overload mismatches:
  - `api/_lib/auth.ts:338`
  - `api/_lib/handlers.ts:1071`
  - `api/_lib/handlers.ts:1084`
  - `api/_lib/handlers.ts:1108`

## 4) Scalability Assessment

### Risks
1. `processAlerts` scales as alerts × tenders × per-match DB query pattern.
   - Potential high DB load and latency amplification under growth.
2. `build_weekly_features` loads up to 10,000 canonical rows into memory per run.
   - Evidence: `functions/pipeline/steps.ts:360`
3. `scheduledConnectorSync` executes connectors serially in one request.
   - Throughput bottleneck as tenant/connector count grows.

### Data/index notes
- Query path in alerts uses `tenant_id` + `first_seen_at` predicate, but observed explicit index coverage is mainly `tenant_id,published_at`.
  - Evidence: `database/migrations/20260215_tenderscurrent_perf_indexes.sql:7`

## 5) UX/UI Assessment (Playwright)

### What worked
- Login page renders cleanly on desktop and mobile.
  - Artifacts:
    - `.playwright-cli/page-2026-03-01T15-29-24-510Z.png`
    - `.playwright-cli/page-2026-03-01T15-29-40-475Z.png`
- Client-side validation for empty credentials and invalid credentials displays user-facing messages.

### UX issues observed
- Protected routes often stall at loading states when backend endpoints are unavailable in local Vite mode.
  - Spinner loops seen on `/company` and `/workbench/search` while `/api/apps/.../getMyProfile` returns 404.
- React warning for null `<select>` value on tenant switcher path.

## 6) Security Posture Summary
- Detailed report: `security_best_practices_report.md`
- Strong controls observed:
  - Tenant header derivation and validation (`functions/getTenantFromHeader.ts`, `api/_lib/auth.ts`)
  - Internal token gates for internal endpoints (`functions/internalOnly.ts`, `api/_lib/internal.ts`)
  - RLS lockdown migration for sensitive tables (`20260225_db_security_lockdown_awards_support_access_v1.sql`)
- Highest-priority gaps:
  - Canonical table write policy mismatch
  - Browser token persistence model
  - Dependency advisories

## 7) Overall Scorecard (Current State)
- Security: **6/10** (good tenancy controls, but high-impact auth/storage and governance risks remain)
- Functionality: **5/10** (core app builds, but core validation suite not green)
- Bug Health: **4/10** (multiple failing quality gates)
- Scalability: **5/10** (works for moderate load; N+1/serial bottlenecks likely under growth)
- UX/UI: **7/10** for login flow; **4/10** for authenticated-route resiliency in local runtime setup

## 8) Immediate Priority Order
1. Restore green quality gates (tests, lint, api typecheck).
2. Fix token handling model and link safety hardening.
3. Remove canonical writes from runtime pipeline paths.
4. Refactor `processAlerts` and scheduler flows for batch/paginated operation.
5. Add/validate index support for high-frequency filtered queries.
