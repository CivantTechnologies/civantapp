# Civant Security Best Practices Report
Date: 2026-03-01
Scope: Frontend React/Vite app (`src/`), server handlers (`api/`, `functions/`), migrations (`database/migrations/`), dependency graph (`npm audit`)

## Executive Summary
Security posture is mixed: tenant-guard patterns are broadly present and internal endpoint checks are implemented, but there are several high-impact risks around token storage, canonical data governance, and unsafe link handling. Dependency audit also reports unresolved high-severity vulnerabilities.

## Critical Findings

### SEC-001: Canonical read-only policy is violated by runtime pipeline writes
- Severity: Critical
- Location:
  - `functions/pipeline/models.ts:93`
  - `functions/pipeline/steps.ts:317`
  - `functions/pipeline/steps.ts:348`
  - `functions/pipeline/steps.ts:350`
- Evidence:
  - Pipeline maps `canonicalTenders` to `canonical_tenders`.
  - `normalise_to_canonical()` performs `filter`, `update`, and `create` on `canonical_tenders`.
- Impact:
  - Breaks canonical immutability and increases risk of corruption/regression in baseline procurement records.
- Fix:
  - Move writes to additive, non-canonical staging/output tables; keep canonical tables read-only from agentic paths.
- Mitigation:
  - Add guard tests that fail if any function writes to `canonical_tenders`, `award_fact_ie`, or `award_signals_ie`.

## High Findings

### SEC-002: Access tokens are persisted in `localStorage` and hydrated from URL parameters
- Severity: High
- Location:
  - `src/lib/app-params.js:67`
  - `src/lib/app-params.js:40`
  - `src/lib/civant-sdk.js:370`
  - `src/lib/civant-sdk.js:372`
  - `src/api/civantClient.js:7`
- Evidence:
  - `access_token` is read from URL and stored in browser storage.
  - Auth tokens are persisted in and read from `localStorage`.
- Impact:
  - Any XSS can exfiltrate bearer tokens; URL token handling increases accidental leak surface.
- Fix:
  - Use secure, httpOnly, sameSite cookies for session tokens; avoid long-lived tokens in JS-accessible storage.
- Mitigation:
  - Short token TTL + rotation, strict CSP, and token audience scoping.

### SEC-003: High-severity vulnerable transitive packages in dependency audit
- Severity: High
- Location: `package-lock.json` dependency graph (`npm audit --json`)
- Evidence:
  - `minimatch` high ReDoS advisories.
  - `rollup` high path traversal advisory (`<4.59.0`).
- Impact:
  - Build-time and potentially runtime risk depending on exploit path and environment.
- Fix:
  - Run controlled upgrades (`npm audit fix` / explicit version bumps), retest build and CI.
- Mitigation:
  - Add dependency policy gate in CI (block high/critical advisories unless exception-approved).

## Medium Findings

### SEC-004: External link opened with `window.open` without `noopener,noreferrer`
- Severity: Medium
- Location: `src/pages/Insights.jsx:207`
- Evidence:
  - `window.open(response.data.file_link, '_blank');`
- Impact:
  - Reverse tabnabbing risk and unsafe opener relationship for externally controlled URLs.
- Fix:
  - Use `window.open(url, '_blank', 'noopener,noreferrer')` and/or sanitize/allowlist host.

### SEC-005: Raw HTML email body interpolates unescaped tender data
- Severity: Medium
- Location:
  - `functions/processAlerts.ts:148`
  - `functions/processAlerts.ts:160`
- Evidence:
  - HTML email template interpolates title/buyer/url fields directly.
- Impact:
  - HTML/link injection risk in email clients and trust/phishing abuse surface.
- Fix:
  - Escape HTML entities for untrusted fields and validate URL scheme/host.

### SEC-006: Mermaid is loaded from CDN without integrity and rendered in loose mode
- Severity: Medium
- Location:
  - `src/pages/Architecture.jsx:9`
  - `src/pages/Architecture.jsx:266`
  - `src/pages/Architecture.jsx:320`
- Evidence:
  - Remote script from jsdelivr with no SRI.
  - `securityLevel: 'loose'` plus `dangerouslySetInnerHTML` rendering path.
- Impact:
  - Supply-chain and HTML injection blast radius if input ceases to be fully static.
- Fix:
  - Self-host/pin assets, add SRI, and use stricter Mermaid security settings where possible.

## Low Findings

### SEC-007: Hardcoded default tenant fallback appears in auth/bootstrap and UI recovery paths
- Severity: Low
- Location:
  - `api/_lib/auth.ts:87`
  - `api/_lib/auth.ts:129`
  - `src/components/AppErrorBoundary.jsx:37`
- Evidence:
  - Explicit `civant_default` fallback/set behavior.
- Impact:
  - Potential policy drift against strict tenant isolation expectations.
- Fix:
  - Centralize tenant bootstrap policy and disallow implicit fallback in client-side recovery paths.

## Positive Controls Observed
- Tenant ID is validated from header in edge/server functions (`functions/getTenantFromHeader.ts:9`, `api/_lib/auth.ts:67`).
- Internal-only endpoints enforce token checks (`functions/internalOnly.ts:40`, `api/_lib/internal.ts:34`).
- RLS hardening exists for sensitive tables (`database/migrations/20260225_db_security_lockdown_awards_support_access_v1.sql:34`).

## Recommended Next Sequence
1. Fix token storage model (`SEC-002`) and add CSP/header hardening.
2. Enforce canonical write protections (`SEC-001`) in code + tests.
3. Remediate dependency vulnerabilities (`SEC-003`) with CI guard.
4. Patch medium findings (`SEC-004`, `SEC-005`, `SEC-006`) in one hardening PR.
