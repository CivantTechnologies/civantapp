# Civant iOS MVP (Monorepo)

## Goal
Ship a mobile-first iPhone workflow for daily opportunity triage with clear explainability and fast actions.

## Current Scope (apps/mobile)
- Expo + React Native scaffold
- Supabase auth session sign-in flow (mobile)
- `Today` mobile screen wired to live API
- iOS local notifications for high-signal opportunities (permission-gated, daily dedupe)
- Offline triage queue with retry/sync on reconnect
- Remote push subscription registration (`/api/mobile/push/register`)
- Internal remote push dispatcher (`/api/mobile/push/dispatch`)
- Critical alerts section
- Quick action grid
- Opportunity cards with triage actions (`Save`, `Ignore`, `Assign`)
- Mobile API routes:
  - `GET /api/mobile/today`
  - `POST /api/mobile/triage`

## Tenant Safety
- Mobile client sends only authenticated bearer tokens.
- `tenant_id` must never be accepted from request payload.
- Backend must derive tenant from auth/header context and enforce RLS.

## Remote Push Dispatch
- Registration endpoint: `POST /api/mobile/push/register` (authenticated)
- Dispatch endpoint: `POST /api/mobile/push/dispatch` (internal token only)
- Internal token envs accepted by dispatch route:
  - `CIVANT_INTERNAL_FUNCTION_TOKEN`
  - `CIVANT_INTERNAL_CRON_TOKEN`
  - `INTERNAL_FUNCTION_TOKEN`

## Next Build Steps
1. Add push notifications for high-signal alerts.
2. Add offline cache for watchlist and latest `Today` feed.
3. Add analytics and crash reporting for iOS production readiness.
4. Add mobile-specific onboarding + watchlist personalization.
5. Add TestFlight distribution pipeline and release gates.
