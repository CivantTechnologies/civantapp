# CIVANT PLATFORM — SESSION CONTINUITY CONTEXT
# Last updated: 24 February 2026 (end of Session 5)
# Purpose: Paste this at the start of each new session so Claude has full context

---

## WHO / WHAT

David Manrique is the founder and CTO of Civant Technologies, a procurement intelligence platform providing predictive insights for public procurement across Ireland, France, and Spain. The platform predicts tender opportunities before publication using contract renewal signals and historical award patterns.

David works from macOS Terminal (zsh), uses React/Vite frontend, Supabase PostgreSQL backend, Deno edge functions, and Vercel deployment.

---

## TECH STACK

| Layer | Technology |
|-------|-----------|
| Frontend | React 18 + Vite, Tailwind CSS, shadcn/ui components, Recharts |
| Backend | Supabase (PostgreSQL + Auth + Edge Functions + RLS) |
| AI Layer | Claude Haiku 4.5 via Anthropic API (web search enabled) |
| Hosting | Vercel (auto-deploy from GitHub main branch) |
| Repo | github.com/CivantTechnologies/civantapp |
| DB Project | ossoggqkqifdkihybbew.supabase.co |

### Key Environment Variables
- `SUPABASE_DB_URL` — direct Postgres connection string (used with psql)
- `VITE_SUPABASE_URL` — Supabase project URL
- `VITE_SUPABASE_ANON_KEY` — public anon key
- `ANTHROPIC_API_KEY` — set as Supabase secret for edge functions

### Common Commands
```bash
cd ~/projects/Civant
npx vite build 2>&1 | tail -5          # Build check
git add . && git commit -m "msg" && git push  # Deploy via Vercel
supabase functions deploy research-buyer --no-verify-jwt  # Edge function deploy
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -c "SQL"  # Direct DB query
```

### Known Gotchas
- Terminal heredocs with backticks/template literals get mangled by zsh — always write Python scripts to files first, then run them
- Supabase edge functions need `--no-verify-jwt` flag
- PostgREST default limit is 1000 rows — use `.limit(N)` or HEAD range header for larger fetches
- RLS policies block direct queries — use SECURITY DEFINER RPCs for cross-tenant data

---

## PROJECT STRUCTURE (key files)

```
src/
├── pages/
│   ├── Home.jsx              # Dashboard with scope toggle, stats cards, trajectory
│   ├── Predictions.jsx       # Forecast page — priority list, forecast list, agent, timeline chart
│   ├── Competitors.jsx       # Competitor profiles — dossier, accounts, contracts, agent
│   ├── Search.jsx            # Tender search
│   └── Onboarding.jsx        # 3-step wizard (company, preferences, billing)
├── components/
│   ├── forecast/
│   │   └── ForecastTimeline.jsx  # Monthly bar chart (replaced dot scatter)
│   ├── competitors/
│   │   ├── CompetitiveExposureSnapshot.jsx
│   │   └── RenewalExposureTimeline.jsx
│   └── home/
│       └── IntelligenceTrajectorySection.jsx
├── lib/
│   ├── supabaseClient.js     # Supabase client init
│   └── tenant.js             # useTenant() hook — provides activeTenantId
└── api/
    └── civantClient.js       # API wrapper

supabase/
├── functions/
│   └── research-buyer/
│       └── index.ts          # Civant Agent edge function (multi-context)
└── config.toml               # JWT verification disabled for research-buyer
```

---

## DATABASE SCHEMA (key tables)

### predictions
Core prediction table — ~13,750 rows across IE, ES, FR
```
id, tenant_id, buyer_name, country, predicted_tender_date, signal_type,
confidence_score, urgency_tier (overdue|upcoming|horizon|distant),
priority_score, category, cpv_division, source_contract_id,
validation_status (confirmed|unmatched|expired|pending),
validated_at, matched_tender_id, matched_tender_title,
matched_publication_date, match_delta_days, validation_method
```

### buyer_research_briefs
Civant Agent cache — briefs keyed by buyer + country + category(context)
```
id (text, brief_xxxx format), tenant_id, buyer_name, country,
category (stores context: 'forecast' | 'competitor'),
summary (text), procurement_intent (jsonb), organizational_context (jsonb),
incumbent_landscape (jsonb), risk_factors (jsonb),
opportunity_score (integer), sources (jsonb),
model_used, tokens_used, research_cost_usd,
researched_at, expires_at (7-day TTL), status,
created_at, updated_at
```

### tracked_competitors
User-tracked competitors
```
id (uuid), tenant_id, company_name, country, website,
search_terms, industry_sectors, notes, active, tracked_by_user
```
**NOTE: The field is `company_name`, NOT `name` or `supplier_name`**

### canonical_tenders
Published tenders from IE, ES, FR portals
```
id, tenant_id, title, buyer_name_raw, buyer_entity_id,
country, publication_date, deadline_date, estimated_value_eur,
cpv_code, status, source_portal
```

### award_fact_ie / award_fact_es / award_fact_fr
Historical contract awards (source data for predictions)
```
buyer_name, supplier_name, contract_title, award_date,
end_date, value_eur, cpv_code, framework_flag, country
```

### company_profiles
Tenant company profiles (from onboarding)
```
tenant_id, company_name, country, categories (jsonb),
keywords (jsonb), target_countries (jsonb)
```

### Key RPCs
- `get_prediction_validation_stats(p_tenant_id)` — returns accuracy breakdown
- `get_buyer_brief(tenant_id, buyer_name, country)` — cached brief lookup
- `analyze_competitor_intelligence(...)` — competitor dossier generation

---

## CIVANT AGENT — CURRENT STATE

### Architecture
- **One edge function** (`research-buyer/index.ts`) with `buildPrompts(context, ...)` switching prompts
- **Two live contexts**: `forecast` (buyer procurement intent) and `competitor` (trajectory/threats)
- **3-strategy JSON extractor**: fence match → summary-based brace match → first-to-last brace
- **Caching**: 7-day TTL in `buyer_research_briefs`, keyed by buyer_name + country + category
- **Cost**: ~$0.005/brief using Claude Haiku 4.5 with web search

### Forecast Page Integration
- Agent button on every row (priority + forecast list)
- Brief displays inline below row: summary, opportunity score (hidden if fallback 50), intent confidence, sources
- Title: "Civant Agent's Brief"

### Competitors Page Integration  
- Agent button on Overview tab ("Analyze Competitor")
- Brief panel shows: summary, trajectory (↑Growing/→Stable/↓Declining), threat level, risk factors
- Props passed from main `Competitors` component to `CompetitorDossier` child: `researchCompetitor`, `agentBrief`, `agentLoading`

---

## PREDICTION VALIDATION — CURRENT METRICS

| Metric | Value |
|--------|-------|
| Upcoming window (60d) | **80.8%** hit rate |
| Spain resolved | **95.2%** |
| Ireland resolved | **93.6%** |
| France resolved | 45.2% (name matching gaps) |
| Median timing delta | ±33 days |
| Total predictions | 13,753 |
| Confirmed | 6,086 |
| Pending | 3,191 |

---

## FORECAST PAGE — CURRENT STATE

- **Priority section**: Top opportunities sorted by priority_score, 5 per page, Previous/Next pagination
- **Forecast list**: All actionable predictions, 10 per page, clickable page numbers with ellipsis
- **Filters**: Urgency tier, country, signal type, scope match toggle
- **Default view**: "Actionable Forecasts" (excludes unverified overdue)
- **Timeline chart**: Monthly forecast pipeline bar chart (ForecastTimeline.jsx)
- **Validation section**: Accuracy metrics between stat cards and priority list

---

## HOME PAGE — CURRENT STATE

- Scope toggle (on/off) filters all dashboard data between scoped (matching company profile) and unscoped
- Stats cards: New tenders 24h, Active predictions, Tracked competitors, Validation accuracy
- Intelligence Trajectory section with trajectory charts
- Latest tenders list
- **Known issue**: "Incumbent Dominance Shift -27.0pp" and other trajectory metrics need review

---

## COMPETITORS PAGE — CURRENT STATE

- List view with search
- Detail view (CompetitorDossier) with tabs: Overview, Accounts, Contracts
- Overview: stats cards, strategic insights, Civant Agent panel, strengths, preferred categories
- Auto-analysis on competitor selection via `analyzeCompetitor` useCallback
- Scope toggle (group vs individual entity matching)

---

## PENDING ITEMS / NEXT PRIORITIES

### Immediate (Next Sprint)
1. **French name matching** — Improve buyer_name normalization to increase FR validation from 45.2%
2. **Nightly batch enrichment** — Scheduled job to pre-populate agent briefs for upcoming predictions using Batch API (50% discount)
3. **Home page metrics review** — "Incumbent Dominance Shift" and trajectory metrics may be inaccurate
4. **Validation automation** — Schedule daily validation runs

### Medium-term
5. **Opportunity context** — Extend agent for specific tender analysis (when user clicks into a tender)
6. **Competitor Accounts tab** — Per-account displacement opportunity insights from agent
7. **Competitor Contracts tab** — Vulnerability flags (near-expiry, low renewal rate)
8. **Agent stats enrichment** — Pass full competitor stats to agent (currently simplified to just name/country)

### Longer-term (see Roadmap doc)
9. Daily briefing context / email alerts
10. Competitor threat comparison (head-to-head)
11. Brief versioning and trend tracking
12. Strategic advisor conversational mode

---

## SESSION HISTORY

| Session | Date | Focus |
|---------|------|-------|
| 1 | 22 Feb | Forecast UI wiring, contract renewal predictions |
| 2 | 23 Feb | Unified forecast UI, SQL functions, data population |
| 3 | 23-24 Feb | Bug fixes (urgency tiers, onboarding gate, competitor RPC) |
| 4 | 24 Feb (AM) | French predictions, forecast UX refinements, sprint report |
| 5 | 24 Feb (PM) | Prediction validation, Home scope filtering, **Civant Agent build** (forecast + competitors), forecast UX redesign |

### Transcripts Location
`/mnt/transcripts/journal.txt` — catalog of all session transcripts

---

## HOW TO START A NEW SESSION

Paste this message:
```
I'm continuing Civant development. The context document is at /mnt/transcripts/civant_context.md — please read it first. Then let's [describe what you want to do].
```

If using Claude Code instead of chat:
```
Read /mnt/transcripts/civant_context.md for full project context. Then [task].
```

At session start, run these to verify current state:
```bash
cd ~/projects/Civant
git log --oneline -5
wc -l src/pages/Predictions.jsx src/pages/Competitors.jsx src/pages/Home.jsx
npx vite build 2>&1 | tail -3
```
