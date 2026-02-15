# CIVANT AGENTS GUIDE
Last updated: 2026-02-15

This document defines how AI agents (Atlas, Codex, future agents)
must behave when working on Civant.

---

## 🎯 SYSTEM PURPOSE

Civant predicts public-sector procurement by combining:

1. Historical tender cycles
2. Award outcomes
3. External signals (hiring, funding, policy, budgets)
4. Market intelligence

Agents MUST preserve data integrity and multi-tenant safety.

---

## 🧱 CORE ARCHITECTURE PRINCIPLES

### Multi-tenant safety
- Every table must include `tenant_id`
- Never bypass RLS
- Never query across tenants

### Canonical data protection
The following tables are **read-only**:

- canonical_tenders
- award_fact_ie
- award_signals_ie

Agents must NEVER modify canonical tables.

Additive tables only.

---

## 📊 SIGNAL ENGINE DESIGN

Signals are additive intelligence inputs.

### Internal signals
Derived from:

- awards
- buyer cycles
- CPV clustering

### External signals
Stored in:

external_signals_ie

Types:
- hiring
- funding
- policy (future)
- budget (future)

External signals must be:

✔ idempotent  
✔ evidence-backed  
✔ replay-safe  

---

## 🧠 AGENT DEVELOPMENT RULES

### Always:

✔ create migrations, never inline schema edits  
✔ write idempotent SQL  
✔ include QA scripts  
✔ include indexes for query paths  
✔ protect tenant isolation  

### Never:

✖ modify canonical tables  
✖ hardcode tenant IDs  
✖ bypass RLS  
✖ add scoring logic inside ingestion pipelines  

---

## ⚙️ SIGNAL INGESTION PRINCIPLES

Signals must include:

- source_event_id (unique per source)
- evidence_url
- signal_date
- raw_payload
- extracted_entities (structured JSON)

---

## 🧪 QA & VALIDATION

Every new data feature must include:

• coverage checks  
• null-rate checks  
• duplication checks  
• explainability outputs  

---

## 🚀 FUTURE AGENTS

Upcoming agents will handle:

- hiring signal ingestion
- funding signal ingestion
- policy monitoring
- buyer entity resolution
- predictive scoring engine

Agents must remain modular.

---

## 🛑 SAFETY RULE

If an agent is unsure whether a change could affect canonical integrity:

→ STOP  
→ ASK FOR HUMAN CONFIRMATION  

---

Civant is a **prediction intelligence platform**, not a scraper.

Accuracy > volume  
Signals > noise  
Evidence > assumptions  

