#!/usr/bin/env python3
"""
Civant Batch Buyer Enrichment
=============================
Pre-populates buyer intelligence briefs for all upcoming predictions
using the Anthropic Messages Batches API (50% cost discount).

Usage:
  # Dry-run: show what would be submitted
  python batch_enrich.py --dry-run

  # Submit batch for upcoming predictions only
  python batch_enrich.py

  # Submit batch for all upcoming + overdue
  python batch_enrich.py --include-overdue

  # Poll an existing batch
  python batch_enrich.py --poll <batch_id>

  # Download and ingest results from a completed batch
  python batch_enrich.py --ingest <batch_id>

Env vars required:
  ANTHROPIC_API_KEY
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
"""

import os
import sys
import json
import re
import time
import argparse
from datetime import datetime, timezone, timedelta

import anthropic
from supabase import create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 1500
TENANT_ID = "civant_default"
BRIEF_TTL_DAYS = 7

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

COUNTRY_NAMES = {"ES": "Spain", "FR": "France", "IE": "Ireland"}

# ---------------------------------------------------------------------------
# Step 1: Fetch unique buyers from predictions
# ---------------------------------------------------------------------------
def fetch_buyers(include_overdue=False):
    """Get unique buyers from predictions table that need briefs."""
    urgencies = ["upcoming"]
    if include_overdue:
        urgencies.append("overdue")

    # Query unique buyer/country pairs from predictions
    try:
        resp = supabase.rpc("get_batch_enrichment_buyers", {
            "p_tenant_id": TENANT_ID,
            "p_urgencies": urgencies,
        }).execute()
        if resp.data:
            return resp.data
    except Exception:
        pass

    # Fallback: direct query if RPC doesn't exist yet
    print("⚠ RPC not found, using direct query fallback...")
    all_buyers = []
    for urgency in urgencies:
        resp = supabase.table("predictions") \
            .select("buyer_name, country") \
            .eq("tenant_id", TENANT_ID) \
            .eq("urgency", urgency) \
            .in_("validation_status", ["pending", "confirmed"]) \
            .execute()
        if resp.data:
            all_buyers.extend(resp.data)

    # Deduplicate
    seen = set()
    unique = []
    for b in all_buyers:
        key = (b["buyer_name"], b["country"])
        if key not in seen:
            seen.add(key)
            unique.append(b)
    return unique


def filter_already_cached(buyers):
    """Remove buyers that already have a valid (non-expired) brief."""
    cutoff = datetime.now(timezone.utc).isoformat()
    cached_set = set()

    # Check in batches of 50
    for i in range(0, len(buyers), 50):
        batch = buyers[i:i+50]
        names = [b["buyer_name"] for b in batch]
        resp = supabase.table("buyer_research_briefs") \
            .select("buyer_name, country") \
            .eq("tenant_id", TENANT_ID) \
            .eq("category", "forecast") \
            .eq("status", "complete") \
            .gt("expires_at", cutoff) \
            .in_("buyer_name", names) \
            .execute()
        if resp.data:
            for row in resp.data:
                cached_set.add((row["buyer_name"], row["country"]))

    filtered = [b for b in buyers if (b["buyer_name"], b["country"]) not in cached_set]
    print(f"  {len(cached_set)} buyers already cached, {len(filtered)} need enrichment")
    return filtered


# ---------------------------------------------------------------------------
# Step 2: Fetch award history for each buyer
# ---------------------------------------------------------------------------
def fetch_award_history(buyer_name, country):
    """Call the get_buyer_award_history RPC for a single buyer."""
    try:
        resp = supabase.rpc("get_buyer_award_history", {
            "p_buyer_name": buyer_name,
            "p_country": country,
        }).execute()
        return resp.data if resp.data else None
    except Exception as e:
        print(f"  ⚠ Award history failed for {buyer_name}: {e}")
        return None


# ---------------------------------------------------------------------------
# Step 3: Build prompts (replicates edge function buildPrompts exactly)
# ---------------------------------------------------------------------------
def build_prompts(buyer_name, country, award_history=None, category=None):
    """
    Replicates the TypeScript buildPrompts() from the edge function.
    Returns (system_prompt, user_message) for the forecast context.
    """
    country_label = COUNTRY_NAMES.get(country, country)
    has_history = (
        award_history
        and award_history.get("stats", {}).get("total_contracts", 0) > 0
    )

    # --- System prompt ---
    history_note = "" if has_history else \
        "\nNOTE: No historical award data was found for this buyer. Rely on web research alone but note the data gap."

    system = (
        "You are Civant Agent, a procurement intelligence analyst with access to both "
        "historical contract award data and web research. Your job is to produce an actionable "
        "intelligence brief for a company deciding whether to pursue a predicted procurement opportunity.\n\n"
        "You have TWO information sources:\n"
        "1. CANONICAL DATA: Real contract award history from official procurement portals (provided below). "
        "This is factual and verified. Analyze it for: renewal cycles, spend trends, incumbent suppliers, "
        "typical contract durations, category patterns, and budget trajectory.\n"
        "2. WEB RESEARCH: Use your web search to find CURRENT intelligence: recent news, leadership changes, "
        "budget announcements, organizational restructuring, upcoming projects, and policy shifts.\n\n"
        "COMBINE both sources into a single coherent brief. Lead with data-backed insights (patterns from "
        "the award history), then layer on web intelligence. If the award history shows clear patterns "
        '(e.g. "renews IT services every 3 years at ~200k"), state them explicitly.\n'
        f"{history_note}\n"
        "Respond ONLY in JSON. Use this EXACT structure (do NOT add extra keys, do NOT nest differently):\n"
        '{"summary":"3-4 sentence executive summary",'
        '"procurement_patterns":{"renewal_cycle":"description","spend_trend":"increasing|stable|decreasing|insufficient_data",'
        '"typical_value_range":"e.g. 50k-200k","preferred_categories":["categories"],"notes":"other patterns"},'
        '"incumbent_landscape":{"known_suppliers":["suppliers"],"dominant_supplier":"name or null","contract_notes":"history"},'
        '"organizational_context":{"type":"municipality|health_authority|university|ministry|agency|school|other",'
        '"leadership":"name","recent_changes":"changes","size_indicator":"small|medium|large"},'
        '"risk_factors":["risks"],'
        '"timing_insight":"when to engage",'
        '"opportunity_score":75,'
        '"opportunity_reasoning":"why this score based on data",'
        '"intent_confidence":"high|medium|low",'
        '"intent_reasoning":"why this confidence level",'
        '"sources":[{"url":"url","title":"title","relevance":"why"}]}\n\n'
        "SCORING RULES for opportunity_score (integer 0-100):\n"
        "- 80-100: Clear renewal cycle approaching, strong spend history, open competition\n"
        "- 60-79: Good award history, moderate patterns, some incumbent lock-in but winnable\n"
        "- 40-59: Limited data or mixed signals, worth monitoring\n"
        "- 20-39: Sparse history, single low-value contract, or locked-in incumbent\n"
        "- 0-19: No meaningful data, speculative only\n\n"
        "INTENT CONFIDENCE based on DATA quality:\n"
        "- high: 3+ contracts with clear renewal cadence and approaching end date\n"
        "- medium: 1-2 contracts or irregular renewal pattern\n"
        "- low: No canonical award data, relying on web research alone\n\n"
        "CRITICAL: Return ONLY the raw JSON object. No markdown, no explanation, no preamble. Start with { end with }."
    )

    # --- User message ---
    parts = [
        "Research this public sector buyer for a predicted procurement opportunity:",
        f"Buyer: {buyer_name}",
        f"Country: {country_label}",
    ]
    if category:
        parts.append(f"Sector: {category}")

    if has_history:
        h = award_history
        s = h["stats"]
        parts.append("")
        parts.append("=== CANONICAL AWARD DATA (from official procurement portals) ===")
        parts.append(f"Total contracts on record: {s['total_contracts']}")
        parts.append(f"Unique suppliers: {s['unique_suppliers']}")
        parts.append(f"Total spend: EUR {int(float(s.get('total_spend') or 0)):,}")
        parts.append(f"Average contract value: EUR {int(float(s.get('avg_contract_value') or 0)):,}")
        parts.append(f"Max contract value: EUR {int(float(s.get('max_contract_value') or 0)):,}")
        parts.append(f"Award history span: {s.get('earliest_award', '?')} to {s.get('latest_award', '?')}")
        parts.append(f"Average contract duration: {s.get('avg_duration_months', '?')} months")
        parts.append(f"Framework agreements: {s.get('framework_count', 0)}")
        if s.get("cpv_clusters"):
            parts.append(f"Procurement categories: {', '.join(s['cpv_clusters'])}")

        if h.get("top_suppliers"):
            parts.append("")
            parts.append("Top suppliers:")
            for sup in h["top_suppliers"]:
                parts.append(
                    f"  - {sup['supplier']}: {sup['contracts']} contracts, "
                    f"EUR {int(float(sup.get('total_value') or 0)):,}, "
                    f"last award {sup.get('last_award', '?')}"
                )

        if h.get("renewal_patterns"):
            parts.append("")
            parts.append("Renewal patterns by category:")
            for rp in h["renewal_patterns"]:
                parts.append(
                    f"  - {rp['cpv_cluster']}: {rp['occurrences']} contracts, "
                    f"avg duration {rp.get('avg_duration', '?')} months, "
                    f"avg value EUR {int(float(rp.get('avg_value') or 0)):,}, "
                    f"last end date {rp.get('last_end_date', 'unknown')}"
                )

        if h.get("recent_contracts"):
            parts.append("")
            parts.append("Most recent contracts:")
            for rc in h["recent_contracts"][:5]:
                parts.append(
                    f"  - EUR {int(float(rc.get('value_eur', 0) or 0)):,} | "
                    f"{rc.get('supplier', 'unknown supplier')} | "
                    f"awarded {rc.get('award_date', '?')} | "
                    f"ends {rc.get('end_date', 'unknown')} | "
                    f"CPV {rc.get('cpv_primary', 'n/a')} | "
                    f"{rc.get('duration_months', '?')} months"
                )

        parts.append("")
        parts.append("=== END CANONICAL DATA ===")
    else:
        parts.append("")
        parts.append("No historical award data found for this buyer in our database.")

    parts.append("")
    parts.append(
        "Now use web search to find current intelligence about this buyer, "
        "then combine with the data above to produce the intelligence brief in JSON format."
    )

    return system, "\n".join(parts)


# ---------------------------------------------------------------------------
# Step 4: Build batch request JSONL
# ---------------------------------------------------------------------------
def build_batch_requests(buyers_with_history):
    """
    Build the list of batch request objects.
    Each entry: { custom_id, params: { model, max_tokens, system, messages, tools } }
    """
    requests = []
    id_map = {}
    for idx, item in enumerate(buyers_with_history):
        buyer_name = item["buyer_name"]
        country = item["country"]
        award_history = item.get("award_history")

        system, user_msg = build_prompts(buyer_name, country, award_history)

        custom_id = f"{country}_{idx:04d}"
        id_map[custom_id] = {"buyer_name": buyer_name, "country": country}

        requests.append({
            "custom_id": custom_id,
            "params": {
                "model": MODEL,
                "max_tokens": MAX_TOKENS,
                "system": system,
                "messages": [{"role": "user", "content": user_msg}],
                "tools": [{"type": "web_search_20250305", "name": "web_search"}],
            }
        })
    return requests, id_map


# ---------------------------------------------------------------------------
# Step 5: Submit batch
# ---------------------------------------------------------------------------
def submit_batch(requests):
    """Submit batch to Anthropic Messages Batches API."""
    print(f"\n📤 Submitting batch of {len(requests)} requests...")
    batch = client.messages.batches.create(requests=requests)
    print(f"✅ Batch created: {batch.id}")
    print(f"   Status: {batch.processing_status}")
    print(f"   Expires: {batch.expires_at}")
    return batch.id


# ---------------------------------------------------------------------------
# Step 6: Poll batch status
# ---------------------------------------------------------------------------
def poll_batch(batch_id, wait=True):
    """Check batch status, optionally polling until complete."""
    batch = client.messages.batches.retrieve(batch_id)
    counts = batch.request_counts
    print(f"\n📊 Batch {batch_id}")
    print(f"   Status: {batch.processing_status}")
    print(f"   Processing: {counts.processing}")
    print(f"   Succeeded:  {counts.succeeded}")
    print(f"   Errored:    {counts.errored}")
    print(f"   Canceled:   {counts.canceled}")
    print(f"   Expired:    {counts.expired}")

    if not wait:
        return batch

    while batch.processing_status != "ended":
        time.sleep(60)
        batch = client.messages.batches.retrieve(batch_id)
        counts = batch.request_counts
        now = datetime.now().strftime("%H:%M:%S")
        print(f"   [{now}] processing={counts.processing} succeeded={counts.succeeded} errored={counts.errored}")

    print(f"\n✅ Batch complete!")
    return batch


# ---------------------------------------------------------------------------
# Step 7: Robust JSON extraction (replicates edge function extractJson)
# ---------------------------------------------------------------------------
def extract_json(raw_text):
    """
    3-tier JSON extraction replicating the TypeScript extractJson().
    Returns a dict with at minimum a 'summary' key.
    """
    # Strip HTML/XML tags (cite tags from web search)
    cleaned = re.sub(r"<[^>]+>", "", raw_text)

    json_str = None

    # Strategy A: markdown code fence
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
    if fence:
        json_str = fence.group(1).strip()

    # Strategy B: outermost braces around "summary"
    if not json_str:
        si = cleaned.find('"summary"')
        if si > -1:
            bs = cleaned.rfind("{", 0, si)
            if bs > -1:
                depth = 0
                be = -1
                for i in range(bs, len(cleaned)):
                    if cleaned[i] == "{":
                        depth += 1
                    elif cleaned[i] == "}":
                        depth -= 1
                    if depth == 0:
                        be = i
                        break
                if be > bs:
                    json_str = cleaned[bs:be+1]

    # Strategy C: first { to last }
    if not json_str:
        f = cleaned.find("{")
        l = cleaned.rfind("}")
        if f > -1 and l > f:
            json_str = cleaned[f:l+1]

    if not json_str:
        return {"summary": cleaned[:400].strip(), "sources": []}

    # Clean for parsing
    json_str = json_str.replace("\n", " ").replace("\r", " ").replace("\t", " ")

    # Attempt 1: direct parse
    try:
        p = json.loads(json_str)
        if isinstance(p, dict) and p.get("summary"):
            return p
    except json.JSONDecodeError:
        pass

    # Attempt 2: fix trailing commas
    try:
        fixed = re.sub(r",\s*}", "}", json_str)
        fixed = re.sub(r",\s*]", "]", fixed)
        p2 = json.loads(fixed)
        if isinstance(p2, dict) and p2.get("summary"):
            return p2
    except json.JSONDecodeError:
        pass

    # Attempt 3: regex field extraction
    result = {"sources": []}

    fields = ["summary", "timing_insight", "opportunity_reasoning", "intent_confidence", "intent_reasoning"]
    for f in fields:
        m = re.search(r'"' + f + r'"\s*:\s*"((?:[^"\\]|\\.)*)"', cleaned)
        if m:
            result[f] = m.group(1).replace('\\"', '"').replace("\\n", " ")

    score_m = re.search(r'"opportunity_score"\s*:\s*(\d+)', cleaned)
    if score_m:
        result["opportunity_score"] = int(score_m.group(1))

    risk_m = re.search(r'"risk_factors"\s*:\s*(\[[^\]]*\])', cleaned)
    if risk_m:
        try:
            result["risk_factors"] = json.loads(risk_m.group(1).replace("\n", " "))
        except json.JSONDecodeError:
            pass

    # Nested objects
    for field in ["procurement_patterns", "incumbent_landscape", "organizational_context"]:
        fi = cleaned.find(f'"{field}"')
        if fi > -1:
            os_idx = cleaned.find("{", fi)
            if os_idx > -1 and os_idx < fi + len(field) + 10:
                depth = 0
                oe = -1
                for i in range(os_idx, len(cleaned)):
                    if cleaned[i] == "{":
                        depth += 1
                    elif cleaned[i] == "}":
                        depth -= 1
                    if depth == 0:
                        oe = i
                        break
                if oe > os_idx:
                    try:
                        result[field] = json.loads(cleaned[os_idx:oe+1].replace("\n", " "))
                    except json.JSONDecodeError:
                        pass

    # Sources array
    si2 = cleaned.rfind('"sources"')
    if si2 > -1:
        as_idx = cleaned.find("[", si2)
        if as_idx > -1:
            depth = 0
            ae = -1
            for i in range(as_idx, len(cleaned)):
                if cleaned[i] == "[":
                    depth += 1
                elif cleaned[i] == "]":
                    depth -= 1
                if depth == 0:
                    ae = i
                    break
            if ae > as_idx:
                try:
                    result["sources"] = json.loads(cleaned[as_idx:ae+1].replace("\n", " "))
                except json.JSONDecodeError:
                    pass

    return result


# ---------------------------------------------------------------------------
# Step 8: Ingest results → buyer_research_briefs
# ---------------------------------------------------------------------------
def ingest_results(batch_id):
    """Download batch results and upsert to buyer_research_briefs."""
    print(f"\n📥 Downloading results for batch {batch_id}...")

    # Load id_map
    import json as _json
    map_file = f"batch_{batch_id}_map.json"
    try:
        with open(map_file) as f:
            id_map = _json.load(f)
        print(f"  Loaded {len(id_map)} entries from {map_file}")
    except FileNotFoundError:
        print(f"  ❌ Map file not found: {map_file}")
        print(f"     Run from the same directory where you submitted.")
        return

    results = []
    succeeded = 0
    errored = 0
    skipped = 0

    for result in client.messages.batches.results(batch_id):
        custom_id = result.custom_id

        if result.result.type == "errored":
            errored += 1
            print(f"  ❌ {custom_id}: {result.result.error}")
            continue

        if result.result.type != "succeeded":
            skipped += 1
            continue

        message = result.result.message

        # Parse custom_id → country|buyer_name
        parts = custom_id.split("|", 1)
        if len(parts) != 2:
            print(f"  ⚠ Bad custom_id: {custom_id}")
            skipped += 1
            continue

        country, buyer_name = parts

        # Extract text from response
        text_blocks = [b.text for b in message.content if b.type == "text"]
        raw_text = "\n".join(text_blocks)

        brief = extract_json(raw_text)

        # Calculate cost
        usage = message.usage
        input_tokens = usage.input_tokens or 0
        output_tokens = usage.output_tokens or 0
        # Batch API: 50% discount → Haiku input $0.40/M, output $2.00/M (after discount)
        cost_usd = (input_tokens * 0.4 + output_tokens * 2.0) / 1_000_000
        # Add web search cost if applicable
        web_searches = 0
        if hasattr(usage, 'server_tool_use') and usage.server_tool_use:
            web_searches = getattr(usage.server_tool_use, 'web_search_requests', 0)
        cost_usd += web_searches * 0.01

        # Build procurement_intent JSONB (same as edge function)
        procurement_intent = {
            **(brief.get("procurement_patterns") or {}),
            "intent_confidence": brief.get("intent_confidence"),
            "intent_reasoning": brief.get("intent_reasoning"),
            "opportunity_reasoning": brief.get("opportunity_reasoning"),
            "timing_insight": brief.get("timing_insight"),
        }

        row = {
            "tenant_id": TENANT_ID,
            "buyer_name": buyer_name,
            "country": country,
            "category": "forecast",
            "summary": brief.get("summary"),
            "procurement_intent": procurement_intent,
            "organizational_context": brief.get("organizational_context"),
            "incumbent_landscape": brief.get("incumbent_landscape"),
            "risk_factors": brief.get("risk_factors"),
            "opportunity_score": brief.get("opportunity_score") if isinstance(brief.get("opportunity_score"), int) else None,
            "sources": brief.get("sources"),
            "model_used": "claude-haiku-4-5",
            "tokens_used": input_tokens + output_tokens,
            "research_cost_usd": round(cost_usd, 6),
            "status": "complete",
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=BRIEF_TTL_DAYS)).isoformat(),
        }

        results.append(row)
        succeeded += 1

    # Bulk upsert to Supabase
    print(f"\n📝 Upserting {len(results)} briefs to buyer_research_briefs...")
    batch_size = 50
    upserted = 0
    for i in range(0, len(results), batch_size):
        chunk = results[i:i+batch_size]
        try:
            resp = supabase.table("buyer_research_briefs").insert(chunk).execute()
            upserted += len(chunk)
        except Exception as e:
            # Try one by one on failure
            print(f"  ⚠ Batch insert failed, trying individually: {e}")
            for row in chunk:
                try:
                    supabase.table("buyer_research_briefs").insert(row).execute()
                    upserted += 1
                except Exception as e2:
                    print(f"  ❌ Failed: {row['buyer_name']}: {e2}")

    # Summary
    total_tokens = sum(r.get("tokens_used", 0) for r in results)
    total_cost = sum(r.get("research_cost_usd", 0) for r in results)
    scores = [r["opportunity_score"] for r in results if r.get("opportunity_score") is not None]
    avg_score = sum(scores) / len(scores) if scores else 0

    print(f"\n{'='*60}")
    print(f"✅ BATCH ENRICHMENT COMPLETE")
    print(f"{'='*60}")
    print(f"  Succeeded:  {succeeded}")
    print(f"  Errored:    {errored}")
    print(f"  Skipped:    {skipped}")
    print(f"  Upserted:   {upserted}")
    print(f"  Total tokens: {total_tokens:,}")
    print(f"  Total cost:   ${total_cost:.2f}")
    print(f"  Avg opp score: {avg_score:.1f}")
    if scores:
        print(f"  Score range:  {min(scores)}-{max(scores)}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Civant Batch Buyer Enrichment")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be submitted")
    parser.add_argument("--include-overdue", action="store_true", help="Include overdue predictions")
    parser.add_argument("--poll", metavar="BATCH_ID", help="Poll an existing batch")
    parser.add_argument("--ingest", metavar="BATCH_ID", help="Download and ingest results")
    parser.add_argument("--no-cache-check", action="store_true", help="Skip checking for existing briefs")
    parser.add_argument("--limit", type=int, help="Limit number of buyers to process")
    args = parser.parse_args()

    # --- Poll mode ---
    if args.poll:
        poll_batch(args.poll, wait=True)
        return

    # --- Ingest mode ---
    if args.ingest:
        ingest_results(args.ingest)
        return

    # --- Build & Submit mode ---
    print("🔍 Fetching unique buyers from predictions...")
    buyers = fetch_buyers(include_overdue=args.include_overdue)
    print(f"  Found {len(buyers)} unique buyer/country pairs")

    if not args.no_cache_check:
        print("\n🔍 Checking for existing cached briefs...")
        buyers = filter_already_cached(buyers)

    if args.limit:
        buyers = buyers[:args.limit]
        print(f"  Limited to {len(buyers)} buyers")

    if not buyers:
        print("\n✅ All buyers already have valid briefs. Nothing to do.")
        return

    # Fetch award history for each buyer
    print(f"\n📊 Fetching award history for {len(buyers)} buyers...")
    buyers_with_history = []
    for i, b in enumerate(buyers):
        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(buyers)}...")
        history = fetch_award_history(b["buyer_name"], b["country"])
        buyers_with_history.append({
            "buyer_name": b["buyer_name"],
            "country": b["country"],
            "award_history": history,
        })

    has_data = sum(1 for b in buyers_with_history if b["award_history"] and b["award_history"].get("stats", {}).get("total_contracts", 0) > 0)
    print(f"  {has_data}/{len(buyers_with_history)} buyers have award history data")

    # Build batch requests
    print("\n🔨 Building batch requests...")
    requests, id_map = build_batch_requests(buyers_with_history)
    print(f"  Built {len(requests)} requests")

    # Country breakdown
    by_country = {}
    for r in requests:
        c = r["custom_id"].split("_")[0]
        by_country[c] = by_country.get(c, 0) + 1
    for c, n in sorted(by_country.items()):
        print(f"    {c}: {n}")

    # Estimate cost
    est_cost = len(requests) * 0.01 + len(requests) * 0.0025  # web search + tokens
    print(f"\n💰 Estimated cost: ~${est_cost:.2f}")
    print(f"   (web search: ${len(requests) * 0.01:.2f} + tokens: ~${len(requests) * 0.0025:.2f})")

    if args.dry_run:
        print("\n🏁 Dry run complete. Use without --dry-run to submit.")
        # Show a sample request
        if requests:
            sample = requests[0]
            print(f"\n--- Sample request: {sample['custom_id']} ---")
            print(f"System prompt: {len(sample['params']['system'])} chars")
            print(f"User message: {len(sample['params']['messages'][0]['content'])} chars")
            print(f"User message preview:\n{sample['params']['messages'][0]['content'][:500]}")
        return

    # Submit
    batch_id = submit_batch(requests)

    # Save id_map for ingestion later
    import json as _json
    map_file = f"batch_{batch_id}_map.json"
    with open(map_file, "w") as f:
        _json.dump(id_map, f)
    print(f"   ID map saved to {map_file}")

    print(f"\n📋 Next steps:")
    print(f"   # Poll status:")
    print(f"   python batch_enrich.py --poll {batch_id}")
    print(f"   # Ingest results when complete:")
    print(f"   python batch_enrich.py --ingest {batch_id}")


if __name__ == "__main__":
    main()
