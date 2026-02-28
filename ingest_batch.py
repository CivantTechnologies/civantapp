#!/usr/bin/env python3
"""
Standalone ingest for Civant batch enrichment results.
Usage: python3 ingest_batch.py msgbatch_01JPCRhApDPgqGw9JmxVg7B4
"""
import os, sys, json, re
from datetime import datetime, timezone, timedelta
import anthropic
from supabase import create_client

BATCH_ID = sys.argv[1] if len(sys.argv) > 1 else None
if not BATCH_ID:
    print("Usage: python3 ingest_batch.py <batch_id>")
    sys.exit(1)

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
TENANT_ID = "civant_default"
BRIEF_TTL_DAYS = 7

# Load id_map
map_file = f"batch_{BATCH_ID}_map.json"
with open(map_file) as f:
    id_map = json.load(f)
print(f"Loaded {len(id_map)} entries from {map_file}")


def extract_json(raw_text):
    cleaned = re.sub(r"<[^>]+>", "", raw_text)
    json_str = None

    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
    if fence:
        json_str = fence.group(1).strip()

    if not json_str:
        si = cleaned.find('"summary"')
        if si > -1:
            bs = cleaned.rfind("{", 0, si)
            if bs > -1:
                depth = 0
                be = -1
                for i in range(bs, len(cleaned)):
                    if cleaned[i] == "{": depth += 1
                    elif cleaned[i] == "}": depth -= 1
                    if depth == 0: be = i; break
                if be > bs:
                    json_str = cleaned[bs:be+1]

    if not json_str:
        f = cleaned.find("{")
        l = cleaned.rfind("}")
        if f > -1 and l > f:
            json_str = cleaned[f:l+1]

    if not json_str:
        return {"summary": cleaned[:400].strip(), "sources": []}

    json_str = json_str.replace("\n", " ").replace("\r", " ").replace("\t", " ")

    try:
        p = json.loads(json_str)
        if isinstance(p, dict) and p.get("summary"):
            return p
    except json.JSONDecodeError:
        pass

    try:
        fixed = re.sub(r",\s*}", "}", json_str)
        fixed = re.sub(r",\s*]", "]", fixed)
        p2 = json.loads(fixed)
        if isinstance(p2, dict) and p2.get("summary"):
            return p2
    except json.JSONDecodeError:
        pass

    result = {"sources": []}
    fields = ["summary", "timing_insight", "opportunity_reasoning", "intent_confidence", "intent_reasoning"]
    for fld in fields:
        m = re.search(r'"' + fld + r'"\s*:\s*"((?:[^"\\]|\\.)*)"', cleaned)
        if m:
            result[fld] = m.group(1).replace('\\"', '"').replace("\\n", " ")

    score_m = re.search(r'"opportunity_score"\s*:\s*(\d+)', cleaned)
    if score_m:
        result["opportunity_score"] = int(score_m.group(1))

    risk_m = re.search(r'"risk_factors"\s*:\s*(\[[^\]]*\])', cleaned)
    if risk_m:
        try: result["risk_factors"] = json.loads(risk_m.group(1).replace("\n", " "))
        except: pass

    for field in ["procurement_patterns", "incumbent_landscape", "organizational_context"]:
        fi = cleaned.find(f'"{field}"')
        if fi > -1:
            os_idx = cleaned.find("{", fi)
            if os_idx > -1 and os_idx < fi + len(field) + 10:
                depth = 0; oe = -1
                for i in range(os_idx, len(cleaned)):
                    if cleaned[i] == "{": depth += 1
                    elif cleaned[i] == "}": depth -= 1
                    if depth == 0: oe = i; break
                if oe > os_idx:
                    try: result[field] = json.loads(cleaned[os_idx:oe+1].replace("\n", " "))
                    except: pass

    si2 = cleaned.rfind('"sources"')
    if si2 > -1:
        as_idx = cleaned.find("[", si2)
        if as_idx > -1:
            depth = 0; ae = -1
            for i in range(as_idx, len(cleaned)):
                if cleaned[i] == "[": depth += 1
                elif cleaned[i] == "]": depth -= 1
                if depth == 0: ae = i; break
            if ae > as_idx:
                try: result["sources"] = json.loads(cleaned[as_idx:ae+1].replace("\n", " "))
                except: pass

    return result


# Process results
print(f"\nDownloading results for batch {BATCH_ID}...")
results = []
succeeded = 0
errored = 0
skipped = 0

for result in client.messages.batches.results(BATCH_ID):
    custom_id = result.custom_id

    if result.result.type == "errored":
        errored += 1
        continue

    if result.result.type != "succeeded":
        skipped += 1
        continue

    # Resolve via id_map
    if custom_id not in id_map:
        print(f"  Unknown custom_id: {custom_id}")
        skipped += 1
        continue

    buyer_name = id_map[custom_id]["buyer_name"]
    country = id_map[custom_id]["country"]

    message = result.result.message
    text_blocks = [b.text for b in message.content if b.type == "text"]
    raw_text = "\n".join(text_blocks)

    brief = extract_json(raw_text)

    usage = message.usage
    input_tokens = usage.input_tokens or 0
    output_tokens = usage.output_tokens or 0
    cost_usd = (input_tokens * 0.4 + output_tokens * 2.0) / 1_000_000
    web_searches = 0
    if hasattr(usage, 'server_tool_use') and usage.server_tool_use:
        web_searches = getattr(usage.server_tool_use, 'web_search_requests', 0)
    cost_usd += web_searches * 0.01

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

# Bulk upsert
print(f"\nUpserting {len(results)} briefs...")
upserted = 0
for i in range(0, len(results), 50):
    chunk = results[i:i+50]
    try:
        supabase.table("buyer_research_briefs").insert(chunk).execute()
        upserted += len(chunk)
        print(f"  {upserted}/{len(results)}...")
    except Exception as e:
        print(f"  Batch insert failed, trying individually: {e}")
        for row in chunk:
            try:
                supabase.table("buyer_research_briefs").insert(row).execute()
                upserted += 1
            except Exception as e2:
                print(f"  Failed: {row['buyer_name']}: {e2}")

# Summary
total_tokens = sum(r.get("tokens_used", 0) for r in results)
total_cost = sum(r.get("research_cost_usd", 0) for r in results)
scores = [r["opportunity_score"] for r in results if r.get("opportunity_score") is not None]
avg_score = sum(scores) / len(scores) if scores else 0

print(f"\n{'='*60}")
print(f"BATCH ENRICHMENT COMPLETE")
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
