#!/usr/bin/env python3
"""Patch batch_enrich.py to fix RPC error handling and None float issues."""
t = open('batch_enrich.py').read()

# Fix 1: wrap RPC call in try/except
old_rpc = '''    resp = supabase.rpc("get_batch_enrichment_buyers", {
        "p_tenant_id": TENANT_ID,
        "p_urgencies": urgencies,
    }).execute()

    if resp.data:
        return resp.data

    # Fallback'''

new_rpc = '''    try:
        resp = supabase.rpc("get_batch_enrichment_buyers", {
            "p_tenant_id": TENANT_ID,
            "p_urgencies": urgencies,
        }).execute()
        if resp.data:
            return resp.data
    except Exception:
        pass

    # Fallback'''

if old_rpc in t:
    t = t.replace(old_rpc, new_rpc)
    print("Fixed RPC try/except")
else:
    print("RPC already fixed or not found")

# Fix 2: None-safe float conversions
replacements = {
    "float(s.get('total_spend', 0))": "float(s.get('total_spend') or 0)",
    "float(s.get('avg_contract_value', 0))": "float(s.get('avg_contract_value') or 0)",
    "float(s.get('max_contract_value', 0))": "float(s.get('max_contract_value') or 0)",
    "float(sup.get('total_value', 0))": "float(sup.get('total_value') or 0)",
    "float(rp.get('avg_value', 0))": "float(rp.get('avg_value') or 0)",
}
for old, new in replacements.items():
    if old in t:
        t = t.replace(old, new)
        print(f"Fixed: {old}")

open('batch_enrich.py', 'w').write(t)
print("Done! Run: python3 batch_enrich.py --dry-run")
