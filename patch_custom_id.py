#!/usr/bin/env python3
"""Patch batch_enrich.py to fix custom_id format (no special chars allowed)."""
t = open('batch_enrich.py').read()

# Fix custom_id: replace country|buyer_name with country_NNNN index
old_build = '''    requests = []
    for item in buyers_with_history:
        buyer_name = item["buyer_name"]
        country = item["country"]
        award_history = item.get("award_history")

        system, user_msg = build_prompts(buyer_name, country, award_history)

        custom_id = f"{country}|{buyer_name}"
        # Batch API custom_id max 64 chars; truncate if needed
        if len(custom_id) > 64:
            custom_id = custom_id[:64]

        requests.append({'''

new_build = '''    requests = []
    id_map = {}
    for idx, item in enumerate(buyers_with_history):
        buyer_name = item["buyer_name"]
        country = item["country"]
        award_history = item.get("award_history")

        system, user_msg = build_prompts(buyer_name, country, award_history)

        custom_id = f"{country}_{idx:04d}"
        id_map[custom_id] = {"buyer_name": buyer_name, "country": country}

        requests.append({'''

if old_build in t:
    t = t.replace(old_build, new_build)
    print("Fixed custom_id format")
else:
    print("custom_id already fixed or not found")

# Fix return value to include id_map
if 'return requests, id_map' not in t and '    return requests\n' in t:
    t = t.replace('    return requests\n', '    return requests, id_map\n', 1)
    print("Fixed return to include id_map")

# Fix submit call to unpack tuple
old_call = '    requests = build_batch_requests(buyers_with_history)'
new_call = '    requests, id_map = build_batch_requests(buyers_with_history)'
if old_call in t:
    t = t.replace(old_call, new_call)
    print("Fixed submit call unpacking")

# Fix country breakdown to not split on pipe
old_country = '''        c = r["custom_id"].split("|")[0]'''
new_country = '''        c = r["custom_id"].split("_")[0]'''
if old_country in t:
    t = t.replace(old_country, new_country)
    print("Fixed country breakdown split")

# Save id_map to JSON file after submitting
old_next = '''    print(f"\\n📋 Next steps:")'''
new_next = '''    # Save id_map for ingestion later
    import json as _json
    map_file = f"batch_{batch_id}_map.json"
    with open(map_file, "w") as f:
        _json.dump(id_map, f)
    print(f"   ID map saved to {map_file}")

    print(f"\\n📋 Next steps:")'''
if old_next in t and 'id_map save' not in t:
    t = t.replace(old_next, new_next)
    print("Added id_map save on submit")

# Fix ingest to load id_map from file
old_ingest = '''    print(f"\\n📥 Downloading results for batch {batch_id}...")

    results = []
    succeeded = 0'''

new_ingest = '''    print(f"\\n📥 Downloading results for batch {batch_id}...")

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
    succeeded = 0'''

if old_ingest in t and 'Load id_map' not in t:
    t = t.replace(old_ingest, new_ingest)
    print("Fixed ingest to load id_map file")

# Fix ingest custom_id parsing
old_parse = '''        # Parse custom_id -> country|buyer_name
        parts = custom_id.split("|", 1)
        if len(parts) != 2:
            print(f"  ⚠ Bad custom_id: {custom_id}")
            skipped += 1
            continue

        country, buyer_name = parts'''

new_parse = '''        # Resolve custom_id via id_map
        if custom_id not in id_map:
            print(f"  ⚠ Unknown custom_id: {custom_id}")
            skipped += 1
            continue
        buyer_name = id_map[custom_id]["buyer_name"]
        country = id_map[custom_id]["country"]'''

if old_parse in t:
    t = t.replace(old_parse, new_parse)
    print("Fixed ingest custom_id parsing")

open('batch_enrich.py', 'w').write(t)
print("\nAll done! Run: python3 batch_enrich.py")
