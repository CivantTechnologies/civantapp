import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const ANTHROPIC_API_KEY = Deno.env.get("ANTHROPIC_API_KEY");
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

function buildPrompts(context: string, buyer_name: string, country: string, category?: string, stats?: any, awardHistory?: any) {
  const countryName: Record<string, string> = { ES: "Spain", FR: "France", IE: "Ireland" };
  const countryLabel = countryName[country] || country;

  if (context === "competitor") {
    const system = `You are a competitive intelligence analyst for public procurement markets. Research this competitor company and produce a structured intelligence brief. You already have their procurement data (provided below). Focus on what is NOT in the data: recent news, financial health, leadership changes, project wins/losses, market trajectory. Assess whether this competitor is GROWING or DECLINING in the relevant markets. Identify vulnerabilities: contract disputes, financial trouble, stretched capacity. Be concise, factual, and actionable. Cite sources. Respond ONLY in JSON with this structure: {"summary":"3-4 sentence executive summary of competitive position and trajectory","trajectory":"growing|stable|declining","threat_level":"high|medium|low","recent_activity":["array of recent notable activities found"],"vulnerabilities":["identified weaknesses or risks"],"key_strengths":["confirmed competitive advantages"],"recommendation":"1-2 sentence actionable advice for competing against them","sources":[{"url":"url","title":"title","relevance":"why relevant"}]}. CRITICAL: Return ONLY the raw JSON object. No markdown fences, no explanation text, no citations. Just the JSON.`;

    const user = [
      `Analyze this competitor in public procurement:`,
      `Company: ${buyer_name}`,
      `Country: ${countryLabel}`,
      stats ? `Known procurement data: ${JSON.stringify(stats)}` : "",
      `Research their recent market activity, financial health, and competitive trajectory. Produce the intelligence brief in JSON format.`
    ].filter(Boolean).join("\n");

    return { system, user };
  }

  const hasHistory = awardHistory?.stats?.total_contracts > 0;

  const system = `You are Civant Agent, a procurement intelligence analyst with access to both historical contract award data and web research. Your job is to produce an actionable intelligence brief for a company deciding whether to pursue a predicted procurement opportunity.

You have TWO information sources:
1. CANONICAL DATA: Real contract award history from official procurement portals (provided below). This is factual and verified. Analyze it for: renewal cycles, spend trends, incumbent suppliers, typical contract durations, category patterns, and budget trajectory.
2. WEB RESEARCH: Use your web search to find CURRENT intelligence: recent news, leadership changes, budget announcements, organizational restructuring, upcoming projects, and policy shifts.

COMBINE both sources into a single coherent brief. Lead with data-backed insights (patterns from the award history), then layer on web intelligence. If the award history shows clear patterns (e.g. "renews IT services every 3 years at ~200k"), state them explicitly.

${hasHistory ? "" : "NOTE: No historical award data was found for this buyer. Rely on web research alone but note the data gap."}

Respond ONLY in JSON. Use this EXACT structure (do NOT add extra keys, do NOT nest differently):
{"summary":"3-4 sentence executive summary","procurement_patterns":{"renewal_cycle":"description","spend_trend":"increasing|stable|decreasing|insufficient_data","typical_value_range":"e.g. 50k-200k","preferred_categories":["categories"],"notes":"other patterns"},"incumbent_landscape":{"known_suppliers":["suppliers"],"dominant_supplier":"name or null","contract_notes":"history"},"organizational_context":{"type":"municipality|health_authority|university|ministry|agency|school|other","leadership":"name","recent_changes":"changes","size_indicator":"small|medium|large"},"risk_factors":["risks"],"timing_insight":"when to engage","opportunity_score":75,"opportunity_reasoning":"why this score based on data","intent_confidence":"high|medium|low","intent_reasoning":"why this confidence level","sources":[{"url":"url","title":"title","relevance":"why"}]}

SCORING RULES for opportunity_score (integer 0-100):
- 80-100: Clear renewal cycle approaching, strong spend history, open competition
- 60-79: Good award history, moderate patterns, some incumbent lock-in but winnable
- 40-59: Limited data or mixed signals, worth monitoring
- 20-39: Sparse history, single low-value contract, or locked-in incumbent
- 0-19: No meaningful data, speculative only

INTENT CONFIDENCE based on DATA quality:
- high: 3+ contracts with clear renewal cadence and approaching end date
- medium: 1-2 contracts or irregular renewal pattern
- low: No canonical award data, relying on web research alone

CRITICAL: Return ONLY the raw JSON object. No markdown, no explanation, no preamble. Start with { end with }.`;

  const userParts = [
    `Research this public sector buyer for a predicted procurement opportunity:`,
    `Buyer: ${buyer_name}`,
    `Country: ${countryLabel}`,
    category ? `Sector: ${category}` : "",
  ];

  if (hasHistory) {
    const h = awardHistory;
    const s = h.stats;
    userParts.push("");
    userParts.push("=== CANONICAL AWARD DATA (from official procurement portals) ===");
    userParts.push(`Total contracts on record: ${s.total_contracts}`);
    userParts.push(`Unique suppliers: ${s.unique_suppliers}`);
    userParts.push(`Total spend: EUR ${Number(s.total_spend).toLocaleString()}`);
    userParts.push(`Average contract value: EUR ${Number(s.avg_contract_value).toLocaleString()}`);
    userParts.push(`Max contract value: EUR ${Number(s.max_contract_value).toLocaleString()}`);
    userParts.push(`Award history span: ${s.earliest_award} to ${s.latest_award}`);
    userParts.push(`Average contract duration: ${s.avg_duration_months} months`);
    userParts.push(`Framework agreements: ${s.framework_count}`);
    if (s.cpv_clusters?.length > 0) {
      userParts.push(`Procurement categories: ${s.cpv_clusters.join(", ")}`);
    }
    if (h.top_suppliers?.length > 0) {
      userParts.push("");
      userParts.push("Top suppliers:");
      for (const sup of h.top_suppliers) {
        userParts.push(`  - ${sup.supplier}: ${sup.contracts} contracts, EUR ${Number(sup.total_value).toLocaleString()}, last award ${sup.last_award}`);
      }
    }
    if (h.renewal_patterns?.length > 0) {
      userParts.push("");
      userParts.push("Renewal patterns by category:");
      for (const rp of h.renewal_patterns) {
        userParts.push(`  - ${rp.cpv_cluster}: ${rp.occurrences} contracts, avg duration ${rp.avg_duration} months, avg value EUR ${Number(rp.avg_value).toLocaleString()}, last end date ${rp.last_end_date || "unknown"}`);
      }
    }
    if (h.recent_contracts?.length > 0) {
      userParts.push("");
      userParts.push("Most recent contracts:");
      for (const rc of h.recent_contracts.slice(0, 5)) {
        userParts.push(`  - EUR ${Number(rc.value_eur || 0).toLocaleString()} | ${rc.supplier || "unknown supplier"} | awarded ${rc.award_date} | ends ${rc.end_date || "unknown"} | CPV ${rc.cpv_primary || "n/a"} | ${rc.duration_months || "?"} months`);
      }
    }
    userParts.push("");
    userParts.push("=== END CANONICAL DATA ===");
  } else {
    userParts.push("");
    userParts.push("No historical award data found for this buyer in our database.");
  }

  userParts.push("");
  userParts.push("Now use web search to find current intelligence about this buyer, then combine with the data above to produce the intelligence brief in JSON format.");

  return { system, user: userParts.filter((p) => p !== undefined).join("\n") };
}

// --- Robust JSON extraction ---

let _lastExtractError = "";

function extractJson(rawText: string): any {
  _lastExtractError = "";

  // Step 1: Strip all HTML/XML tags (cite tags from web search)
  const cleaned = rawText.replace(/<[^>]+>/g, "");

  // Step 2: Locate JSON string
  let jsonStr: string | null = null;

  // Strategy A: markdown code fence
  const fenceMatch = cleaned.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fenceMatch) {
    jsonStr = fenceMatch[1].trim();
  }

  // Strategy B: outermost braces around "summary"
  if (!jsonStr) {
    const si = cleaned.indexOf('"summary"');
    if (si > -1) {
      const bs = cleaned.lastIndexOf("{", si);
      if (bs > -1) {
        let depth = 0, be = -1;
        for (let i = bs; i < cleaned.length; i++) {
          if (cleaned[i] === "{") depth++;
          if (cleaned[i] === "}") depth--;
          if (depth === 0) { be = i; break; }
        }
        if (be > bs) jsonStr = cleaned.slice(bs, be + 1);
      }
    }
  }

  // Strategy C: first { to last }
  if (!jsonStr) {
    const f = cleaned.indexOf("{");
    const l = cleaned.lastIndexOf("}");
    if (f > -1 && l > f) jsonStr = cleaned.slice(f, l + 1);
  }

  if (!jsonStr) {
    return { summary: cleaned.slice(0, 400).trim(), sources: [] };
  }

  // Step 3: Clean for parsing - replace literal newlines and tabs with spaces
  jsonStr = jsonStr.replace(/\n/g, " ").replace(/\r/g, " ").replace(/\t/g, " ");

  // Attempt 1: direct parse
  try {
    const p = JSON.parse(jsonStr);
    if (p && p.summary) {
      console.log("extractJson: OK, keys:", Object.keys(p).join(", "));
      return p;
    }
  } catch (e) {
    _lastExtractError = String(e);
    console.error("extractJson: parse failed:", String(e).slice(0, 300));
  }

  // Attempt 2: fix trailing commas
  try {
    const fixed = jsonStr.replace(/,\s*}/g, "}").replace(/,\s*]/g, "]");
    const p2 = JSON.parse(fixed);
    if (p2 && p2.summary) {
      console.log("extractJson: OK after trailing comma fix");
      return p2;
    }
  } catch (_) {}

  // Attempt 3: regex field extraction (always produces results)
  console.log("extractJson: falling back to field extraction");
  const result: any = { sources: [] };

  // Extract simple string fields
  const fields = ["summary", "timing_insight", "opportunity_reasoning", "intent_confidence", "intent_reasoning"];
  for (const f of fields) {
    const re = new RegExp('"' + f + '"\\s*:\\s*"((?:[^"\\\\]|\\\\.)*)"');
    const m = cleaned.match(re);
    if (m) result[f] = m[1].replace(/\\"/g, '"').replace(/\\n/g, " ");
  }

  // Extract opportunity_score (number)
  const scoreMatch = cleaned.match(/"opportunity_score"\s*:\s*(\d+)/);
  if (scoreMatch) result.opportunity_score = parseInt(scoreMatch[1], 10);

  // Extract risk_factors array
  const riskMatch = cleaned.match(/"risk_factors"\s*:\s*(\[[^\]]*\])/);
  if (riskMatch) {
    try { result.risk_factors = JSON.parse(riskMatch[1].replace(/\n/g, " ")); } catch (_) {}
  }

  // Extract nested objects
  for (const field of ["procurement_patterns", "incumbent_landscape", "organizational_context"]) {
    const fi = cleaned.indexOf('"' + field + '"');
    if (fi > -1) {
      const os = cleaned.indexOf("{", fi);
      if (os > -1 && os < fi + field.length + 10) {
        let depth = 0, oe = -1;
        for (let i = os; i < cleaned.length; i++) {
          if (cleaned[i] === "{") depth++;
          if (cleaned[i] === "}") depth--;
          if (depth === 0) { oe = i; break; }
        }
        if (oe > os) {
          try {
            result[field] = JSON.parse(cleaned.slice(os, oe + 1).replace(/\n/g, " "));
          } catch (_) {}
        }
      }
    }
  }

  // Extract sources array
  const si2 = cleaned.lastIndexOf('"sources"');
  if (si2 > -1) {
    const as = cleaned.indexOf("[", si2);
    if (as > -1) {
      let depth = 0, ae = -1;
      for (let i = as; i < cleaned.length; i++) {
        if (cleaned[i] === "[") depth++;
        if (cleaned[i] === "]") depth--;
        if (depth === 0) { ae = i; break; }
      }
      if (ae > as) {
        try { result.sources = JSON.parse(cleaned.slice(as, ae + 1).replace(/\n/g, " ")); } catch (_) {}
      }
    }
  }

  console.log("extractJson: extracted keys:", Object.keys(result).join(", "));
  return result;
}

// --- Server ---

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  try {
    const { buyer_name, country, category, context = "forecast", stats, tenant_id = "civant_default" } = await req.json();
    if (!buyer_name || !country) {
      return new Response(JSON.stringify({ error: "buyer_name and country are required" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const supabase = createClient(SUPABASE_URL!, SUPABASE_SERVICE_KEY!);

    // Check cache
    const { data: cached } = await supabase
      .from("buyer_research_briefs")
      .select("*")
      .eq("tenant_id", tenant_id)
      .ilike("buyer_name", buyer_name)
      .eq("country", country)
      .eq("category", context)
      .eq("status", "complete")
      .gt("expires_at", new Date().toISOString())
      .order("researched_at", { ascending: false })
      .limit(1)
      .single();

    if (cached) {
      return new Response(JSON.stringify({ brief: cached, source: "cache" }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // Fetch canonical award history for forecast context
    let awardHistory = null;
    if (context === "forecast") {
      const { data: historyData, error: historyErr } = await supabase
        .rpc("get_buyer_award_history", { p_buyer_name: buyer_name, p_country: country });
      if (historyErr) {
        console.error("Award history fetch failed:", historyErr);
      } else {
        awardHistory = historyData;
        console.log("Award history:", awardHistory?.stats?.total_contracts, "contracts for", buyer_name);
      }
    }

    const { system, user } = buildPrompts(context, buyer_name, country, category, stats, awardHistory);

    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY!,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 1500,
        tools: [{ type: "web_search_20250305", name: "web_search" }],
        system,
        messages: [{ role: "user", content: user }],
      }),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error("Anthropic API error:", errText);
      return new Response(JSON.stringify({ error: "AI research failed", detail: errText }),
        { status: 502, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const result = await response.json();
    const textBlocks = result.content?.filter((b: any) => b.type === "text") || [];
    const rawText = textBlocks.map((b: any) => b.text).join("\n");

    // TEMP DEBUG: if tenant_id starts with "rawdebug", return diagnostics
    if (tenant_id.startsWith("rawdebug")) {
      const debugBrief = extractJson(rawText);
      return new Response(JSON.stringify({
        raw_text_length: rawText.length,
        has_cite: rawText.includes("<cite"),
        parsed_keys: Object.keys(debugBrief),
        opportunity_score: debugBrief?.opportunity_score ?? "MISSING",
        intent_confidence: debugBrief?.intent_confidence ?? "MISSING",
        timing_insight: debugBrief?.timing_insight ?? "MISSING",
        opportunity_reasoning: debugBrief?.opportunity_reasoning ?? "MISSING",
        summary_start: (debugBrief?.summary || "").slice(0, 200),
        has_procurement_patterns: !!debugBrief?.procurement_patterns,
        has_incumbent_landscape: !!debugBrief?.incumbent_landscape,
        parse_error: _lastExtractError || "none",
      }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const brief = extractJson(rawText);

    const inputTokens = result.usage?.input_tokens || 0;
    const outputTokens = result.usage?.output_tokens || 0;
    const costUsd = (inputTokens * 1.0 + outputTokens * 5.0) / 1_000_000;

    // Build procurement_intent JSONB
    let procurementIntent = null;
    if (context === "forecast") {
      procurementIntent = {
        ...(brief?.procurement_patterns || {}),
        intent_confidence: brief?.intent_confidence || null,
        intent_reasoning: brief?.intent_reasoning || null,
        opportunity_reasoning: brief?.opportunity_reasoning || null,
        timing_insight: brief?.timing_insight || null,
      };
    } else if (brief?.recent_activity) {
      procurementIntent = { signals: brief.recent_activity || [], confidence: brief.threat_level || "medium", notes: brief.recommendation || "" };
    }

    const { data: stored, error: storeErr } = await supabase
      .from("buyer_research_briefs")
      .insert({
        tenant_id,
        buyer_name,
        country,
        category: context,
        summary: brief?.summary || null,
        procurement_intent: procurementIntent,
        organizational_context: brief?.organizational_context || (brief?.trajectory ? { trajectory: brief.trajectory, threat_level: brief.threat_level } : null),
        incumbent_landscape: brief?.incumbent_landscape || (brief?.key_strengths ? { known_suppliers: brief.key_strengths, contract_notes: brief.vulnerabilities?.join("; ") || "" } : null),
        risk_factors: brief?.risk_factors || brief?.vulnerabilities || null,
        opportunity_score: typeof brief?.opportunity_score === "number" ? brief.opportunity_score : null,
        sources: brief?.sources || null,
        model_used: "claude-haiku-4-5",
        tokens_used: inputTokens + outputTokens,
        research_cost_usd: costUsd,
        status: "complete",
        expires_at: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString(),
      })
      .select()
      .single();

    if (storeErr) console.error("Failed to store brief:", storeErr);

    return new Response(JSON.stringify({
      brief: stored || { ...brief, buyer_name, country, category: context },
      source: "fresh",
      usage: { input_tokens: inputTokens, output_tokens: outputTokens, cost_usd: costUsd },
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (err) {
    console.error("Research agent error:", err);
    return new Response(JSON.stringify({ error: "Internal error", detail: String(err) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
