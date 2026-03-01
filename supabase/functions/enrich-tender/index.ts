import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const ANTHROPIC_API_KEY = Deno.env.get("ANTHROPIC_API_KEY");
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

function buildPrompt(tender: any, awardHistory: any) {
  const hasHistory = awardHistory?.stats?.total_contracts > 0;
  const countryName: Record<string, string> = { ES: "Spain", FR: "France", IE: "Ireland" };
  const countryLabel = countryName[tender.country] || tender.country || "Unknown";

  const system = `You are Civant Agent, a procurement intelligence analyst. You produce actionable intelligence briefs for companies evaluating whether to bid on a specific public tender.

You have access to:
1. THE TENDER: Full details of the published procurement notice.
2. BUYER HISTORY: Historical contract award data for this buyer from official procurement portals (if available).
3. WEB RESEARCH: Use web search to find current intelligence about the buyer, similar contracts, and market context.

Your job is to produce a comprehensive intelligence brief that helps a company make a BID / MONITOR / PASS decision.

Respond ONLY in JSON with this EXACT structure:
{
  "summary": "3-5 sentence executive summary of the opportunity, who the buyer is, what they want, and whether this looks winnable",
  "bid_recommendation": "bid|monitor|pass",
  "bid_reasoning": "2-3 sentences explaining the recommendation based on evidence",
  "opportunity_score": 72,
  "key_requirements": [
    {"requirement": "description of requirement", "complexity": "low|medium|high", "notes": "any clarification"}
  ],
  "evaluation_criteria": [
    {"criterion": "name", "weight": "60%", "importance": "high|medium|low", "notes": "what this means for bidders"}
  ],
  "risk_assessment": {
    "overall_risk": "low|medium|high",
    "timeline_risk": "low|medium|high",
    "compliance_risk": "low|medium|high",
    "competition_risk": "low|medium|high",
    "key_concerns": ["concern 1", "concern 2"],
    "mitigations": ["mitigation 1", "mitigation 2"]
  },
  "competitive_landscape": {
    "likely_competitors": ["company names if identifiable from history"],
    "incumbent": "name or null",
    "incumbent_advantage": "description of any advantage or null",
    "market_notes": "relevant market context"
  },
  "timing_analysis": {
    "deadline_pressure": "low|medium|high",
    "preparation_time_adequate": true,
    "key_dates": "summary of important dates",
    "recommendation": "when to start and key milestones"
  },
  "buyer_context": {
    "organization_type": "municipality|health_authority|university|ministry|agency|school|other",
    "size_indicator": "small|medium|large",
    "procurement_maturity": "basic|standard|sophisticated",
    "recent_activity": "summary of recent procurement or organizational activity",
    "relationship_notes": "any notes on how to approach this buyer"
  },
  "sources": [{"url": "url", "title": "title", "relevance": "why relevant"}]
}

SCORING RULES for opportunity_score (0-100):
- 80-100: Clear requirements match, adequate timeline, open competition, good value
- 60-79: Solid opportunity with some unknowns or moderate competition
- 40-59: Mixed signals, tight timeline, or strong incumbent
- 20-39: Significant barriers, very tight timeline, or likely wired for incumbent
- 0-19: Major red flags, unrealistic requirements, or not worth pursuing

BID RECOMMENDATION:
- "bid": Score 60+, requirements are clear, timeline is feasible, winnable
- "monitor": Score 40-59, worth tracking for clarifications or if scope changes
- "pass": Score below 40, or major red flags regardless of score

CRITICAL: Return ONLY the raw JSON object. No markdown, no explanation, no preamble. Start with { end with }.`;

  const userParts = [
    "=== TENDER DETAILS ===",
    `Title: ${tender.title || "Not provided"}`,
    `Buyer: ${tender.buyer_name || "Not specified"}`,
    `Country: ${countryLabel}`,
    `Published: ${tender.publication_date || "Unknown"}`,
    `Deadline: ${tender.deadline_date || "Unknown"}`,
    tender.estimated_value ? `Estimated Value: EUR ${Number(tender.estimated_value).toLocaleString()}` : "",
    tender.cpv_codes?.length > 0 ? `CPV Codes: ${tender.cpv_codes.join(", ")}` : "",
    tender.description ? `Description: ${tender.description.slice(0, 2000)}` : "",
    tender.procedure_type ? `Procedure: ${tender.procedure_type}` : "",
    tender.source ? `Source: ${tender.source}` : "",
    "=== END TENDER DETAILS ===",
  ];

  if (hasHistory) {
    const h = awardHistory;
    const s = h.stats;
    userParts.push("");
    userParts.push("=== BUYER AWARD HISTORY (from official procurement portals) ===");
    userParts.push(`Total contracts on record: ${s.total_contracts}`);
    userParts.push(`Unique suppliers: ${s.unique_suppliers}`);
    userParts.push(`Total spend: EUR ${Number(s.total_spend).toLocaleString()}`);
    userParts.push(`Average contract value: EUR ${Number(s.avg_contract_value).toLocaleString()}`);
    userParts.push(`Award history span: ${s.earliest_award} to ${s.latest_award}`);
    userParts.push(`Average contract duration: ${s.avg_duration_months} months`);
    if (s.cpv_clusters?.length > 0) {
      userParts.push(`Procurement categories: ${s.cpv_clusters.join(", ")}`);
    }
    if (h.top_suppliers?.length > 0) {
      userParts.push("");
      userParts.push("Top suppliers:");
      for (const sup of h.top_suppliers.slice(0, 8)) {
        userParts.push(`  - ${sup.supplier}: ${sup.contracts} contracts, EUR ${Number(sup.total_value).toLocaleString()}, last award ${sup.last_award}`);
      }
    }
    if (h.renewal_patterns?.length > 0) {
      userParts.push("");
      userParts.push("Renewal patterns:");
      for (const rp of h.renewal_patterns.slice(0, 5)) {
        userParts.push(`  - ${rp.cpv_cluster}: ${rp.occurrences} contracts, avg ${rp.avg_duration} months, avg EUR ${Number(rp.avg_value).toLocaleString()}`);
      }
    }
    if (h.recent_contracts?.length > 0) {
      userParts.push("");
      userParts.push("Recent contracts:");
      for (const rc of h.recent_contracts.slice(0, 5)) {
        userParts.push(`  - EUR ${Number(rc.value_eur || 0).toLocaleString()} | ${rc.supplier || "unknown"} | awarded ${rc.award_date} | ${rc.duration_months || "?"} months`);
      }
    }
    userParts.push("=== END BUYER HISTORY ===");
  } else {
    userParts.push("");
    userParts.push("No historical award data found for this buyer.");
  }

  userParts.push("");
  userParts.push("Use web search to research this buyer and similar tenders, then combine all information into the intelligence brief JSON.");

  return { system, user: userParts.filter(Boolean).join("\n") };
}

function extractJson(rawText: string): any {
  const cleaned = rawText.replace(/<[^>]+>/g, "");
  let jsonStr: string | null = null;

  // Strategy A: markdown fence
  const fenceMatch = cleaned.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fenceMatch) jsonStr = fenceMatch[1].trim();

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

  jsonStr = jsonStr.replace(/\n/g, " ").replace(/\r/g, " ").replace(/\t/g, " ");

  // Attempt 1: direct parse
  try {
    const p = JSON.parse(jsonStr);
    if (p && p.summary) return p;
  } catch (_) {}

  // Attempt 2: fix trailing commas
  try {
    const fixed = jsonStr.replace(/,\s*}/g, "}").replace(/,\s*]/g, "]");
    const p2 = JSON.parse(fixed);
    if (p2 && p2.summary) return p2;
  } catch (_) {}

  // Attempt 3: field extraction fallback
  const result: any = { sources: [] };
  const fields = ["summary", "bid_recommendation", "bid_reasoning"];
  for (const f of fields) {
    const re = new RegExp('"' + f + '"\\s*:\\s*"((?:[^"\\\\]|\\\\.)*)"');
    const m = cleaned.match(re);
    if (m) result[f] = m[1].replace(/\\"/g, '"').replace(/\\n/g, " ");
  }
  const scoreMatch = cleaned.match(/"opportunity_score"\s*:\s*(\d+)/);
  if (scoreMatch) result.opportunity_score = parseInt(scoreMatch[1], 10);

  for (const field of ["risk_assessment", "competitive_landscape", "timing_analysis", "buyer_context"]) {
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
          try { result[field] = JSON.parse(cleaned.slice(os, oe + 1).replace(/\n/g, " ")); } catch (_) {}
        }
      }
    }
  }

  for (const arrField of ["key_requirements", "evaluation_criteria"]) {
    const ai = cleaned.indexOf('"' + arrField + '"');
    if (ai > -1) {
      const as = cleaned.indexOf("[", ai);
      if (as > -1) {
        let depth = 0, ae = -1;
        for (let i = as; i < cleaned.length; i++) {
          if (cleaned[i] === "[") depth++;
          if (cleaned[i] === "]") depth--;
          if (depth === 0) { ae = i; break; }
        }
        if (ae > as) {
          try { result[arrField] = JSON.parse(cleaned.slice(as, ae + 1).replace(/\n/g, " ")); } catch (_) {}
        }
      }
    }
  }

  return result;
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const { tender_id, tenant_id = "civant_default" } = await req.json();
    if (!tender_id) {
      return new Response(JSON.stringify({ error: "tender_id is required" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const supabase = createClient(SUPABASE_URL!, SUPABASE_SERVICE_KEY!);

    // Check cache (valid for 30 days)
    const { data: cached } = await supabase
      .from("tender_enrichments")
      .select("*")
      .eq("tenant_id", tenant_id)
      .eq("tender_id", tender_id)
      .eq("status", "complete")
      .gt("expires_at", new Date().toISOString())
      .order("created_at", { ascending: false })
      .limit(1)
      .single();

    if (cached) {
      return new Response(JSON.stringify({ enrichment: cached, source: "cache" }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // Load the tender
    const { data: tenderRows, error: tenderErr } = await supabase
      .from("canonical_tenders")
      .select("*")
      .eq("id", tender_id)
      .limit(1);

    if (tenderErr || !tenderRows?.length) {
      return new Response(JSON.stringify({ error: "Tender not found" }),
        { status: 404, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const tender = tenderRows[0];

    // Fetch buyer award history if buyer is known
    let awardHistory = null;
    if (tender.buyer_name && tender.country) {
      const { data: historyData, error: historyErr } = await supabase
        .rpc("get_buyer_award_history", { p_buyer_name: tender.buyer_name, p_country: tender.country });
      if (historyErr) {
        console.error("Award history fetch failed:", historyErr);
      } else {
        awardHistory = historyData;
        console.log("Award history:", awardHistory?.stats?.total_contracts, "contracts for", tender.buyer_name);
      }
    }

    const { system, user } = buildPrompt(tender, awardHistory);

    // Call Claude
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY!,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 2000,
        tools: [{ type: "web_search_20250305", name: "web_search" }],
        system,
        messages: [{ role: "user", content: user }],
      }),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error("Anthropic API error:", errText);
      return new Response(JSON.stringify({ error: "AI analysis failed", detail: errText }),
        { status: 502, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const result = await response.json();
    const textBlocks = result.content?.filter((b: any) => b.type === "text") || [];
    const rawText = textBlocks.map((b: any) => b.text).join("\n");

    const brief = extractJson(rawText);

    const inputTokens = result.usage?.input_tokens || 0;
    const outputTokens = result.usage?.output_tokens || 0;
    const costUsd = (inputTokens * 1.0 + outputTokens * 5.0) / 1_000_000;

    // Store enrichment
    const { data: stored, error: storeErr } = await supabase
      .from("tender_enrichments")
      .insert({
        tenant_id,
        tender_id,
        canonical_id: tender.canonical_id || tender.id,
        summary: brief?.summary || null,
        bid_recommendation: brief?.bid_recommendation || null,
        bid_reasoning: brief?.bid_reasoning || null,
        opportunity_score: typeof brief?.opportunity_score === "number" ? brief.opportunity_score : null,
        key_requirements: brief?.key_requirements || [],
        evaluation_criteria: brief?.evaluation_criteria || [],
        risk_assessment: brief?.risk_assessment || {},
        competitive_landscape: brief?.competitive_landscape || {},
        timing_analysis: brief?.timing_analysis || {},
        buyer_context: brief?.buyer_context || {},
        sources: brief?.sources || [],
        model_used: "claude-haiku-4-5",
        tokens_used: inputTokens + outputTokens,
        research_cost_usd: costUsd,
        status: "complete",
        expires_at: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString(),
      })
      .select()
      .single();

    if (storeErr) console.error("Failed to store enrichment:", storeErr);

    return new Response(JSON.stringify({
      enrichment: stored || { ...brief, tender_id, tenant_id },
      source: "fresh",
      usage: { input_tokens: inputTokens, output_tokens: outputTokens, cost_usd: costUsd },
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (err) {
    console.error("Enrich tender error:", err);
    return new Response(JSON.stringify({ error: "Internal error", detail: String(err) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
