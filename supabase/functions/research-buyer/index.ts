import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const ANTHROPIC_API_KEY = Deno.env.get("ANTHROPIC_API_KEY");
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  try {
    const { buyer_name, country, category, tenant_id = "civant_default" } = await req.json();
    if (!buyer_name || !country) {
      return new Response(
        JSON.stringify({ error: "buyer_name and country are required" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const supabase = createClient(SUPABASE_URL!, SUPABASE_SERVICE_KEY!);

    // Check cache
    const { data: cached } = await supabase
      .from("buyer_research_briefs")
      .select("*")
      .eq("tenant_id", tenant_id)
      .ilike("buyer_name", buyer_name)
      .eq("country", country)
      .eq("status", "complete")
      .gt("expires_at", new Date().toISOString())
      .order("researched_at", { ascending: false })
      .limit(1)
      .single();

    if (cached) {
      return new Response(
        JSON.stringify({ brief: cached, source: "cache" }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const countryName: Record<string, string> = { ES: "Spain", FR: "France", IE: "Ireland" };
    const countryLabel = countryName[country] || country;
    const categoryCtx = category ? ` in the ${category} sector` : "";

    const systemPrompt = [
      "You are a procurement intelligence analyst.",
      "Research a public sector buyer and produce a structured intelligence brief",
      "for a company deciding whether to pursue a predicted procurement opportunity.",
      "Focus on ACTIONABLE intelligence: budget signals, procurement intent,",
      "organizational context, incumbent suppliers, and risk factors.",
      "Be concise and factual. Cite sources.",
      "If you cannot find information on a specific area, say so.",
      "Respond ONLY in JSON with this structure:",
      '{"summary":"2-3 sentence executive summary",',
      '"procurement_intent":{"signals":["array of budget/spending signals"],',
      '"confidence":"high|medium|low","notes":"assessment"},',
      '"organizational_context":{"type":"municipality|health_authority|university|ministry|agency|other",',
      '"leadership":"key decision maker if found",',
      '"recent_changes":"organizational changes",',
      '"size_indicator":"small|medium|large"},',
      '"incumbent_landscape":{"known_suppliers":["suppliers found"],',
      '"contract_notes":"relevant history"},',
      '"risk_factors":["identified risks"],',
      '"opportunity_score":0-100,',
      '"sources":[{"url":"url","title":"title","relevance":"why relevant"}]}. CRITICAL: Return ONLY the raw JSON object. No markdown fences, no explanation text before or after, no citations. Just the JSON.'
    ].join(" ");

    const userPrompt = [
      `Research this public sector buyer for a predicted procurement opportunity${categoryCtx}:`,
      "",
      `Buyer: ${buyer_name}`,
      `Country: ${countryLabel}`,
      category ? `Sector: ${category}` : "",
      "",
      "Produce the intelligence brief in JSON format."
    ].filter(Boolean).join("\n");

    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY!,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 1024,
        tools: [{ type: "web_search_20250305", name: "web_search" }],
        system: systemPrompt,
        messages: [{ role: "user", content: userPrompt }],
      }),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error("Anthropic API error:", errText);
      return new Response(
        JSON.stringify({ error: "AI research failed", detail: errText }),
        { status: 502, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const result = await response.json();
    const textBlocks = result.content?.filter((b: any) => b.type === "text") || [];
    const rawText = textBlocks.map((b: any) => b.text).join("\n");

    let brief: any;
    try {
      // Strategy 1: Extract JSON from markdown code fence
      const fenceMatch = rawText.match(/```(?:json)?\s*([\s\S]*?)```/);
      let jsonStr = fenceMatch ? fenceMatch[1].trim() : null;
      
      // Strategy 2: Find outermost { } containing "summary"
      if (!jsonStr) {
        const summaryIdx = rawText.indexOf('"summary"');
        if (summaryIdx > -1) {
          let braceStart = rawText.lastIndexOf("{", summaryIdx);
          if (braceStart > -1) {
            let depth = 0;
            let braceEnd = -1;
            for (let i = braceStart; i < rawText.length; i++) {
              if (rawText[i] === "{") depth++;
              if (rawText[i] === "}") depth--;
              if (depth === 0) { braceEnd = i; break; }
            }
            if (braceEnd > braceStart) {
              jsonStr = rawText.slice(braceStart, braceEnd + 1);
            }
          }
        }
      }
      
      // Strategy 3: Fallback to first { to last }
      if (!jsonStr) {
        const first = rawText.indexOf("{");
        const last = rawText.lastIndexOf("}");
        if (first > -1 && last > first) {
          jsonStr = rawText.slice(first, last + 1);
        }
      }

      // Clean citation tags before parsing
      if (jsonStr) {
        jsonStr = jsonStr.replace(/<cite[^>]*>[^<]*<\/cite>/g, "").replace(/<\/cite>/g, "");
      }
      
      brief = jsonStr ? JSON.parse(jsonStr) : null;
      if (!brief || !brief.summary) throw new Error("No valid brief parsed");
    } catch (_parseErr) {
      // Last resort: extract what we can from the raw text
      const summaryMatch = rawText.match(/"summary"\s*:\s*"([^"]+)"/);
      brief = {
        summary: summaryMatch ? summaryMatch[1] : rawText.replace(/```[\s\S]*?```/g, "").slice(0, 300).trim(),
        procurement_intent: { signals: [], confidence: "low", notes: "Partial extraction from AI response" },
        organizational_context: {},
        incumbent_landscape: {},
        risk_factors: [],
        opportunity_score: 50,
        sources: [],
      };
    }

    const inputTokens = result.usage?.input_tokens || 0;
    const outputTokens = result.usage?.output_tokens || 0;
    const costUsd = (inputTokens * 1.0 + outputTokens * 5.0) / 1_000_000;

    const { data: stored, error: storeErr } = await supabase
      .from("buyer_research_briefs")
      .insert({
        tenant_id,
        buyer_name,
        country,
        category: category || null,
        summary: brief?.summary || null,
        procurement_intent: brief?.procurement_intent || null,
        organizational_context: brief?.organizational_context || null,
        incumbent_landscape: brief?.incumbent_landscape || null,
        risk_factors: brief?.risk_factors || null,
        opportunity_score: brief?.opportunity_score || null,
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

    return new Response(
      JSON.stringify({
        brief: stored || { ...brief, buyer_name, country, category },
        source: "fresh",
        usage: { input_tokens: inputTokens, output_tokens: outputTokens, cost_usd: costUsd },
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (err) {
    console.error("Research agent error:", err);
    return new Response(
      JSON.stringify({ error: "Internal error", detail: String(err) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});
