import { createClientFromRequest } from './civantSdk.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }
        
        const { company_name } = await req.json() as { company_name?: string };
        
        if (!company_name) {
            return Response.json({ error: 'company_name is required' }, { status: 400 });
        }
        
        // Search for awards and tenders mentioning this company
        const allTenders = await civant.entities.TendersCurrent.list('-publication_date', 2000) as Array<Record<string, any>>;
        
        // Filter tenders that mention the competitor
        const relatedTenders = allTenders.filter((t: Record<string, any>) => 
            t.title?.toLowerCase().includes(company_name.toLowerCase()) ||
            t.buyer_name?.toLowerCase().includes(company_name.toLowerCase())
        );
        
        // Also check enrichment data for contractor details
        const enrichments = await civant.entities.TenderEnrichment.list('-enrichment_date', 500) as Array<Record<string, any>>;
        const enrichmentMatches = enrichments.filter((e: Record<string, any>) => {
            try {
                const contractor = JSON.parse(e.contractor_details || '{}');
                return contractor.contractor_name?.toLowerCase().includes(company_name.toLowerCase());
            } catch {
                return false;
            }
        });
        
        // Get tender UIDs from enrichments
        const enrichedTenderUids = enrichmentMatches.map((e: Record<string, any>) => e.tender_uid);
        const enrichedTenders = allTenders.filter((t: Record<string, any>) => enrichedTenderUids.includes(t.tender_uid));
        
        // Combine and deduplicate
        const allMatchingTenders = [...relatedTenders, ...enrichedTenders]
            .filter((tender: Record<string, any>, index: number, self: Array<Record<string, any>>) => 
                index === self.findIndex((t: Record<string, any>) => t.id === tender.id)
            );
        
        if (allMatchingTenders.length === 0) {
            return Response.json({
                success: true,
                company_name,
                found_tenders: 0,
                message: 'No tenders found for this competitor'
            });
        }
        
        // Prepare data for AI analysis
        const tenderSummary = allMatchingTenders.map((t: Record<string, any>) => ({
            title: t.title,
            buyer: t.buyer_name,
            country: t.country,
            notice_type: t.notice_type,
            cpv_codes: t.cpv_codes,
            value: t.estimated_value,
            publication_date: t.publication_date,
            source: t.source
        }));
        
        // AI Analysis
        const prompt = `You are an expert in competitive intelligence and public procurement analysis. Analyze the following competitor's performance in public tenders.

Competitor: ${company_name}
Number of Related Tenders/Awards: ${allMatchingTenders.length}

Tender History:
${JSON.stringify(tenderSummary, null, 2)}

Provide comprehensive analysis including:
1. Win Rate & Success Pattern: How often do they win? What patterns emerge?
2. Preferred CPV Codes: Which categories do they dominate?
3. Geographic Presence: Where are they most active?
4. Value Range Analysis: Typical contract values they pursue
5. Buyer Relationships: Key buyers they work with repeatedly
6. Strengths: What makes them competitive (technical capability, pricing, experience, relationships)
7. Weaknesses: Where they struggle or are less competitive
8. Strategic Insights: How to compete against them effectively
9. Trend Analysis: Are they growing, stable, or declining?

Be specific with data points and actionable insights.`;

        const response = await civant.integrations.Core.InvokeLLM({
            prompt: prompt,
            response_json_schema: {
                type: "object",
                properties: {
                    win_analysis: {
                        type: "object",
                        properties: {
                            total_tenders: { type: "number" },
                            estimated_wins: { type: "number" },
                            win_rate_percentage: { type: "number" },
                            pattern: { type: "string" }
                        }
                    },
                    preferred_cpv_codes: {
                        type: "array",
                        items: {
                            type: "object",
                            properties: {
                                cpv_code: { type: "string" },
                                frequency: { type: "number" },
                                success_rate: { type: "string" }
                            }
                        }
                    },
                    geographic_presence: {
                        type: "object",
                        properties: {
                            primary_countries: { type: "array", items: { type: "string" } },
                            strongest_region: { type: "string" }
                        }
                    },
                    value_analysis: {
                        type: "object",
                        properties: {
                            typical_range: { type: "string" },
                            average_contract_value: { type: "number" },
                            largest_win: { type: "number" }
                        }
                    },
                    key_buyers: {
                        type: "array",
                        items: {
                            type: "object",
                            properties: {
                                buyer_name: { type: "string" },
                                frequency: { type: "number" },
                                relationship_strength: { type: "string" }
                            }
                        }
                    },
                    strengths: {
                        type: "array",
                        items: { type: "string" }
                    },
                    weaknesses: {
                        type: "array",
                        items: { type: "string" }
                    },
                    strategic_insights: {
                        type: "array",
                        items: { type: "string" }
                    },
                    trend: {
                        type: "string",
                        enum: ["growing", "stable", "declining"]
                    }
                }
            }
        });
        
        return Response.json({
            success: true,
            company_name,
            found_tenders: allMatchingTenders.length,
            analysis: response,
            sample_tenders: allMatchingTenders.slice(0, 10).map((t: Record<string, any>) => ({
                id: t.id,
                title: t.title,
                buyer: t.buyer_name,
                value: t.estimated_value,
                date: t.publication_date,
                notice_type: t.notice_type
            }))
        });
        
    } catch (error: unknown) {
        return Response.json({ 
            error: getErrorMessage(error) || 'Analysis failed'
        }, { status: 500 });
    }
});
