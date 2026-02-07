import { createClientFromRequest } from './civantSdk.ts';

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }
        
        const { tender_id } = await req.json();
        
        if (!tender_id) {
            return Response.json({ error: 'tender_id is required' }, { status: 400 });
        }
        
        // Fetch tender details
        const tenders = await civant.entities.TendersCurrent.filter({ id: tender_id });
        if (tenders.length === 0) {
            return Response.json({ error: 'Tender not found' }, { status: 404 });
        }
        
        const tender = tenders[0];
        
        // Check if enrichment already exists
        const existing = await civant.entities.TenderEnrichment.filter({
            tender_uid: tender.tender_uid
        });
        
        if (existing.length > 0) {
            return Response.json({
                success: true,
                message: 'Enrichment already exists',
                enrichment: existing[0]
            });
        }
        
        // Use AI to analyze and enrich tender data
        const prompt = `You are an expert in public procurement analysis. Analyze the following tender/award and extract structured information.

Tender Details:
- Title: ${tender.title}
- Buyer: ${tender.buyer_name}
- Notice Type: ${tender.notice_type}
- CPV Codes: ${tender.cpv_codes || 'N/A'}
- Estimated Value: ${tender.estimated_value ? tender.estimated_value + ' EUR' : 'N/A'}
- Publication Date: ${tender.publication_date || 'N/A'}
- Deadline: ${tender.deadline_date || 'N/A'}
- Country: ${tender.country}

Extract and provide:
1. Key Clauses: Important contractual terms, obligations, or requirements (array of strings)
2. Risk Assessment: Identify potential risks (complexity, timeline, compliance, financial)
3. Evaluation Criteria: Likely evaluation factors (price, technical capability, experience, etc.)
4. Technical Requirements: Main technical specifications or capabilities needed
5. Complexity Assessment: Overall complexity level (low/medium/high)
6. Contractor Details: For awards, extract contractor information if available

${tender.notice_type === 'award' ? 'This is an AWARD notice - focus on contractor details and contract specifics.' : 'This is a TENDER notice - focus on requirements and evaluation criteria.'}

Provide practical, actionable insights based on the tender information.`;

        const response = await civant.integrations.Core.InvokeLLM({
            prompt: prompt,
            response_json_schema: {
                type: "object",
                properties: {
                    key_clauses: {
                        type: "array",
                        items: { type: "string" }
                    },
                    risk_assessment: {
                        type: "object",
                        properties: {
                            overall_risk: { type: "string" },
                            complexity_risk: { type: "string" },
                            timeline_risk: { type: "string" },
                            compliance_risk: { type: "string" },
                            financial_risk: { type: "string" },
                            key_concerns: { type: "array", items: { type: "string" } }
                        }
                    },
                    evaluation_criteria: {
                        type: "array",
                        items: {
                            type: "object",
                            properties: {
                                criterion: { type: "string" },
                                weight: { type: "string" },
                                importance: { type: "string" }
                            }
                        }
                    },
                    technical_requirements: {
                        type: "array",
                        items: { type: "string" }
                    },
                    estimated_complexity: {
                        type: "string",
                        enum: ["low", "medium", "high"]
                    },
                    contractor_details: {
                        type: "object",
                        properties: {
                            contractor_name: { type: "string" },
                            contract_value: { type: "string" },
                            contract_duration: { type: "string" },
                            key_deliverables: { type: "array", items: { type: "string" } }
                        }
                    },
                    confidence_score: {
                        type: "number"
                    }
                }
            }
        });
        
        // Store enrichment
        const enrichmentData = {
            tender_uid: tender.tender_uid,
            key_clauses: JSON.stringify(response.key_clauses || []),
            risk_assessment: JSON.stringify(response.risk_assessment || {}),
            evaluation_criteria: JSON.stringify(response.evaluation_criteria || []),
            contractor_details: JSON.stringify(response.contractor_details || {}),
            technical_requirements: JSON.stringify(response.technical_requirements || []),
            estimated_complexity: response.estimated_complexity || 'medium',
            enrichment_date: new Date().toISOString(),
            confidence_score: response.confidence_score || 0.7
        };
        
        const enrichment = await civant.entities.TenderEnrichment.create(enrichmentData);
        
        return Response.json({
            success: true,
            enrichment: enrichment,
            raw_analysis: response
        });
        
    } catch (error) {
        return Response.json({ 
            error: error.message || 'Enrichment failed'
        }, { status: 500 });
    }
});