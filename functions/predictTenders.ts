import { createClientFromRequest } from './civantSdk.ts';

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }
        
        const { buyer_name, country } = await req.json();
        
        // Fetch historical tenders for the buyer
        const query = buyer_name 
            ? { buyer_name: buyer_name }
            : country ? { country: country } : {};
            
        const tenders = await civant.entities.TendersCurrent.filter(query);
        
        if (tenders.length < 2) {
            return Response.json({ 
                error: 'Insufficient data',
                message: 'At least 2 historical tenders required for prediction'
            }, { status: 400 });
        }
        
        // Sort by publication date
        const sortedTenders = tenders
            .filter(t => t.publication_date)
            .sort((a, b) => new Date(a.publication_date) - new Date(b.publication_date));
        
        // Separate awards from regular tenders
        const awards = sortedTenders.filter(t => t.notice_type === 'award');
        const regularTenders = sortedTenders.filter(t => t.notice_type !== 'award');
        
        // Analyze tender titles for framework patterns and recurring keywords
        let titleAnalysis;
        try {
            titleAnalysis = await civant.integrations.Core.InvokeLLM({
            prompt: `Analyze these tender titles to identify:
1. Framework agreements (multi-year, panel, DPS)
2. Recurring service patterns (annual, maintenance, renewal)
3. Common keywords and themes
4. Contract type categorization

Titles: ${sortedTenders.slice(0, 50).map(t => t.title).join('\n')}

Extract patterns and categorize tenders.`,
            response_json_schema: {
                type: "object",
                properties: {
                    framework_indicators: {
                        type: "array",
                        items: {
                            type: "object",
                            properties: {
                                keyword: { type: "string" },
                                count: { type: "number" },
                                typical_duration_years: { type: "number" }
                            }
                        }
                    },
                    recurring_patterns: {
                        type: "array",
                        items: {
                            type: "object",
                            properties: {
                                pattern: { type: "string" },
                                frequency: { type: "string" },
                                examples: { type: "array", items: { type: "string" } }
                            }
                        }
                    },
                    contract_categories: {
                        type: "object",
                        properties: {
                            services: { type: "number" },
                            supplies: { type: "number" },
                            works: { type: "number" },
                            frameworks: { type: "number" }
                        }
                    }
                }
            }
        });
        
        // Calculate contract end dates for awards
        const contractCycles = awards.map(award => {
            const enrichments = civant.entities.TenderEnrichment.filter({ tender_uid: award.tender_uid });
            let contractDuration = 24; // Default 2 years in months
            
            // Check for framework indicators in title
            const titleLower = award.title.toLowerCase();
            if (titleLower.includes('framework') || titleLower.includes('panel')) {
                contractDuration = 48; // 4 years for frameworks
            } else if (titleLower.includes('annual') || titleLower.includes('yearly')) {
                contractDuration = 12;
            }
            
            // Try to extract duration from enrichment if available
            if (enrichments.length > 0 && enrichments[0].contractor_details) {
                try {
                    const details = JSON.parse(enrichments[0].contractor_details);
                    if (details.contract_duration_months) {
                        contractDuration = details.contract_duration_months;
                    }
                } catch (e) {}
            }
            
            const awardDate = new Date(award.publication_date);
            const estimatedEndDate = new Date(awardDate);
            estimatedEndDate.setMonth(estimatedEndDate.getMonth() + contractDuration);
            
            return {
                award_date: award.publication_date,
                estimated_end_date: estimatedEndDate.toISOString().split('T')[0],
                title: award.title,
                buyer: award.buyer_name,
                cpv: award.cpv_codes,
                value: award.estimated_value,
                contract_duration_months: contractDuration,
                is_framework: titleLower.includes('framework') || titleLower.includes('panel'),
                is_recurring: titleLower.includes('annual') || titleLower.includes('maintenance')
            };
        });
        
        // Fetch historical prediction feedback for learning
        let historicalFeedback = [];
        let feedbackSummary = null;
        try {
            const feedbackQuery = buyer_name ? { buyer_name } : {};
            historicalFeedback = await civant.entities.PredictionFeedback.filter(feedbackQuery);
            feedbackSummary = historicalFeedback.length > 0 ? {
            total_predictions: historicalFeedback.length,
            accurate_count: historicalFeedback.filter(f => f.actual_published).length,
            avg_rating: historicalFeedback.reduce((sum, f) => sum + (f.accuracy_rating || 0), 0) / historicalFeedback.length,
            common_issues: historicalFeedback.map(f => f.feedback_notes).filter(n => n).slice(0, 5)
        } : null;
        } catch (feedbackError) {
            console.error('Failed to fetch feedback:', feedbackError);
        }
        
        // Prepare comprehensive data summary for LLM
        const dataSummary = sortedTenders.map(t => ({
            date: t.publication_date,
            type: t.notice_type,
            title: t.title,
            buyer: t.buyer_name,
            cpv: t.cpv_codes,
            value: t.estimated_value,
            deadline: t.deadline_date
        }));
        
        // Use LLM for advanced time-series analysis and prediction
        const prompt = `You are an expert in public procurement forecasting, contract lifecycle analysis, and NLP-based pattern recognition. Analyze the following comprehensive data.

Historical Tender Data:
${JSON.stringify(dataSummary, null, 2)}

NLP Analysis Results:
${JSON.stringify(titleAnalysis, null, 2)}

${contractCycles.length > 0 ? `
Contract Award Cycles (Critical for Prediction):
${JSON.stringify(contractCycles, null, 2)}

FRAMEWORK DETECTION: Identify which contracts are framework agreements (typically 4 years) vs standard contracts (1-3 years). Frameworks have call-offs and may have mini-competitions.
RECURRING PATTERNS: Identify annual maintenance, seasonal services, or cyclical procurement patterns.
` : ''}

${feedbackSummary ? `
Historical Prediction Feedback (Learn from Past):
${JSON.stringify(feedbackSummary, null, 2)}

Adjust predictions based on past accuracy and user feedback patterns.
` : ''}

Perform comprehensive analysis using:
1. **Contract End Date Analysis** (PRIMARY): Calculate end dates + 3-6 month pre-tender lead time
2. **Framework Agreement Detection**: Identify multi-year frameworks and predict call-offs/renewals
3. **Recurring Pattern Recognition**: Detect annual/cyclical services (maintenance, seasonal work)
4. **NLP-Based Categorization**: Use title patterns to classify contract types and predict similar future tenders
5. **Seasonality & Budget Cycles**: Q1 budget allocations, Q4 spend patterns
6. **Historical Accuracy Learning**: Incorporate feedback to refine confidence levels

Prediction Strategy:
- HIGH confidence: Contract end date + framework renewal + recurring pattern match
- MEDIUM confidence: Historical pattern + seasonality + CPV similarity
- LOW confidence: General trends only

For each of 3 predictions, include:
- predicted_date (YYYY-MM-DD, calculated from contract cycles)
- confidence_level (high/medium/low)
- confidence_reasoning (detailed explanation)
- contract_basis (which award/pattern drives this prediction)
- tender_type (framework_renewal, annual_maintenance, new_procurement, call_off)
- expected_cpv_codes (based on historical patterns)
- estimated_value_range (min/max EUR)
- renewal_likelihood (if framework/recurring)
- seasonality_factor
- key_indicators (contract end dates, patterns, NLP insights)`;

        const response = await civant.integrations.Core.InvokeLLM({
            prompt: prompt,
            response_json_schema: {
                type: "object",
                properties: {
                    analysis: {
                        type: "object",
                        properties: {
                            avg_interval_days: { type: "number" },
                            seasonality_detected: { type: "boolean" },
                            peak_months: { type: "array", items: { type: "number" } },
                            trend: { type: "string" },
                            data_quality: { type: "string" }
                        }
                    },
                    predictions: {
                        type: "array",
                        items: {
                            type: "object",
                            properties: {
                                predicted_date: { type: "string" },
                                confidence_level: { type: "string" },
                                confidence_score: { type: "number" },
                                confidence_reasoning: { type: "string" },
                                contract_basis: { type: "string" },
                                tender_type: { type: "string" },
                                expected_cpv_codes: { type: "array", items: { type: "string" } },
                                estimated_value_range: {
                                    type: "object",
                                    properties: {
                                        min: { type: "number" },
                                        max: { type: "number" }
                                    }
                                },
                                renewal_likelihood: { type: "string" },
                                seasonality_factor: { type: "string" },
                                key_indicators: { type: "array", items: { type: "string" } }
                            }
                        }
                    },
                    confidence_intervals: {
                        type: "object",
                        properties: {
                            low: { type: "string" },
                            medium: { type: "string" },
                            high: { type: "string" }
                        }
                    }
                }
            }
        });
        } catch (llmError) {
            console.error('LLM prediction failed:', llmError);
            return Response.json({ 
                error: 'Prediction failed',
                message: 'AI analysis service unavailable',
                details: llmError.message
            }, { status: 503 });
        }
        
        if (!response || !response.predictions || !Array.isArray(response.predictions)) {
            return Response.json({ 
                error: 'Invalid prediction response',
                message: 'AI returned invalid data format'
            }, { status: 500 });
        }
        
        return Response.json({
            success: true,
            buyer_name: buyer_name,
            historical_count: sortedTenders.length,
            awards_count: awards.length,
            contract_cycles_analyzed: contractCycles.length,
            framework_count: contractCycles.filter(c => c.is_framework).length,
            recurring_count: contractCycles.filter(c => c.is_recurring).length,
            nlp_analysis: titleAnalysis,
            feedback_summary: feedbackSummary,
            ...response
        });
        
    } catch (error) {
        return Response.json({ 
            error: error.message || 'Prediction failed'
        }, { status: 500 });
    }
});