import { createClientFromRequest } from './civantSdk.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

type TenderRecord = {
  id?: string;
  tender_uid?: string;
  publication_date?: string;
  notice_type?: string;
  title?: string;
  buyer_name?: string;
  cpv_codes?: string;
  estimated_value?: number;
  deadline_date?: string;
  country?: string;
};

type ContractCycle = {
  award_date: string;
  estimated_end_date: string;
  title: string;
  buyer: string;
  cpv: string;
  value: number | null;
  contract_duration_months: number;
  is_framework: boolean;
  is_recurring: boolean;
};

type FeedbackRecord = {
  actual_published?: boolean;
  accuracy_rating?: number;
  feedback_notes?: string;
};

type PredictionResponse = {
  predictions?: Array<Record<string, unknown>>;
  [key: string]: unknown;
};

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await civant.auth.me();

    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { buyer_name, country } = await req.json() as { buyer_name?: string; country?: string };

    const query = buyer_name
      ? { buyer_name }
      : country
        ? { country }
        : {};

    const tenders = await civant.entities.TendersCurrent.filter(query) as TenderRecord[];

    if (tenders.length < 2) {
      return Response.json(
        {
          error: 'Insufficient data',
          message: 'At least 2 historical tenders required for prediction'
        },
        { status: 400 }
      );
    }

    const sortedTenders = tenders
      .filter((t: TenderRecord) => Boolean(t.publication_date))
      .sort(
        (a: TenderRecord, b: TenderRecord) =>
          new Date(String(a.publication_date)).getTime() - new Date(String(b.publication_date)).getTime()
      );

    const awards = sortedTenders.filter((t: TenderRecord) => t.notice_type === 'award');

    let titleAnalysis: Record<string, unknown> = {};
    try {
      titleAnalysis = await civant.integrations.Core.InvokeLLM({
        prompt: `Analyze these tender titles to identify:
1. Framework agreements (multi-year, panel, DPS)
2. Recurring service patterns (annual, maintenance, renewal)
3. Common keywords and themes
4. Contract type categorization

Titles: ${sortedTenders.slice(0, 50).map((t: TenderRecord) => t.title || '').join('\n')}

Extract patterns and categorize tenders.`,
        response_json_schema: {
          type: 'object',
          properties: {
            framework_indicators: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  keyword: { type: 'string' },
                  count: { type: 'number' },
                  typical_duration_years: { type: 'number' }
                }
              }
            },
            recurring_patterns: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  pattern: { type: 'string' },
                  frequency: { type: 'string' },
                  examples: { type: 'array', items: { type: 'string' } }
                }
              }
            },
            contract_categories: {
              type: 'object',
              properties: {
                services: { type: 'number' },
                supplies: { type: 'number' },
                works: { type: 'number' },
                frameworks: { type: 'number' }
              }
            }
          }
        }
      }) as Record<string, unknown>;
    } catch (titleError: unknown) {
      console.error('Title analysis failed:', getErrorMessage(titleError));
      titleAnalysis = {};
    }

    const contractCycles: ContractCycle[] = [];
    for (const award of awards) {
      let contractDuration = 24; // months
      const title = String(award.title || '');
      const titleLower = title.toLowerCase();

      if (titleLower.includes('framework') || titleLower.includes('panel')) {
        contractDuration = 48;
      } else if (titleLower.includes('annual') || titleLower.includes('yearly')) {
        contractDuration = 12;
      }

      if (award.tender_uid) {
        const enrichments = await civant.entities.TenderEnrichment.filter({
          tender_uid: award.tender_uid
        }) as Array<Record<string, unknown>>;

        if (enrichments.length > 0 && enrichments[0].contractor_details) {
          try {
            const details = JSON.parse(String(enrichments[0].contractor_details || '{}')) as { contract_duration_months?: number };
            if (details.contract_duration_months) {
              contractDuration = Number(details.contract_duration_months);
            }
          } catch {
            // Ignore malformed enrichment JSON.
          }
        }
      }

      const awardDate = new Date(String(award.publication_date || new Date().toISOString()));
      const estimatedEndDate = new Date(awardDate);
      estimatedEndDate.setMonth(estimatedEndDate.getMonth() + contractDuration);

      contractCycles.push({
        award_date: String(award.publication_date || ''),
        estimated_end_date: estimatedEndDate.toISOString().split('T')[0],
        title,
        buyer: String(award.buyer_name || ''),
        cpv: String(award.cpv_codes || ''),
        value: award.estimated_value ?? null,
        contract_duration_months: contractDuration,
        is_framework: titleLower.includes('framework') || titleLower.includes('panel'),
        is_recurring: titleLower.includes('annual') || titleLower.includes('maintenance')
      });
    }

    let historicalFeedback: FeedbackRecord[] = [];
    let feedbackSummary: Record<string, unknown> | null = null;
    try {
      const feedbackQuery = buyer_name ? { buyer_name } : {};
      historicalFeedback = await civant.entities.PredictionFeedback.filter(feedbackQuery) as FeedbackRecord[];
      if (historicalFeedback.length > 0) {
        feedbackSummary = {
          total_predictions: historicalFeedback.length,
          accurate_count: historicalFeedback.filter((f: FeedbackRecord) => Boolean(f.actual_published)).length,
          avg_rating:
            historicalFeedback.reduce((sum: number, f: FeedbackRecord) => sum + Number(f.accuracy_rating || 0), 0) /
            historicalFeedback.length,
          common_issues: historicalFeedback
            .map((f: FeedbackRecord) => f.feedback_notes)
            .filter((n: string | undefined) => Boolean(n))
            .slice(0, 5)
        };
      }
    } catch (feedbackError: unknown) {
      console.error('Failed to fetch feedback:', getErrorMessage(feedbackError));
    }

    const dataSummary = sortedTenders.map((t: TenderRecord) => ({
      date: t.publication_date,
      type: t.notice_type,
      title: t.title,
      buyer: t.buyer_name,
      cpv: t.cpv_codes,
      value: t.estimated_value,
      deadline: t.deadline_date
    }));

    let response: PredictionResponse;
    try {
      response = await civant.integrations.Core.InvokeLLM({
        prompt: `You are an expert in public procurement forecasting, contract lifecycle analysis, and NLP-based pattern recognition. Analyze the following comprehensive data.

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
- key_indicators (contract end dates, patterns, NLP insights)`,
        response_json_schema: {
          type: 'object',
          properties: {
            analysis: {
              type: 'object',
              properties: {
                avg_interval_days: { type: 'number' },
                seasonality_detected: { type: 'boolean' },
                peak_months: { type: 'array', items: { type: 'number' } },
                trend: { type: 'string' },
                data_quality: { type: 'string' }
              }
            },
            predictions: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  predicted_date: { type: 'string' },
                  confidence_level: { type: 'string' },
                  confidence_score: { type: 'number' },
                  confidence_reasoning: { type: 'string' },
                  contract_basis: { type: 'string' },
                  tender_type: { type: 'string' },
                  expected_cpv_codes: { type: 'array', items: { type: 'string' } },
                  estimated_value_range: {
                    type: 'object',
                    properties: {
                      min: { type: 'number' },
                      max: { type: 'number' }
                    }
                  },
                  renewal_likelihood: { type: 'string' },
                  seasonality_factor: { type: 'string' },
                  key_indicators: { type: 'array', items: { type: 'string' } }
                }
              }
            },
            confidence_intervals: {
              type: 'object',
              properties: {
                low: { type: 'string' },
                medium: { type: 'string' },
                high: { type: 'string' }
              }
            }
          }
        }
      }) as PredictionResponse;
    } catch (llmError: unknown) {
      console.error('LLM prediction failed:', getErrorMessage(llmError));
      return Response.json(
        {
          error: 'Prediction failed',
          message: 'AI analysis service unavailable',
          details: getErrorMessage(llmError)
        },
        { status: 503 }
      );
    }

    if (!response || !Array.isArray(response.predictions)) {
      return Response.json(
        {
          error: 'Invalid prediction response',
          message: 'AI returned invalid data format'
        },
        { status: 500 }
      );
    }

    return Response.json({
      success: true,
      buyer_name: buyer_name,
      historical_count: sortedTenders.length,
      awards_count: awards.length,
      contract_cycles_analyzed: contractCycles.length,
      framework_count: contractCycles.filter((c: ContractCycle) => c.is_framework).length,
      recurring_count: contractCycles.filter((c: ContractCycle) => c.is_recurring).length,
      nlp_analysis: titleAnalysis,
      feedback_summary: feedbackSummary,
      ...response
    });
  } catch (error: unknown) {
    return Response.json(
      {
        error: getErrorMessage(error) || 'Prediction failed'
      },
      { status: 500 }
    );
  }
});
