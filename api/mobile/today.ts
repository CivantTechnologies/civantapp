import { getCurrentUser } from '../_lib/auth.js';
import { methodNotAllowed, normalizeError, sendJson, type RequestLike, type ResponseLike } from '../_lib/http.js';
import { getServerSupabase } from '../_lib/supabase.js';

type PredictionRow = {
  prediction_id: string;
  tenant_id: string;
  buyer_display_name: string | null;
  cpv_cluster_label: string | null;
  expected_window_start: string | null;
  confidence: number | null;
  confidence_band: string | null;
  forecast_score: number | null;
  explainability_json: Record<string, unknown> | null;
};

type DriverRow = {
  prediction_id: string;
  label: string | null;
  narrative: string | null;
  contribution: number | null;
};

type ScorecardRow = {
  prediction_id: string;
  total_score: number | null;
  data_quality_score: number | null;
};

function asIsoDate(value: string | null) {
  if (!value) return '';
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? '' : parsed.toISOString();
}

function getDaysUntil(value: string | null) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const diffMs = date.getTime() - Date.now();
  return Math.floor(diffMs / (24 * 60 * 60 * 1000));
}

function getConfidenceBand(rawBand: unknown, confidence: number) {
  const normalized = String(rawBand || '').trim();
  if (normalized === 'Very High') return 'Very High';
  if (normalized === 'High') return 'High';
  if (normalized === 'Low') return 'Low';
  if (confidence >= 85) return 'Very High';
  if (confidence >= 65) return 'High';
  if (confidence >= 45) return 'Medium';
  return 'Low';
}

function toOpportunityTitle(row: PredictionRow) {
  const cluster = String(row.cpv_cluster_label || '').trim();
  if (!cluster) return 'Predicted opportunity';
  return cluster;
}

function toValueBand(row: PredictionRow) {
  const explainability =
    row.explainability_json && typeof row.explainability_json === 'object' && !Array.isArray(row.explainability_json)
      ? row.explainability_json
      : {};
  const explicitValueBand = String(explainability.value_band || explainability.valueBand || '').trim();
  if (explicitValueBand) return explicitValueBand;
  return 'Value band not yet scored';
}

function toNextAction(confidenceBand: string, daysUntilStart: number | null) {
  if (confidenceBand === 'Very High' || confidenceBand === 'High') {
    if (daysUntilStart !== null && daysUntilStart <= 14) return 'Assign capture owner now';
    return 'Prioritize in this week sprint';
  }
  if (confidenceBand === 'Medium') return 'Review evidence and competitor landscape';
  return 'Monitor for stronger signals';
}

export default async function handler(req: RequestLike, res: ResponseLike) {
  try {
    if ((req.method || 'GET').toUpperCase() !== 'GET') {
      throw methodNotAllowed('GET');
    }

    const user = await getCurrentUser(req);
    const tenantId = String(user.tenantId || '').trim().toLowerCase();
    if (!tenantId) {
      sendJson(res, 403, { error: 'Tenant not available for user' });
      return;
    }

    const supabase = getServerSupabase() as any;
    const predictionsResult = await supabase
      .from('predictions_current')
      .select(
        'prediction_id,tenant_id,buyer_display_name,cpv_cluster_label,expected_window_start,confidence,confidence_band,forecast_score,explainability_json'
      )
      .eq('tenant_id', tenantId)
      .order('confidence', { ascending: false })
      .order('forecast_score', { ascending: false })
      .limit(24);

    if (predictionsResult.error) {
      throw Object.assign(new Error(predictionsResult.error.message), { status: 500 });
    }

    const predictionRows = (Array.isArray(predictionsResult.data) ? predictionsResult.data : []) as PredictionRow[];
    const predictionIds = predictionRows
      .map((row) => String(row.prediction_id || '').trim())
      .filter(Boolean);

    const [driversResult, scorecardsResult] = predictionIds.length
      ? await Promise.all([
          supabase
            .from('prediction_drivers')
            .select('prediction_id,label,narrative,contribution')
            .eq('tenant_id', tenantId)
            .in('prediction_id', predictionIds)
            .order('contribution', { ascending: false }),
          supabase
            .from('prediction_scorecard')
            .select('prediction_id,total_score,data_quality_score')
            .eq('tenant_id', tenantId)
            .in('prediction_id', predictionIds)
        ])
      : [{ data: [], error: null }, { data: [], error: null }];

    if (driversResult.error) {
      throw Object.assign(new Error(driversResult.error.message), { status: 500 });
    }
    if (scorecardsResult.error) {
      throw Object.assign(new Error(scorecardsResult.error.message), { status: 500 });
    }

    const scorecardByPrediction = new Map<string, ScorecardRow>();
    for (const row of (Array.isArray(scorecardsResult.data) ? scorecardsResult.data : []) as ScorecardRow[]) {
      const predictionId = String(row.prediction_id || '').trim();
      if (!predictionId) continue;
      scorecardByPrediction.set(predictionId, row);
    }

    const driversByPrediction = new Map<string, DriverRow[]>();
    for (const row of (Array.isArray(driversResult.data) ? driversResult.data : []) as DriverRow[]) {
      const predictionId = String(row.prediction_id || '').trim();
      if (!predictionId) continue;
      const list = driversByPrediction.get(predictionId) || [];
      list.push(row);
      driversByPrediction.set(predictionId, list);
    }

    const opportunities = predictionRows.map((row) => {
      const predictionId = String(row.prediction_id || '').trim();
      const confidenceScore = Number(row.confidence || 0);
      const confidenceBand = getConfidenceBand(row.confidence_band, confidenceScore);
      const dueDate = asIsoDate(row.expected_window_start);
      const daysUntilStart = getDaysUntil(row.expected_window_start);
      const scorecard = scorecardByPrediction.get(predictionId);
      const topDrivers = (driversByPrediction.get(predictionId) || []).slice(0, 3);

      const signalSummary = topDrivers.length
        ? topDrivers.map((driver) => {
            const label = String(driver.label || '').trim();
            const narrative = String(driver.narrative || '').trim();
            if (label && narrative) return `${label}: ${narrative}`;
            return label || narrative || 'Deterministic scoring signal detected';
          })
        : ['Awaiting detailed driver narratives from current scoring run'];

      return {
        id: predictionId,
        buyer: String(row.buyer_display_name || 'Unknown buyer'),
        title: toOpportunityTitle(row),
        dueDate,
        valueBand: toValueBand(row),
        confidenceScore,
        confidenceBand,
        signalSummary,
        nextAction: toNextAction(confidenceBand, daysUntilStart),
        scorecard: {
          totalScore: Number(scorecard?.total_score || 0),
          dataQualityScore: Number(scorecard?.data_quality_score || 0),
        },
      };
    });

    const highConfidenceCount = opportunities.filter(
      (opportunity) => opportunity.confidenceBand === 'Very High' || opportunity.confidenceBand === 'High'
    ).length;
    const dueSoonCount = opportunities.filter((opportunity) => {
      const daysUntil = getDaysUntil(opportunity.dueDate);
      return daysUntil !== null && daysUntil <= 14;
    }).length;

    const criticalAlerts = opportunities
      .filter((opportunity) => opportunity.confidenceBand === 'Very High' || opportunity.confidenceBand === 'High')
      .slice(0, 3)
      .map((opportunity) => ({
        id: `alert-${opportunity.id}`,
        title: `High-confidence: ${opportunity.buyer}`,
        description: `${opportunity.title} window opens ${opportunity.dueDate || 'soon'}.`,
      }));

    const quickActions = [
      { id: 'triage-inbox', label: 'Triage Inbox', detail: `${opportunities.length} items` },
      { id: 'high-confidence', label: 'High Confidence', detail: `${highConfidenceCount} opportunities` },
      { id: 'due-soon', label: 'Due Soon', detail: `${dueSoonCount} windows in 14 days` },
      { id: 'follow-up', label: 'Follow Up', detail: `${Math.max(0, opportunities.length - highConfidenceCount)} needs review` },
    ];

    sendJson(res, 200, {
      tenantId,
      generatedAt: new Date().toISOString(),
      criticalAlerts,
      quickActions,
      opportunities,
    });
  } catch (error) {
    const normalized = normalizeError(error, 'Failed to load mobile today feed');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
