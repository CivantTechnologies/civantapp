/**
 * attach_renewal_signals.ts
 * 
 * Pipeline step that calls get_renewal_signals() database function
 * and feeds contract renewal signals into the prediction engine.
 * 
 * Signal types:
 * - framework_expiry (0.92 confidence)
 * - repeat_buyer_cycle (0.88 confidence)
 * - contract_renewal (0.75 confidence)
 */

import { PIPELINE_NAMES } from './models.ts';

type CivantClient = ReturnType<typeof import('../civantSdk.ts').createClientFromRequest>;

type RenewalSignal = {
  signal_type: string;
  buyer_id: string;
  buyer_name: string;
  cpv_cluster: string;
  country: string;
  confidence: number;
  urgency: string;
  days_until_expiry: number;
  predicted_tender_date: string;
  latest_contract_end: string;
  total_contracts: number;
  expiring_count: number;
  avg_duration_months: number;
  total_value_eur: number;
  avg_value_eur: number;
  distinct_suppliers: number;
  incumbent_suppliers: string[];
  has_frameworks: boolean;
  expiring_contracts: Array<{
    supplier: string;
    value_eur: number;
    start_date: string;
    end_date: string;
    duration_months: number;
    is_framework: boolean;
  }>;
};

type RenewalResponse = {
  success: boolean;
  total_signals: number;
  signals: RenewalSignal[];
  summary: Record<string, unknown>;
};

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
}

/**
 * Maps CPV cluster names to CPV family codes for compatibility
 * with the existing prediction schema
 */
function cpvClusterToFamily(cluster: string): string {
  const map: Record<string, string> = {
    'cluster_it_software': '72',
    'cluster_digital': '72',
    'cluster_consulting': '79',
    'cluster_construction': '45',
    'cluster_facilities': '50',
    'cluster_transport': '60',
    'cluster_health': '33',
    'cluster_education': '80',
    'cluster_environment': '90',
    'cluster_food': '15',
    'cluster_security': '79',
    'cluster_telecom': '64',
    'cluster_energy': '09',
    'cluster_finance': '66',
    'cluster_legal': '79',
    'cluster_marketing': '79',
    'cluster_hr': '79',
    'cluster_research': '73',
  };
  return map[cluster] || cluster.replace('cluster_', '').slice(0, 2);
}

/**
 * Fetches renewal signals from the database function via Supabase RPC.
 */
async function fetchRenewalSignals(
  monthsAhead: number = 18,
  minValueEur: number = 0
): Promise<RenewalResponse> {
  const supabaseUrl = Deno.env.get('SUPABASE_URL') || Deno.env.get('VITE_SUPABASE_URL');
  const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

  if (!supabaseUrl || !serviceRoleKey) {
    console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
    return { success: false, total_signals: 0, signals: [], summary: {} };
  }

  const rpcUrl = `${supabaseUrl}/rest/v1/rpc/get_renewal_signals`;
  const response = await fetch(rpcUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${serviceRoleKey}`,
      'apikey': serviceRoleKey,
      'Prefer': 'return=representation'
    },
    body: JSON.stringify({
      p_months_ahead: monthsAhead,
      p_min_value_eur: minValueEur
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error('get_renewal_signals RPC failed:', response.status, errorText);
    return { success: false, total_signals: 0, signals: [], summary: {} };
  }

  return await response.json() as RenewalResponse;
}

/**
 * Pipeline step: Inserts renewal signals into market_signals table
 * for downstream consumption by generate_predictions.
 */
export async function attach_renewal_signals(
  civant: CivantClient,
  params: {
    tenant_id: string;
    months_ahead?: number;
    min_value_eur?: number;
  }
): Promise<{ inserted: number; skipped: number; errors: number }> {
  const renewals = await fetchRenewalSignals(
    params.months_ahead || 18,
    params.min_value_eur || 0
  );

  if (!renewals.success || !renewals.signals.length) {
    console.warn('No renewal signals retrieved');
    return { inserted: 0, skipped: 0, errors: 0 };
  }

  let inserted = 0;
  let skipped = 0;
  let errors = 0;

  for (const signal of renewals.signals) {
    try {
      // Check for existing signal to avoid duplicates
      const existing = await civant.asServiceRole.entities[PIPELINE_NAMES.marketSignals].filter({
        tenant_id: params.tenant_id,
        signal_type: signal.signal_type,
        entity_id: signal.buyer_id,
      });

      // Skip if we already have a signal for this buyer + type
      // (will be refreshed on next pipeline run)
      if (existing.length > 0) {
        skipped += 1;
        continue;
      }

      await civant.asServiceRole.entities[PIPELINE_NAMES.marketSignals].create({
        id: makeId('rsig'),
        tenant_id: params.tenant_id,
        signal_type: signal.signal_type,
        entity_id: signal.buyer_id,
        source_url: null,
        source_quality: 0.95, // Award data is high quality (official records)
        signal_strength: signal.confidence,
        start_date: signal.latest_contract_end,
        end_date: signal.predicted_tender_date,
        evidence_snippet: `${signal.buyer_name}: ${signal.expiring_count} ${signal.cpv_cluster} contract(s) expiring ${signal.urgency} (${signal.days_until_expiry} days). €${Math.round(signal.total_value_eur).toLocaleString()} total value. ${signal.distinct_suppliers} incumbent supplier(s).`,
        extracted_json: signal
      });
      inserted += 1;
    } catch (err) {
      console.error('Failed to insert renewal signal:', err);
      errors += 1;
    }
  }

  console.log(`Renewal signals: ${inserted} inserted, ${skipped} skipped (existing), ${errors} errors`);
  return { inserted, skipped, errors };
}

/**
 * Generates predictions directly from renewal signals.
 * This handles buyer-category pairs that don't exist in tender_features_weekly
 * but DO have expiring contracts — net-new predictions from award data.
 */
export async function generate_renewal_predictions(
  civant: CivantClient,
  params: {
    tenant_id: string;
    model_version?: string;
  }
): Promise<{ created: number }> {
  const renewals = await fetchRenewalSignals(18, 0);

  if (!renewals.success || !renewals.signals.length) {
    return { created: 0 };
  }

  // Check which buyer-category pairs already have predictions
  const existingPredictions = await civant.asServiceRole.entities[PIPELINE_NAMES.predictions].filter({
    tenant_id: params.tenant_id
  }, '-generated_at', 50000);

  const existingKeys = new Set(
    existingPredictions.map((p: Record<string, unknown>) =>
      `${p.buyer_id || 'unknown'}|${p.cpv_family || 'unknown'}`
    )
  );

  let created = 0;

  for (const signal of renewals.signals) {
    const cpvFamily = cpvClusterToFamily(signal.cpv_cluster);
    const key = `${signal.buyer_id}|${cpvFamily}`;

    // Skip if feature-based prediction already exists for this pair
    if (existingKeys.has(key)) continue;

    // Calculate probability from signal data
    const baseProbability = signal.confidence;
    const urgencyBoost = signal.urgency === 'imminent' ? 0.10
      : signal.urgency === 'upcoming' ? 0.05
      : 0;
    const historyBoost = Math.min(signal.total_contracts / 10, 0.10);
    const probability = Math.min(0.98, baseProbability + urgencyBoost + historyBoost);

    const drivers: string[] = [
      `Contract end date: ${signal.latest_contract_end} (${signal.days_until_expiry} days)`,
      `Signal type: ${signal.signal_type}`,
      `${signal.total_contracts} historical contracts with this buyer`,
      `${signal.expiring_count} contract(s) expiring in window`,
      `€${Math.round(signal.total_value_eur).toLocaleString()} total value`,
      `${signal.distinct_suppliers} incumbent supplier(s)`,
      signal.has_frameworks ? 'Framework agreement detected' : 'Standard contract',
      `Average duration: ${signal.avg_duration_months} months`,
    ];

    const evidence = (signal.expiring_contracts || []).slice(0, 5).map(c => ({
      source_url: null,
      evidence_snippet: `${c.supplier}: €${c.value_eur?.toLocaleString() || 'N/A'}, ends ${c.end_date}`,
      signal_type: signal.signal_type
    }));

    await civant.asServiceRole.entities[PIPELINE_NAMES.predictions].create({
      id: makeId('rpred'),
      tenant_id: params.tenant_id,
      buyer_id: signal.buyer_id,
      buyer_name: signal.buyer_name,
      country: signal.country,
      category: signal.cpv_cluster,
      cpv_family: cpvFamily,
      time_window: 'next_18_months',
      probability,
      confidence: signal.confidence,
      confidence_breakdown: {
        data_confidence: signal.total_contracts >= 3 ? 35 : signal.total_contracts >= 2 ? 25 : 15,
        signal_confidence: signal.confidence * 30,
        model_confidence: 20,
        overall_confidence: Math.round(signal.confidence * 100),
        drivers
      },
      top_drivers: drivers.slice(0, 5),
      evidence,
      predicted_tender_date: signal.predicted_tender_date,
      signal_type: signal.signal_type,
      urgency: signal.urgency,
      total_value_eur: signal.total_value_eur,
      renewal_source: {
        buyer_name: signal.buyer_name,
        cpv_cluster: signal.cpv_cluster,
        country: signal.country,
        days_until_expiry: signal.days_until_expiry,
        incumbent_suppliers: signal.incumbent_suppliers,
        expiring_contracts: signal.expiring_contracts,
        has_frameworks: signal.has_frameworks,
        avg_duration_months: signal.avg_duration_months
      },
      model_version: params.model_version || 'renewal-v1',
      generated_at: new Date().toISOString()
    });

    created += 1;
    existingKeys.add(key); // Prevent duplicates within this run
  }

  return { created };
}
