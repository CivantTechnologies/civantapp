import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { format, formatDistanceToNow } from 'date-fns';
import {
  AlertTriangle,
  Building2,
  Calendar,
  ChevronDown,
  ChevronUp,
  Clock,
  Filter,
  Loader2,
  RefreshCw,
  Shield,
  Target,
  Timer,
  TrendingUp,
  Users,
} from 'lucide-react';
import { civant } from '@/api/civantClient';
import { supabase } from '@/lib/supabaseClient';
import { useTenant } from '@/lib/tenant';
import { createPageUrl } from '../utils';
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Page,
  PageBody,
  PageDescription,
  PageHeader,
  PageTitle,
} from '@/components/ui';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

/* ─────────────────────────── constants ─────────────────────────── */

const COUNTRY_OPTIONS = [
  { value: 'all', label: 'All Countries' },
  { value: 'IE', label: '🇮🇪 Ireland' },
  { value: 'FR', label: '🇫🇷 France' },
  { value: 'ES', label: '🇪🇸 Spain' },
];

const COUNTRY_FLAGS = { IE: '🇮🇪', FR: '🇫🇷', ES: '🇪🇸' };

const URGENCY_OPTIONS = [
  { value: 'all', label: 'All Urgency' },
  { value: 'imminent', label: 'Imminent' },
  { value: 'upcoming', label: 'Upcoming' },
  { value: 'planned', label: 'Planned' },
];

const SIGNAL_TYPE_LABELS = {
  framework_expiry: 'Framework Expiry',
  repeat_buyer_cycle: 'Repeat Buyer Cycle',
  contract_renewal: 'Contract Renewal',
};

const WINDOW_LABELS = {
  next_30: 'Next 30 days',
  m1_3: '1–3 months',
  m3_6: '3–6 months',
  m6_12: '6–12 months',
};

const VIEW_TABS = [
  { value: 'renewals', label: 'Renewal Signals' },
  { value: 'engine', label: 'Predictive Engine' },
];

/* ─────────────────────────── helpers ─────────────────────────── */

function getUrgencyStyle(urgency) {
  switch (String(urgency || '').toLowerCase()) {
    case 'imminent':
      return {
        bg: 'bg-rose-500/12 border-rose-400/30',
        text: 'text-rose-300',
        icon: AlertTriangle,
        pulse: true,
      };
    case 'upcoming':
      return {
        bg: 'bg-amber-500/12 border-amber-400/30',
        text: 'text-amber-300',
        icon: Timer,
        pulse: false,
      };
    case 'planned':
      return {
        bg: 'bg-blue-500/12 border-blue-400/30',
        text: 'text-blue-300',
        icon: Calendar,
        pulse: false,
      };
    default:
      return {
        bg: 'bg-slate-500/12 border-slate-400/25',
        text: 'text-slate-400',
        icon: Clock,
        pulse: false,
      };
  }
}

function getSignalTypeBadge(signalType) {
  switch (signalType) {
    case 'framework_expiry':
      return 'bg-purple-500/12 text-purple-300 border-purple-400/30';
    case 'repeat_buyer_cycle':
      return 'bg-civant-teal/12 text-civant-teal border-civant-teal/30';
    case 'contract_renewal':
      return 'bg-blue-500/12 text-blue-300 border-blue-400/30';
    default:
      return 'bg-slate-500/12 text-slate-400 border-slate-400/25';
  }
}

function getConfidenceBadgeClass(band) {
  switch (String(band || '').toLowerCase()) {
    case 'very high':
      return 'bg-civant-teal/15 text-civant-teal border-civant-teal/40';
    case 'high':
      return 'bg-blue-500/15 text-blue-300 border-blue-400/35';
    case 'medium':
      return 'bg-amber-500/15 text-amber-300 border-amber-400/35';
    default:
      return 'bg-slate-500/15 text-slate-300 border-slate-400/30';
  }
}

function formatPct(value) {
  return `${Math.round(Number(value || 0) * 100)}%`;
}

function formatDate(value) {
  if (!value) return 'N/A';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return format(d, 'MMM d, yyyy');
}

function formatValue(eur) {
  const n = Number(eur || 0);
  if (n >= 1_000_000) return `€${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `€${(n / 1_000).toFixed(0)}K`;
  if (n > 0) return `€${n.toLocaleString()}`;
  return '—';
}

function daysAway(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return null;
  return Math.ceil((d - new Date()) / (1000 * 60 * 60 * 24));
}

function toMap(rows, keyField) {
  const out = new Map();
  (rows || []).forEach((row) => {
    const key = row?.[keyField];
    if (key) out.set(key, row);
  });
  return out;
}

function toGroupedDrivers(rows) {
  const out = new Map();
  (rows || []).forEach((row) => {
    const key = row?.prediction_id;
    if (!key) return;
    const list = out.get(key) || [];
    list.push(row);
    out.set(key, list);
  });
  for (const [key, list] of out.entries()) {
    list.sort((a, b) => Number(b?.contribution || 0) - Number(a?.contribution || 0));
    out.set(key, list);
  }
  return out;
}

/* ─────────────────────── Renewal Prediction Card ─────────────────────── */

function RenewalCard({ prediction, defaultExpanded = false }) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const urgencyStyle = getUrgencyStyle(prediction.urgency);
  const UrgencyIcon = urgencyStyle.icon;
  const days = daysAway(prediction.predicted_tender_date);
  const renewalSource = prediction.renewal_source || {};
  const incumbents = renewalSource.incumbent_suppliers || [];
  const expiringContracts = renewalSource.expiring_contracts || [];
  const topDrivers = prediction.top_drivers || [];

  return (
    <Card className="bg-white/[0.02] border border-white/[0.06] hover:border-white/[0.12] transition-all duration-200">
      {/* Header row — always visible */}
      <div
        className="flex items-start gap-4 p-5 cursor-pointer select-none"
        onClick={() => setExpanded((v) => !v)}
      >
        {/* Urgency indicator */}
        <div
          className={`mt-0.5 shrink-0 flex items-center justify-center w-10 h-10 rounded-xl border ${urgencyStyle.bg}`}
        >
          <UrgencyIcon
            className={`w-5 h-5 ${urgencyStyle.text} ${urgencyStyle.pulse ? 'animate-pulse' : ''}`}
          />
        </div>

        {/* Main info */}
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-center gap-2 mb-1.5">
            <span className="text-lg">
              {COUNTRY_FLAGS[prediction.country] || '🌍'}
            </span>
            <Badge
              className={`text-[11px] font-medium border ${getSignalTypeBadge(prediction.signal_type)}`}
            >
              {SIGNAL_TYPE_LABELS[prediction.signal_type] || prediction.signal_type}
            </Badge>
            <Badge
              className={`text-[11px] font-medium border ${urgencyStyle.bg} ${urgencyStyle.text}`}
            >
              {String(prediction.urgency || 'unknown').charAt(0).toUpperCase() +
                String(prediction.urgency || 'unknown').slice(1)}
            </Badge>
          </div>

          <h3 className="text-base font-semibold text-slate-100 truncate">
            {prediction.buyer_name || prediction.buyer_id || 'Unknown Buyer'}
          </h3>
          <p className="text-sm text-slate-500 mt-0.5">
            {prediction.category || prediction.cpv_family} · {prediction.country}
          </p>
        </div>

        {/* Right-side stats */}
        <div className="hidden sm:flex items-center gap-6 shrink-0">
          <div className="text-right">
            <p className="text-[11px] uppercase tracking-wider text-slate-500">
              Predicted
            </p>
            <p className="text-sm font-semibold text-slate-200">
              {formatDate(prediction.predicted_tender_date)}
            </p>
            {days !== null && (
              <p
                className={`text-xs mt-0.5 ${
                  days <= 30
                    ? 'text-rose-400'
                    : days <= 90
                      ? 'text-amber-400'
                      : 'text-slate-500'
                }`}
              >
                {days <= 0 ? 'Overdue' : `${days}d away`}
              </p>
            )}
          </div>
          <div className="text-right">
            <p className="text-[11px] uppercase tracking-wider text-slate-500">
              Value
            </p>
            <p className="text-sm font-semibold text-civant-teal">
              {formatValue(prediction.total_value_eur)}
            </p>
          </div>
          <div className="text-right">
            <p className="text-[11px] uppercase tracking-wider text-slate-500">
              Confidence
            </p>
            <p className="text-sm font-semibold text-slate-200">
              {formatPct(prediction.probability || prediction.confidence)}
            </p>
          </div>
          {expanded ? (
            <ChevronUp className="w-4 h-4 text-slate-500" />
          ) : (
            <ChevronDown className="w-4 h-4 text-slate-500" />
          )}
        </div>
      </div>

      {/* Mobile stats row */}
      <div className="flex sm:hidden items-center gap-4 px-5 pb-3 text-xs">
        <span className="text-slate-400">
          {formatDate(prediction.predicted_tender_date)}
        </span>
        <span className="text-civant-teal font-medium">
          {formatValue(prediction.total_value_eur)}
        </span>
        <span className="text-slate-400">
          {formatPct(prediction.probability || prediction.confidence)}
        </span>
      </div>

      {/* Expanded detail */}
      {expanded && (
        <CardContent className="pt-0 pb-5 space-y-4 border-t border-white/[0.04]">
          {/* Metric tiles */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-4">
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
              <p className="text-[11px] text-slate-500 uppercase tracking-wider">
                Incumbents
              </p>
              <p className="text-lg font-semibold text-slate-200">
                {renewalSource.distinct_suppliers || incumbents.length || '—'}
              </p>
            </div>
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
              <p className="text-[11px] text-slate-500 uppercase tracking-wider">
                Expiring
              </p>
              <p className="text-lg font-semibold text-slate-200">
                {renewalSource.expiring_count ||
                  expiringContracts.length ||
                  '—'}{' '}
                <span className="text-xs font-normal text-slate-500">
                  contracts
                </span>
              </p>
            </div>
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
              <p className="text-[11px] text-slate-500 uppercase tracking-wider">
                Avg Duration
              </p>
              <p className="text-lg font-semibold text-slate-200">
                {renewalSource.avg_duration_months
                  ? `${Math.round(renewalSource.avg_duration_months)}mo`
                  : '—'}
              </p>
            </div>
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
              <p className="text-[11px] text-slate-500 uppercase tracking-wider">
                Framework
              </p>
              <p className="text-lg font-semibold text-slate-200">
                {renewalSource.has_frameworks ? (
                  <span className="text-civant-teal">Yes</span>
                ) : (
                  'No'
                )}
              </p>
            </div>
          </div>

          {/* Incumbent suppliers */}
          {incumbents.length > 0 && (
            <div>
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500 mb-2 flex items-center gap-1.5">
                <Users className="w-3.5 h-3.5" /> Incumbent Suppliers
              </p>
              <div className="flex flex-wrap gap-1.5">
                {incumbents.map((name, i) => (
                  <Badge
                    key={i}
                    className="bg-white/[0.04] text-slate-300 border border-white/[0.08] text-xs"
                  >
                    {name}
                  </Badge>
                ))}
              </div>
            </div>
          )}

          {/* Top drivers */}
          {topDrivers.length > 0 && (
            <div>
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500 mb-2">
                Prediction Drivers
              </p>
              <div className="space-y-1.5">
                {topDrivers.slice(0, 5).map((driver, idx) => (
                  <div
                    key={idx}
                    className="text-sm text-slate-400 flex items-start gap-2"
                  >
                    <span className="text-slate-600 mt-0.5">•</span>
                    <span>{driver}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Expiring contracts table */}
          {expiringContracts.length > 0 && (
            <div>
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500 mb-2">
                Expiring Contracts
              </p>
              <div className="rounded-xl border border-white/[0.06] overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-white/[0.04] text-[11px] text-slate-500 uppercase tracking-wider">
                      <th className="text-left px-3 py-2">Supplier</th>
                      <th className="text-right px-3 py-2">Value</th>
                      <th className="text-right px-3 py-2">End Date</th>
                      <th className="text-center px-3 py-2">Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {expiringContracts.slice(0, 5).map((c, idx) => (
                      <tr
                        key={idx}
                        className="border-b border-white/[0.03] last:border-0"
                      >
                        <td className="px-3 py-2 text-slate-300">
                          {c.supplier}
                        </td>
                        <td className="px-3 py-2 text-right text-slate-400">
                          {formatValue(c.value_eur)}
                        </td>
                        <td className="px-3 py-2 text-right text-slate-400">
                          {formatDate(c.end_date)}
                        </td>
                        <td className="px-3 py-2 text-center">
                          {c.is_framework ? (
                            <Badge className="bg-purple-500/10 text-purple-300 border-purple-400/25 text-[10px]">
                              Framework
                            </Badge>
                          ) : (
                            <span className="text-slate-500 text-xs">
                              Standard
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Action */}
          <div className="flex items-center justify-between pt-1">
            <div className="text-xs text-slate-500 flex items-center gap-2">
              <Calendar className="w-3.5 h-3.5" />
              Generated {formatDate(prediction.generated_at)}
              <span className="text-slate-600">·</span>
              {prediction.model_version || 'renewal-v1'}
            </div>
            <Button variant="outline" className="border-white/[0.08]" asChild>
              <Link
                to={createPageUrl(
                  `search?buyer=${encodeURIComponent(
                    prediction.buyer_name || prediction.buyer_id || ''
                  )}`
                )}
              >
                View related tenders
              </Link>
            </Button>
          </div>
        </CardContent>
      )}
    </Card>
  );
}

/* ─────────────────── Engine Prediction Card (existing) ─────────────────── */

function EnginePredictionCard({ prediction, scorecard, drivers }) {
  return (
    <Card className="bg-white/[0.02] border border-white/[0.06]">
      <CardHeader className="pb-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <div className="flex flex-wrap items-center gap-2 mb-2">
              <span className="text-xl">
                {COUNTRY_FLAGS[prediction.region] || '🌍'}
              </span>
              <Badge className={getConfidenceBadgeClass(prediction.confidence_band)}>
                {prediction.confidence_band}
              </Badge>
              <Badge
                variant="outline"
                className="border-white/[0.08] text-slate-400"
              >
                {WINDOW_LABELS[prediction.next_window_label] ||
                  prediction.next_window_label}
              </Badge>
            </div>
            <CardTitle className="text-card-foreground text-xl">
              {prediction.buyer_display_name || prediction.buyer_entity_id}
            </CardTitle>
            <p className="text-sm text-slate-500 mt-1">
              {prediction.cpv_cluster_label || prediction.cpv_cluster_id} ·{' '}
              {prediction.region}
            </p>
          </div>
          <Button variant="outline" className="border-white/[0.08]" asChild>
            <Link
              to={createPageUrl(
                `search?buyer=${encodeURIComponent(
                  prediction.buyer_display_name ||
                    prediction.buyer_entity_id ||
                    ''
                )}`
              )}
            >
              View related tenders
            </Link>
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          {[
            ['Probability', formatPct(prediction.probability)],
            ['Confidence', `${Number(prediction.confidence || 0)}%`],
            ['Forecast Score', Number(prediction.forecast_score || 0)],
            [
              'Expected Window',
              `${formatDate(prediction.expected_window_start)} – ${formatDate(prediction.expected_window_end)}`,
              true,
            ],
            ['Fallback Tier', Number(prediction.fallback_tier || 0)],
          ].map(([label, value, small], idx) => (
            <div
              key={idx}
              className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3"
            >
              <p className="text-[11px] text-slate-500">{label}</p>
              <p
                className={`${small ? 'text-sm' : 'text-lg'} font-semibold text-slate-200`}
              >
                {value}
              </p>
            </div>
          ))}
        </div>

        {scorecard && (
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-2 text-xs">
            {[
              ['Cycle', scorecard.cycle_score],
              ['Timing', scorecard.timing_score],
              ['Behaviour', scorecard.behavioural_score],
              ['Structural', scorecard.structural_score],
              ['External', scorecard.external_signal_score],
              ['Data quality', scorecard.data_quality_score],
            ].map(([label, score], idx) => (
              <div
                key={idx}
                className="rounded border border-white/[0.06] p-2 text-slate-400"
              >
                {label} {score}
              </div>
            ))}
          </div>
        )}

        <div>
          <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500 mb-2">
            Top Drivers
          </p>
          {(drivers || []).length === 0 ? (
            <p className="text-sm text-slate-500">No driver details yet.</p>
          ) : (
            <div className="space-y-2">
              {drivers.slice(0, 3).map((driver, idx) => (
                <div
                  key={`${prediction.prediction_id}-${driver.driver_type}-${idx}`}
                  className="rounded-xl border border-white/[0.06] p-3"
                >
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-sm font-medium text-slate-200">
                      {driver.label}
                    </p>
                    <Badge
                      variant="outline"
                      className="border-white/[0.08] text-slate-400"
                    >
                      {Number(driver.contribution || 0).toFixed(3)}
                    </Badge>
                  </div>
                  <p className="mt-1 text-sm text-slate-400">
                    {driver.narrative}
                  </p>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="text-xs text-slate-500 flex items-center gap-2">
          <Calendar className="w-3.5 h-3.5" />
          Last computed {formatDate(prediction.last_computed_at)}
          <Clock className="w-3.5 h-3.5 ml-2" />
          {Number(prediction.drivers_count || 0)} drivers ·{' '}
          {Number(prediction.evidence_count || 0)} evidence refs
        </div>
      </CardContent>
    </Card>
  );
}

/* ════════════════════════════ MAIN PAGE ════════════════════════════ */

export default function Predictions() {
  const { activeTenantId, isLoadingTenants } = useTenant();

  // Filters
  const [activeView, setActiveView] = useState('renewals');
  const [selectedCountry, setSelectedCountry] = useState('all');
  const [selectedUrgency, setSelectedUrgency] = useState('all');

  // Data — renewals
  const [renewals, setRenewals] = useState([]);
  const [loadingRenewals, setLoadingRenewals] = useState(true);
  const [companyProfile, setCompanyProfile] = useState(null);

  // Data — engine predictions (existing)
  const [predictions, setPredictions] = useState([]);
  const [scorecardsByPrediction, setScorecardsByPrediction] = useState(new Map());
  const [driversByPrediction, setDriversByPrediction] = useState(new Map());
  const [latestRuns, setLatestRuns] = useState([]);
  const [loadingEngine, setLoadingEngine] = useState(true);

  /* ───── Load company profile ───── */
  useEffect(() => {
    if (isLoadingTenants || !activeTenantId) return;
    (async () => {
      try {
        const { data } = await supabase
          .from('company_profiles')
          .select(
            'target_cpv_clusters,target_countries,target_buyer_types,contract_size_min_eur,contract_size_max_eur'
          )
          .eq('tenant_id', activeTenantId)
          .maybeSingle();
        setCompanyProfile(data || null);
      } catch (e) {
        console.error('Failed to load company profile:', e);
      }
    })();
  }, [activeTenantId, isLoadingTenants]);

  /* ───── Load renewal predictions ───── */
  useEffect(() => {
    if (isLoadingTenants || !activeTenantId) return;
    void loadRenewals();
  }, [activeTenantId, isLoadingTenants, selectedCountry, selectedUrgency]);

  const loadRenewals = async () => {
    setLoadingRenewals(true);
    try {
      let query = supabase
        .from('predictions')
        .select('*')
        .eq('tenant_id', activeTenantId)
        .not('signal_type', 'is', null)
        .order('predicted_tender_date', { ascending: true })
        .limit(300);

      if (selectedCountry !== 'all') {
        query = query.eq('country', selectedCountry);
      }
      if (selectedUrgency !== 'all') {
        query = query.eq('urgency', selectedUrgency);
      }

      const { data, error } = await query;
      if (error) throw error;

      let rows = data || [];

      // Filter by company profile if available
      if (companyProfile) {
        const targetClusters = companyProfile.target_cpv_clusters || [];
        const targetCountries = companyProfile.target_countries || [];
        const minVal = companyProfile.contract_size_min_eur || 0;
        const maxVal = companyProfile.contract_size_max_eur || 0;

        rows = rows.filter((row) => {
          // Country filter
          if (
            targetCountries.length > 0 &&
            !targetCountries.includes(row.country)
          ) {
            return false;
          }
          // CPV cluster filter — match category field against target clusters
          if (targetClusters.length > 0 && row.category) {
            if (!targetClusters.includes(row.category)) return false;
          }
          // Contract value filter
          if (minVal > 0 && Number(row.total_value_eur || 0) < minVal) {
            return false;
          }
          if (maxVal > 0 && Number(row.total_value_eur || 0) > maxVal) {
            return false;
          }
          return true;
        });
      }

      setRenewals(rows);
    } catch (e) {
      console.error('Failed to load renewal predictions:', e);
      setRenewals([]);
    } finally {
      setLoadingRenewals(false);
    }
  };

  /* ───── Load engine predictions (existing logic) ───── */
  useEffect(() => {
    if (isLoadingTenants || !activeTenantId) return;
    void loadEngine();
  }, [activeTenantId, isLoadingTenants, selectedCountry]);

  const loadEngine = async () => {
    setLoadingEngine(true);
    try {
      const baseQuery = { tenant_id: activeTenantId };
      if (selectedCountry !== 'all') baseQuery.region = selectedCountry;

      const predictionFields = [
        'prediction_id',
        'buyer_entity_id',
        'buyer_display_name',
        'cpv_cluster_id',
        'cpv_cluster_label',
        'region',
        'next_window_label',
        'expected_window_start',
        'expected_window_end',
        'probability',
        'confidence',
        'confidence_band',
        'forecast_score',
        'fallback_tier',
        'drivers_count',
        'evidence_count',
        'last_computed_at',
      ].join(',');

      const predictionRows = await civant.entities.predictions_current.filter(
        baseQuery,
        '-forecast_score,-probability',
        250,
        0,
        predictionFields
      );
      setPredictions(Array.isArray(predictionRows) ? predictionRows : []);

      const ids = (predictionRows || [])
        .map((r) => r?.prediction_id)
        .filter(Boolean);

      if (ids.length > 0) {
        const [scorecards, drivers] = await Promise.all([
          civant.entities.prediction_scorecard.filter(
            { tenant_id: activeTenantId, prediction_id: ids },
            '-total_score',
            500,
            0,
            'prediction_id,total_score,cycle_score,timing_score,behavioural_score,structural_score,external_signal_score,data_quality_score,created_at'
          ),
          civant.entities.prediction_drivers.filter(
            { tenant_id: activeTenantId, prediction_id: ids },
            '-contribution',
            2000,
            0,
            'prediction_id,driver_type,label,contribution,narrative,evidence_refs,created_at'
          ),
        ]);
        setScorecardsByPrediction(toMap(scorecards || [], 'prediction_id'));
        setDriversByPrediction(toGroupedDrivers(drivers || []));
      } else {
        setScorecardsByPrediction(new Map());
        setDriversByPrediction(new Map());
      }

      const runs = await civant.entities.prediction_runs.filter(
        { tenant_id: activeTenantId },
        '-started_at',
        20,
        0,
        'run_id,run_type,status,started_at,finished_at,pairs_processed,error_message'
      );
      setLatestRuns(Array.isArray(runs) ? runs : []);
    } catch (e) {
      console.error('Failed to load engine data:', e);
      setPredictions([]);
      setScorecardsByPrediction(new Map());
      setDriversByPrediction(new Map());
      setLatestRuns([]);
    } finally {
      setLoadingEngine(false);
    }
  };

  /* ───── Stats ───── */
  const renewalStats = useMemo(() => {
    const imminent = renewals.filter((r) => r.urgency === 'imminent').length;
    const upcoming = renewals.filter((r) => r.urgency === 'upcoming').length;
    const totalValue = renewals.reduce(
      (sum, r) => sum + Number(r.total_value_eur || 0),
      0
    );
    return { total: renewals.length, imminent, upcoming, totalValue };
  }, [renewals]);

  const engineStats = useMemo(() => {
    const high = predictions.filter(
      (r) => Number(r?.confidence || 0) >= 60
    ).length;
    const medium = predictions.filter((r) => {
      const c = Number(r?.confidence || 0);
      return c >= 40 && c < 60;
    }).length;
    return { total: predictions.length, high, medium };
  }, [predictions]);

  const latestRun = latestRuns[0] || null;
  const loading = activeView === 'renewals' ? loadingRenewals : loadingEngine;

  /* ═══════════════════════════ RENDER ═══════════════════════════ */
  return (
    <Page>
      <PageHeader>
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <PageTitle>Tender Panorama</PageTitle>
            <PageDescription>
              {activeView === 'renewals'
                ? 'Contract renewal signals from award data — filtered to your company profile.'
                : 'Deterministic forecast outputs from the scheduled predictive engine.'}
            </PageDescription>
          </div>
          <div className="flex items-center gap-3">
            {/* View toggle */}
            <div className="flex rounded-lg border border-white/[0.08] bg-white/[0.02] p-0.5">
              {VIEW_TABS.map((tab) => (
                <button
                  key={tab.value}
                  onClick={() => setActiveView(tab.value)}
                  className={`px-3 py-1.5 text-sm rounded-md transition-all ${
                    activeView === tab.value
                      ? 'bg-civant-teal/15 text-civant-teal font-medium'
                      : 'text-slate-400 hover:text-slate-300'
                  }`}
                >
                  {tab.label}
                </button>
              ))}
            </div>

            {/* Country filter */}
            <Select value={selectedCountry} onValueChange={setSelectedCountry}>
              <SelectTrigger className="w-[160px] bg-white/[0.02] border-white/[0.08] text-slate-300">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {COUNTRY_OPTIONS.map((o) => (
                  <SelectItem key={o.value} value={o.value}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            {/* Urgency filter — renewals only */}
            {activeView === 'renewals' && (
              <Select
                value={selectedUrgency}
                onValueChange={setSelectedUrgency}
              >
                <SelectTrigger className="w-[150px] bg-white/[0.02] border-white/[0.08] text-slate-300">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {URGENCY_OPTIONS.map((o) => (
                    <SelectItem key={o.value} value={o.value}>
                      {o.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          </div>
        </div>
      </PageHeader>

      <PageBody className="space-y-6">
        {/* ═══ Company profile notice ═══ */}
        {activeView === 'renewals' && companyProfile && (
          <div className="flex items-center gap-2 text-xs text-slate-500 px-1">
            <Filter className="w-3.5 h-3.5 text-civant-teal" />
            <span>
              Filtered by profile:{' '}
              {(companyProfile.target_countries || [])
                .map((c) => COUNTRY_FLAGS[c] || c)
                .join(' ')}{' '}
              · {(companyProfile.target_cpv_clusters || []).length} categories
              {companyProfile.contract_size_min_eur > 0 &&
                ` · Min ${formatValue(companyProfile.contract_size_min_eur)}`}
            </span>
            <Link
              to="/companyprofile"
              className="text-civant-teal hover:underline ml-1"
            >
              Edit
            </Link>
          </div>
        )}

        {/* ═══ RENEWALS VIEW ═══ */}
        {activeView === 'renewals' && (
          <>
            {/* Stat cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="pt-5 flex items-center justify-between">
                  <div>
                    <p className="text-[11px] text-slate-500 uppercase tracking-wider">
                      Total Signals
                    </p>
                    <p className="text-3xl font-bold text-slate-100">
                      {renewalStats.total}
                    </p>
                  </div>
                  <RefreshCw className="w-7 h-7 text-civant-teal opacity-60" />
                </CardContent>
              </Card>
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="pt-5 flex items-center justify-between">
                  <div>
                    <p className="text-[11px] text-slate-500 uppercase tracking-wider">
                      Imminent
                    </p>
                    <p className="text-3xl font-bold text-rose-400">
                      {renewalStats.imminent}
                    </p>
                  </div>
                  <AlertTriangle className="w-7 h-7 text-rose-400 opacity-60" />
                </CardContent>
              </Card>
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="pt-5 flex items-center justify-between">
                  <div>
                    <p className="text-[11px] text-slate-500 uppercase tracking-wider">
                      Upcoming
                    </p>
                    <p className="text-3xl font-bold text-amber-400">
                      {renewalStats.upcoming}
                    </p>
                  </div>
                  <Timer className="w-7 h-7 text-amber-400 opacity-60" />
                </CardContent>
              </Card>
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="pt-5 flex items-center justify-between">
                  <div>
                    <p className="text-[11px] text-slate-500 uppercase tracking-wider">
                      Pipeline Value
                    </p>
                    <p className="text-3xl font-bold text-civant-teal">
                      {formatValue(renewalStats.totalValue)}
                    </p>
                  </div>
                  <TrendingUp className="w-7 h-7 text-civant-teal opacity-60" />
                </CardContent>
              </Card>
            </div>

            {/* Loading / empty / list */}
            {loadingRenewals && (
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="py-12 flex items-center justify-center gap-3 text-slate-400">
                  <Loader2 className="w-5 h-5 animate-spin" />
                  Loading renewal predictions…
                </CardContent>
              </Card>
            )}

            {!loadingRenewals && renewals.length === 0 && (
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="py-12 text-center space-y-2">
                  <p className="text-slate-400">
                    No renewal signals match your current filters.
                  </p>
                  <p className="text-xs text-slate-500">
                    Check your{' '}
                    <Link
                      to="/companyprofile"
                      className="text-civant-teal hover:underline"
                    >
                      company profile
                    </Link>{' '}
                    settings or adjust the country/urgency filters above.
                  </p>
                </CardContent>
              </Card>
            )}

            {!loadingRenewals && renewals.length > 0 && (
              <div className="space-y-3">
                {renewals.map((r, idx) => (
                  <RenewalCard
                    key={r.id || idx}
                    prediction={r}
                    defaultExpanded={idx === 0}
                  />
                ))}
              </div>
            )}
          </>
        )}

        {/* ═══ ENGINE VIEW (existing) ═══ */}
        {activeView === 'engine' && (
          <>
            {/* Latest run banner */}
            {latestRun && (
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="pt-5 flex flex-col gap-2 text-sm md:flex-row md:items-center md:justify-between">
                  <div className="flex items-center gap-2">
                    <Badge
                      className={
                        latestRun.status === 'success'
                          ? 'bg-civant-teal/15 text-civant-teal border-civant-teal/35'
                          : 'bg-rose-500/15 text-rose-300 border-rose-400/35'
                      }
                    >
                      {String(latestRun.status || 'unknown').toUpperCase()}
                    </Badge>
                    <span className="text-slate-400">
                      {latestRun.run_type} run
                    </span>
                  </div>
                  <div className="text-slate-400">
                    Last run:{' '}
                    {formatDate(latestRun.finished_at || latestRun.started_at)}{' '}
                    · Pairs: {Number(latestRun.pairs_processed || 0)}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Engine stat cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="pt-6 flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-400">High Confidence</p>
                    <p className="text-3xl font-bold text-slate-100">
                      {engineStats.high}
                    </p>
                  </div>
                  <Target className="w-8 h-8 text-civant-teal" />
                </CardContent>
              </Card>
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="pt-6 flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-400">Medium Confidence</p>
                    <p className="text-3xl font-bold text-slate-100">
                      {engineStats.medium}
                    </p>
                  </div>
                  <TrendingUp className="w-8 h-8 text-civant-teal" />
                </CardContent>
              </Card>
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="pt-6 flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-400">Total Forecasts</p>
                    <p className="text-3xl font-bold text-slate-100">
                      {engineStats.total}
                    </p>
                  </div>
                  <Building2 className="w-8 h-8 text-civant-teal" />
                </CardContent>
              </Card>
            </div>

            {/* Engine loading / empty / list */}
            {loadingEngine && (
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="py-12 flex items-center justify-center gap-3 text-slate-400">
                  <Loader2 className="w-5 h-5 animate-spin" />
                  Loading forecast outputs…
                </CardContent>
              </Card>
            )}

            {!loadingEngine && predictions.length === 0 && (
              <Card className="bg-white/[0.02] border border-white/[0.06]">
                <CardContent className="py-12 text-center text-slate-400">
                  No forecast rows yet for this filter. Run the scheduled
                  predictive jobs or execute a manual rollout.
                </CardContent>
              </Card>
            )}

            {!loadingEngine && predictions.length > 0 && (
              <div className="space-y-4">
                {predictions.map((p) => (
                  <EnginePredictionCard
                    key={p.prediction_id}
                    prediction={p}
                    scorecard={scorecardsByPrediction.get(p.prediction_id)}
                    drivers={(
                      driversByPrediction.get(p.prediction_id) || []
                    ).slice(0, 3)}
                  />
                ))}
              </div>
            )}
          </>
        )}
      </PageBody>
    </Page>
  );
}
