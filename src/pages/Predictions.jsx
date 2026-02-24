import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { differenceInCalendarDays, format, startOfDay } from 'date-fns';
import { Loader2 } from 'lucide-react';
import { supabase } from '@/lib/supabaseClient';
import { civant } from '@/api/civantClient';
import { useTenant } from '@/lib/tenant';
import {
  isCompanyScopeFilterTemporarilyDisabled,
  setCompanyScopeFilterTemporarilyDisabled
} from '@/lib/companyScopeSession';
import { createPageUrl } from '../utils';
import {
  Button,
  Card,
  CardContent,
  Page,
  PageBody,
  PageTitle
} from '@/components/ui';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select';
import ForecastTimeline from '@/components/forecast/ForecastTimeline';
import HomePlatformFooter from '@/components/home/HomePlatformFooter';

const COUNTRY_OPTIONS = [
  { value: 'all', label: 'All Countries' },
  { value: 'IE', label: '🇮🇪 Ireland' },
  { value: 'FR', label: '🇫🇷 France' },
  { value: 'ES', label: '🇪🇸 Spain' }
];

const URGENCY_OPTIONS = [
  { value: 'actionable', label: 'Actionable' },
  { value: 'upcoming', label: 'Upcoming' },
  { value: 'horizon', label: 'Horizon' },
  { value: 'distant', label: 'Distant' },
  { value: 'overdue', label: 'Overdue (Unverified)' },
  { value: 'all', label: 'All (incl. Overdue)' }
];

const SOURCE_OPTIONS = [
  { value: 'all', label: 'All Signals' },
  { value: 'contract_renewal', label: 'Contract Renewal' },
  { value: 'framework_expiry', label: 'Framework Expiry' },
  { value: 'repeat_buyer_cycle', label: 'Repeat Buyer Cycle' }
];

const CATEGORY_TO_PROFILE_CLUSTER = {
  cluster_digital: 'cluster_it_software',
  cluster_it_software: 'cluster_it_software',
  cluster_professional_services: 'cluster_consulting',
  cluster_consulting: 'cluster_consulting',
  cluster_construction: 'cluster_construction',
  cluster_facilities: 'cluster_facilities_maintenance',
  cluster_facilities_maintenance: 'cluster_facilities_maintenance',
  cluster_health: 'cluster_health_medical',
  cluster_health_medical: 'cluster_health_medical',
  cluster_education: 'cluster_education_training',
  cluster_education_training: 'cluster_education_training',
  cluster_transport: 'cluster_transport',
  cluster_food: 'cluster_food_catering',
  cluster_food_catering: 'cluster_food_catering',
  cluster_energy: 'cluster_energy_environment',
  cluster_energy_environment: 'cluster_energy_environment',
  cluster_environment: 'cluster_energy_environment',
  cluster_communications: 'cluster_communications_media',
  cluster_communications_media: 'cluster_communications_media',
  cluster_finance: 'cluster_financial_legal',
  cluster_financial_legal: 'cluster_financial_legal',
  cluster_legal: 'cluster_financial_legal',
  cluster_manufacturing: 'cluster_manufacturing',
  cluster_defence: 'cluster_defence_security',
  cluster_defence_security: 'cluster_defence_security',
  cluster_security: 'cluster_defence_security',
  cluster_research: 'cluster_research',
  digital: 'cluster_it_software',
  it: 'cluster_it_software',
  software: 'cluster_it_software',
  telecommunications: 'cluster_it_software',
  telecoms: 'cluster_it_software',
  'professional services': 'cluster_consulting',
  consulting: 'cluster_consulting',
  construction: 'cluster_construction',
  maintenance: 'cluster_facilities_maintenance',
  furniture: 'cluster_facilities_maintenance',
  installation: 'cluster_facilities_maintenance',
  electrical: 'cluster_facilities_maintenance',
  healthcare: 'cluster_health_medical',
  medical: 'cluster_health_medical',
  education: 'cluster_education_training',
  transport: 'cluster_transport',
  vehicles: 'cluster_transport',
  food: 'cluster_food_catering',
  hospitality: 'cluster_food_catering',
  energy: 'cluster_energy_environment',
  environmental: 'cluster_energy_environment',
  agriculture: 'cluster_energy_environment',
  utilities: 'cluster_energy_environment',
  financial: 'cluster_financial_legal',
  legal: 'cluster_financial_legal',
  culture: 'cluster_communications_media',
  printing: 'cluster_communications_media',
  publishing: 'cluster_communications_media',
  security: 'cluster_defence_security',
  research: 'cluster_research',
  scientific: 'cluster_research'
};

const SIGNAL_TYPE_LABELS = {
  framework_expiry: 'Framework expiry history',
  repeat_buyer_cycle: 'Repeat buyer cycle history',
  contract_renewal: 'Contract renewal pattern'
};

function normaliseCategory(category) {
  if (!category) return null;
  const raw = String(category).trim();
  if (!raw) return null;
  const key = raw in CATEGORY_TO_PROFILE_CLUSTER ? raw : raw.toLowerCase();
  return CATEGORY_TO_PROFILE_CLUSTER[key] || null;
}


function predictionDate(row) {
  return row?.predicted_window_start || row?.predicted_tender_date || row?.contract_end_date || null;
}

function predictionConfidencePercent(row) {
  const raw = Number(row?.probability ?? row?.confidence ?? 0);
  if (!Number.isFinite(raw)) return 0;
  if (raw <= 1) return Math.max(0, Math.min(100, Math.round(raw * 100)));
  return Math.max(0, Math.min(100, Math.round(raw)));
}

function formatCurrency(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) return '—';
  if (amount >= 1_000_000_000) return `€${(amount / 1_000_000_000).toFixed(1)}B`;
  if (amount >= 1_000_000) return `€${(amount / 1_000_000).toFixed(1)}M`;
  if (amount >= 1_000) return `€${(amount / 1_000).toFixed(0)}K`;
  return `€${Math.round(amount).toLocaleString()}`;
}

function formatRenewalWindow(row) {
  const startValue = row?.predicted_window_start || row?.predicted_tender_date;
  const endValue = row?.predicted_window_end || row?.predicted_tender_date;
  if (!startValue && !endValue) return 'Window pending';
  const start = startValue ? new Date(startValue) : null;
  const end = endValue ? new Date(endValue) : null;
  const validStart = start && !Number.isNaN(start.getTime()) ? start : null;
  const validEnd = end && !Number.isNaN(end.getTime()) ? end : null;
  if (validStart && validEnd && format(validStart, 'MMM yyyy') !== format(validEnd, 'MMM yyyy')) {
    return `${format(validStart, 'MMM yyyy')} – ${format(validEnd, 'MMM yyyy')}`;
  }
  const single = validStart || validEnd;
  return single ? format(single, 'MMM yyyy') : 'Window pending';
}

function cycleReference(row) {
  const avgCycleDays = Number(row?.avg_cycle_days || 0);
  if (Number.isFinite(avgCycleDays) && avgCycleDays > 0) {
    return `Avg cycle ${Math.round(avgCycleDays)} days`;
  }

  const drivers = Array.isArray(row?.top_drivers) ? row.top_drivers : [];
  if (drivers.length > 0) {
    const first = drivers[0];
    const label = typeof first === 'string'
      ? first
      : first?.label || first?.narrative || '';
    if (label) return label;
  }

  const signalLabel = SIGNAL_TYPE_LABELS[row?.signal_type];
  return signalLabel || 'Pattern baseline';
}

function buyerLabel(row) {
  return row?.buyer_name || row?.buyer_display_name || row?.buyer_entity_id || row?.buyer_id || 'Unknown buyer';
}

function sortPredictions(rows) {
  return [...rows].sort(comparePredictions);
}

function comparePredictions(a, b) {
  const dateA = predictionDate(a);
  const dateB = predictionDate(b);
  const parsedA = dateA ? new Date(dateA) : null;
  const parsedB = dateB ? new Date(dateB) : null;
  const timeA = parsedA && !Number.isNaN(parsedA.getTime()) ? parsedA.getTime() : Number.POSITIVE_INFINITY;
  const timeB = parsedB && !Number.isNaN(parsedB.getTime()) ? parsedB.getTime() : Number.POSITIVE_INFINITY;
  if (timeA !== timeB) return timeA - timeB;
  return predictionConfidencePercent(b) - predictionConfidencePercent(a);
}

function predictionMatchesCompanyScope(row, profile) {
  if (!profile) return true;

  const clusters = Array.isArray(profile.target_cpv_clusters) ? profile.target_cpv_clusters : [];
  const countries = Array.isArray(profile.target_countries) ? profile.target_countries : [];
  const minValue = Number(profile.contract_size_min_eur || 0);
  const maxValue = Number(profile.contract_size_max_eur || 0);

  const rowCountry = row.country || row.region;
  if (countries.length > 0 && rowCountry && !countries.includes(rowCountry)) return false;

  if (clusters.length > 0 && row.category) {
    const mapped = normaliseCategory(row.category);
    if (mapped && !clusters.includes(mapped)) return false;
  }

  const amount = Number(row.total_value_eur || 0);
  if (minValue > 0 && amount > 0 && amount < minValue) return false;
  if (maxValue > 0 && amount > 0 && amount > maxValue) return false;
  return true;
}

function priorityTimeWeight(row) {
  const dateValue = predictionDate(row);
  if (!dateValue) return 0.75;

  const predictedDate = new Date(dateValue);
  if (Number.isNaN(predictedDate.getTime())) return 0.75;

  const daysUntilWindow = differenceInCalendarDays(startOfDay(predictedDate), startOfDay(new Date()));
  if (daysUntilWindow <= 30) return 1.0;
  if (daysUntilWindow <= 90) return 0.9;
  return 0.75;
}

function strategicWeight(row) {
  const raw = Number(row?.strategic_weight ?? row?.strategic_priority_weight ?? 1);
  return Number.isFinite(raw) && raw > 0 ? raw : 1;
}

function priorityScore(row) {
  const prob = Number(row?.probability ?? 0);
  const conf = Number(row?.confidence ?? 0);
  const tw = priorityTimeWeight(row);
  const sw = strategicWeight(row);

  if (!Number.isFinite(prob) || prob <= 0) return 0;
  if (!Number.isFinite(conf) || conf <= 0) return 0;

  return prob * conf * tw * sw;
}

function SummaryTile({ label, value, hint }) {
  return (
    <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
      <CardContent className="px-4 py-4">
        <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">{label}</p>
        <p className="mt-1 text-4xl font-semibold tracking-tight text-card-foreground tabular-nums">{value}</p>
        {hint ? <p className="mt-1 text-xs text-muted-foreground/85">{hint}</p> : null}
      </CardContent>
    </Card>
  );
}

export default function Predictions() {
  const { activeTenantId, isLoadingTenants } = useTenant();
  const [countryFilter, setCountryFilter] = useState('all');
  const [urgencyFilter, setUrgencyFilter] = useState('actionable');
  const [sourceFilter, setSourceFilter] = useState('all');
  const [allPredictions, setAllPredictions] = useState([]);
  const [companyProfile, setCompanyProfile] = useState(null);
  const [scopeFilterTemporarilyDisabled, setScopeFilterTemporarilyDisabledState] = useState(() => (
    isCompanyScopeFilterTemporarilyDisabled(activeTenantId)
  ));
  const [loading, setLoading] = useState(true);
  const [validationStats, setValidationStats] = useState(null);
  const [agentBriefs, setAgentBriefs] = useState({});
  const [agentLoading, setAgentLoading] = useState({});
  const [priorityPage, setPriorityPage] = useState(1);
  const PRIORITY_PAGE_SIZE = 20;
  const persistedScopeFilterEnabled = companyProfile?.company_scope_filter_enabled !== false;
  const companyScopeFilteringActive = persistedScopeFilterEnabled && !scopeFilterTemporarilyDisabled;

  useEffect(() => {
    setScopeFilterTemporarilyDisabledState(isCompanyScopeFilterTemporarilyDisabled(activeTenantId));
  }, [activeTenantId]);

  const researchBuyer = async (row) => {
    const rowId = row.id || row.prediction_id;
    if (agentLoading[rowId]) return;
    setAgentLoading((prev) => ({ ...prev, [rowId]: true }));
    try {
      const { data: { session } } = await supabase.auth.getSession();
      const res = await fetch(
        `${import.meta.env.VITE_SUPABASE_URL || 'https://ossoggqkqifdkihybbew.supabase.co'}/functions/v1/research-buyer`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${session?.access_token || import.meta.env.VITE_SUPABASE_ANON_KEY}`,
          },
          body: JSON.stringify({
            buyer_name: row.buyer_name || row.buyer_display_name,
            country: row.country || row.region,
            category: row.category || row.cpv_cluster_label || row.cpv_cluster_id,
            tenant_id: activeTenantId,
          }),
        }
      );
      const data = await res.json();
      if (data.brief) {
        setAgentBriefs((prev) => ({ ...prev, [rowId]: data.brief }));
      }
    } catch (err) {
      console.error('Civant Agent error:', err);
      setAgentBriefs((prev) => ({ ...prev, [rowId]: { summary: 'Research unavailable. Please try again.', _error: true } }));
    } finally {
      setAgentLoading((prev) => ({ ...prev, [rowId]: false }));
    }
  };

    const loadProfile = useCallback(async () => {
    if (!activeTenantId) return null;
    try {
      const rows = await civant.entities.company_profiles.filter(
        { tenant_id: activeTenantId },
        '-updated_at',
        1,
        'target_cpv_clusters,target_countries,target_buyer_types,contract_size_min_eur,contract_size_max_eur,company_scope_filter_enabled'
      );
      const profile = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
      setCompanyProfile(profile || null);
      return profile || null;
    } catch (error) {
      console.error('Failed to load company profile:', error);
      return null;
    }
  }, [activeTenantId]);

  const loadPredictions = useCallback(async () => {
    if (!activeTenantId) return;
    setLoading(true);
    try {
      await loadProfile();
      const { data, error } = await supabase
        .rpc('get_tenant_predictions', { p_tenant_id: activeTenantId }).range(0, 19999);

      if (error) {
        console.warn('get_tenant_predictions RPC unavailable:', error.message);
        const { data: fallback, error: fallbackError } = await supabase
          .from('predictions')
          .select('*')
          .eq('tenant_id', activeTenantId)
          .order('generated_at', { ascending: false })
          .limit(20000);
        if (fallbackError) throw fallbackError;
        setAllPredictions(fallback || []);
      } else {
        setAllPredictions(data || []);
      }
    } catch (error) {
      console.error('Failed to load predictions:', error);
      setAllPredictions([]);
    } finally {
      // Fetch validation accuracy stats (independent of predictions success/failure)
      try {
        const { data: vStats, error: vErr } = await supabase
          .rpc('get_prediction_validation_stats', { p_tenant_id: activeTenantId });
        if (!vErr && vStats) setValidationStats(vStats);
      } catch (e) {
        console.warn('Validation stats unavailable:', e);
      }
      setLoading(false);
    }
  }, [activeTenantId, loadProfile]);

  useEffect(() => {
    if (!isLoadingTenants && activeTenantId) {
      void loadPredictions();
    }
  }, [activeTenantId, isLoadingTenants, loadPredictions]);

  const filtered = useMemo(() => {
    let rows = [...allPredictions];

    if (companyProfile) {
      rows = rows.map((row) => ({ ...row, _scopeMatch: predictionMatchesCompanyScope(row, companyProfile) }));
      if (companyScopeFilteringActive) {
        rows = rows.filter((row) => row._scopeMatch);
      }
    }

    if (countryFilter !== 'all') rows = rows.filter((row) => (row.country || row.region) === countryFilter);
    if (urgencyFilter === 'actionable') rows = rows.filter((row) => (row.urgency || '').toLowerCase() !== 'overdue');
    else if (urgencyFilter !== 'all') rows = rows.filter((row) => row.urgency === urgencyFilter);
    if (sourceFilter !== 'all') rows = rows.filter((row) => row.signal_type === sourceFilter);

    if (companyProfile && !companyScopeFilteringActive) {
      return [...rows].sort((a, b) => {
        const scopeDiff = Number(Boolean(b._scopeMatch)) - Number(Boolean(a._scopeMatch));
        if (scopeDiff !== 0) return scopeDiff;
        return comparePredictions(a, b);
      });
    }

    return sortPredictions(rows);
  }, [allPredictions, companyProfile, companyScopeFilteringActive, countryFilter, urgencyFilter, sourceFilter]);


  const stats = useMemo(() => {
    const actionable = filtered.filter((row) => (row.urgency || '').toLowerCase() !== 'overdue');
    const upcoming = actionable.filter((row) => (row.urgency || '').toLowerCase() === 'upcoming').length;
    const horizon = actionable.filter((row) => ['horizon', 'distant'].includes((row.urgency || '').toLowerCase())).length;
    const pipelineValue = actionable.reduce((sum, row) => sum + Number(row.total_value_eur || 0), 0);
    return {
      actionableForecasts: actionable.length,
      upcoming: upcoming,
      horizon: horizon,
      pipelineValue: pipelineValue
    };
  }, [filtered]);

  const priorityRows = useMemo(
    () => filtered
      .map((row) => ({
        ...row,
        priorityScore: priorityScore(row),
        confidencePercent: predictionConfidencePercent(row)
      }))
      .sort((a, b) => {
        if (!companyScopeFilteringActive) {
          const scopeDiff = Number(Boolean(b._scopeMatch)) - Number(Boolean(a._scopeMatch));
          if (scopeDiff !== 0) return scopeDiff;
        }
        if (b.priorityScore !== a.priorityScore) return b.priorityScore - a.priorityScore;
        return b.confidencePercent - a.confidencePercent;
      })
      .slice(0, 5),
    [filtered, companyScopeFilteringActive]
  );

  const lastDataRefresh = useMemo(() => {
    const timestamps = allPredictions
      .map((row) => row?.last_computed_at || row?.generated_at || null)
      .filter(Boolean)
      .map((value) => new Date(value))
      .filter((date) => !Number.isNaN(date.getTime()))
      .map((date) => date.getTime());
    if (timestamps.length === 0) return null;
    return new Date(Math.max(...timestamps)).toISOString();
  }, [allPredictions]);

  const clearScopeFilterTemporarily = useCallback(() => {
    setCompanyScopeFilterTemporarilyDisabled(activeTenantId, true);
    setScopeFilterTemporarilyDisabledState(true);
  }, [activeTenantId]);

  const restoreScopeFilter = useCallback(() => {
    setCompanyScopeFilterTemporarilyDisabled(activeTenantId, false);
    setScopeFilterTemporarilyDisabledState(false);
  }, [activeTenantId]);
  const scopeContextLabel = companyScopeFilteringActive
    ? 'Renewal windows within your tracked scope.'
    : 'Market-wide renewal windows prioritized by your Company scope.';
  const summaryScopeHint = companyScopeFilteringActive
    ? 'Within active scope'
    : 'Market-wide, scope-prioritized';
  const prioritySectionHint = companyScopeFilteringActive
    ? 'Ranked by strategic priority within your tracked scope.'
    : 'Ranked by strategic priority using your Company scope.';
  const noPriorityHint = companyScopeFilteringActive
    ? 'No priority opportunities identified within your current scope.'
    : 'No priority opportunities identified in the current market set.';
  const noForecastHint = companyScopeFilteringActive
    ? 'No high-confidence renewal windows detected within your current scope.'
    : 'No high-confidence renewal windows detected in the current market set.';

  return (
    <Page className="space-y-8">
      <div className="space-y-1">
        <PageTitle>Forecast</PageTitle>
        <p className="text-sm text-muted-foreground">{scopeContextLabel}</p>
      </div>
      <div className="rounded-xl bg-white/[0.01] px-3 py-3 space-y-2">
        {companyProfile && companyScopeFilteringActive ? (
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span>Filtered by Company Scope</span>
            <Link to={createPageUrl('Company?tab=personalization')} className="text-cyan-300 hover:underline">Edit scope</Link>
            <button type="button" onClick={clearScopeFilterTemporarily} className="text-cyan-300 hover:underline">Clear temporarily</button>
          </div>
        ) : null}
  
        {companyProfile && persistedScopeFilterEnabled && scopeFilterTemporarilyDisabled ? (
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span>Company scope filter temporarily cleared for this session.</span>
            <button type="button" onClick={restoreScopeFilter} className="text-cyan-300 hover:underline">Turn back on</button>
          </div>
        ) : null}
        <div className="flex flex-wrap items-center gap-3">
        <p className="px-2 text-[11px] uppercase tracking-[0.08em] text-muted-foreground">Scope</p>
        <Select value={sourceFilter} onValueChange={setSourceFilter}>
          <SelectTrigger className="h-8 w-[150px] border-white/[0.08] bg-white/[0.02] text-xs text-slate-300">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {SOURCE_OPTIONS.map((option) => (
              <SelectItem key={option.value} value={option.value}>{option.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={countryFilter} onValueChange={setCountryFilter}>
          <SelectTrigger className="h-8 w-[150px] border-white/[0.08] bg-white/[0.02] text-xs text-slate-300">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {COUNTRY_OPTIONS.map((option) => (
              <SelectItem key={option.value} value={option.value}>{option.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={urgencyFilter} onValueChange={setUrgencyFilter}>
          <SelectTrigger className="h-8 w-[140px] border-white/[0.08] bg-white/[0.02] text-xs text-slate-300">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {URGENCY_OPTIONS.map((option) => (
              <SelectItem key={option.value} value={option.value}>{option.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        </div>
      </div>
      {companyProfile && !persistedScopeFilterEnabled ? (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          <span>Full market view active. Company scope is used to prioritize and highlight.</span>
          <Link to={createPageUrl('Company?tab=personalization')} className="text-cyan-300 hover:underline">Edit scope behavior</Link>
        </div>
      ) : null}

      <PageBody className="space-y-6">
        <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
          <SummaryTile
            label="Actionable Forecasts"
            value={stats.actionableForecasts.toLocaleString()}
            hint="Excludes unverified overdue"
          />
          <SummaryTile
            label="Upcoming"
            value={stats.upcoming.toLocaleString()}
            hint="Next 60 days"
          />
          <SummaryTile
            label="Horizon"
            value={stats.horizon.toLocaleString()}
            hint="60+ days out"
          />
          <SummaryTile
            label="Pipeline Value"
            value={formatCurrency(stats.pipelineValue)}
            hint="Actionable opportunities"
          />
        </div>
        {validationStats ? (() => {
          const upcoming = validationStats.accuracy_by_urgency?.find((u) => u.urgency === 'upcoming');
          const horizon = validationStats.accuracy_by_urgency?.find((u) => u.urgency === 'horizon');
          const matureCountries = validationStats.accuracy_by_country?.filter((c) => c.country !== 'FR') || [];
          const bestCountryAccuracy = matureCountries.length > 0 ? Math.max(...matureCountries.map((c) => c.accuracy || 0)) : null;
          return (
          <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] p-4 space-y-3">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div className="space-y-0.5">
                <h3 className="text-sm font-semibold text-card-foreground">Forecast Accuracy — Validated Against Published Tenders</h3>
                <p className="text-[11px] text-muted-foreground">
                  {validationStats.confirmed?.toLocaleString()} predictions independently confirmed by matching published procurement notices
                </p>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <div className="rounded-lg bg-emerald-500/[0.08] border border-emerald-500/20 px-3 py-2.5">
                <p className="text-[10px] uppercase tracking-wider text-emerald-400/80">Upcoming Window</p>
                <p className="text-2xl font-bold text-emerald-400">{upcoming?.hit_rate || '—'}%</p>
                <p className="text-[10px] text-muted-foreground">{upcoming?.confirmed?.toLocaleString()} of {upcoming?.total?.toLocaleString()} confirmed</p>
              </div>
              {bestCountryAccuracy ? (
              <div className="rounded-lg bg-emerald-500/[0.08] border border-emerald-500/20 px-3 py-2.5">
                <p className="text-[10px] uppercase tracking-wider text-emerald-400/80">Mature Markets</p>
                <p className="text-2xl font-bold text-emerald-400">{'>'}{Math.floor(bestCountryAccuracy / 5) * 5}%</p>
                <p className="text-[10px] text-muted-foreground">IE &amp; ES resolved predictions</p>
              </div>
              ) : null}
              <div className="rounded-lg bg-white/[0.03] px-3 py-2.5">
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Horizon Signals</p>
                <p className="text-2xl font-semibold text-card-foreground">{horizon?.hit_rate || '—'}%</p>
                <p className="text-[10px] text-muted-foreground">{horizon?.confirmed?.toLocaleString()} of {horizon?.total?.toLocaleString()} confirmed</p>
              </div>
              <div className="rounded-lg bg-white/[0.03] px-3 py-2.5">
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Median Timing</p>
                <p className="text-2xl font-semibold text-card-foreground">±{validationStats.median_delta_days}d</p>
                <p className="text-[10px] text-muted-foreground">from predicted to published</p>
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-4 pt-1">
              {validationStats.accuracy_by_country?.map((c) => (
                <div key={c.country} className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">{c.country === 'ES' ? '🇪🇸' : c.country === 'FR' ? '🇫🇷' : c.country === 'IE' ? '🇮🇪' : c.country}</span>
                  <span className="text-xs font-medium text-card-foreground">{c.accuracy}%</span>
                  <span className="text-[10px] text-muted-foreground">({c.confirmed?.toLocaleString()} confirmed)</span>
                </div>
              ))}
              <span className="text-[10px] text-muted-foreground/50 ml-auto">{validationStats.pending?.toLocaleString()} predictions awaiting future validation</span>
            </div>
          </div>
          );
        })() : null}


        <section className="space-y-3 pt-4">
          <div className="space-y-1">
            <h3 className="text-base font-semibold text-card-foreground">Top Priority Opportunities</h3>
            <p className="text-xs text-muted-foreground">{prioritySectionHint}</p>
          </div>

          {loading ? (
            <div className="flex items-center gap-2 py-3 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Ranking priority opportunities...
            </div>
          ) : null}

          {!loading && priorityRows.length === 0 ? (
            <div className="space-y-1 py-2 text-sm text-muted-foreground">
              <p>{noPriorityHint}</p>
              <p>The forecast engine continues to monitor renewal cycles.</p>
            </div>
          ) : null}

          {!loading && priorityRows.length > 0 ? (
            <div className="divide-y divide-white/[0.06]">
              {priorityRows.slice(0, priorityPage * PRIORITY_PAGE_SIZE).map((row, index) => (
                <div
                  key={row.id || row.prediction_id || index}
                  className="grid grid-cols-1 gap-3 py-3 md:grid-cols-[2.5fr_auto] md:items-center"
                >
                  <div className="min-w-0">
                    <p className="truncate text-sm font-semibold text-card-foreground">{buyerLabel(row)}</p>
                    {!companyScopeFilteringActive && row._scopeMatch ? (
                      <p className="mt-0.5 text-[11px] text-cyan-300/90">Scope match</p>
                    ) : null}
                    <p className="mt-0.5 text-xs text-muted-foreground">{formatRenewalWindow(row)}</p>
                    <p className="mt-1 text-sm font-medium text-slate-200 tabular-nums">
                      {row.confidencePercent}% confidence
                    </p>
                  </div>

                  <div className="flex items-center justify-between gap-4 md:justify-end">
                    <div className="text-right">
                      <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">Priority {index + 1}</p>
                      <p className="text-sm font-medium text-slate-300 tabular-nums">{row.priorityScore.toFixed(1)}</p>
                    </div>
                    <Button variant="ghost" size="sm" asChild>
                      <Link to={createPageUrl(`search?buyer=${encodeURIComponent(buyerLabel(row))}`)}>
                        Plan Engagement
                      </Link>
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          ) : null}
                  {priorityRows.length > PRIORITY_PAGE_SIZE ? (
            <div className="flex items-center justify-between pt-3 border-t border-white/[0.06]">
              <p className="text-xs text-muted-foreground">
                Showing {Math.min(priorityPage * PRIORITY_PAGE_SIZE, priorityRows.length)} of {priorityRows.length} priority opportunities
              </p>
              <div className="flex items-center gap-2">
                {priorityPage > 1 ? (
                  <button type="button" onClick={() => setPriorityPage((p) => p - 1)} className="px-3 py-1 text-xs text-cyan-300 hover:text-cyan-200 border border-white/[0.08] rounded-md hover:bg-white/[0.04]">Previous</button>
                ) : null}
                {priorityPage * PRIORITY_PAGE_SIZE < priorityRows.length ? (
                  <button type="button" onClick={() => setPriorityPage((p) => p + 1)} className="px-3 py-1 text-xs text-cyan-300 hover:text-cyan-200 border border-white/[0.08] rounded-md hover:bg-white/[0.04]">Show More</button>
                ) : null}
              </div>
            </div>
          ) : null}
</section>

        {!loading ? <ForecastTimeline rows={filtered} /> : null}

        {loading ? (
          <Card className="border border-white/[0.06] bg-white/[0.015] shadow-none">
            <CardContent className="flex items-center justify-center gap-3 py-12 text-muted-foreground">
              <Loader2 className="h-5 w-5 animate-spin" />
              Loading forecast…
            </CardContent>
          </Card>
        ) : null}

        {!loading && filtered.length === 0 ? (
          <Card className="border border-white/[0.06] bg-white/[0.015] shadow-none">
            <CardContent className="space-y-2 py-12 text-center">
              <p className="text-sm text-slate-300">Forecast engine active.</p>
              <p className="text-sm text-muted-foreground">{noForecastHint}</p>
            </CardContent>
          </Card>
        ) : null}

        {!loading && filtered.length > 0 ? (
          <section className="space-y-3 rounded-2xl bg-white/[0.015] px-4 py-4">
            <h3 className="text-base font-semibold text-card-foreground">Forecast List</h3>
            <div className="sticky top-0 z-10 hidden border-b border-white/[0.06] pb-2.5 pt-1 md:grid md:grid-cols-[2fr_1.4fr_1fr_1.6fr_auto] md:items-center md:gap-4">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Buyer</p>
              <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Window</p>
              <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Confidence</p>
              <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Signal</p>
              <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-400 md:justify-self-end">Action</p>
            </div>
            <div className="divide-y divide-white/[0.06]">
              {filtered.map((row, index) => (
                <div key={row.id || row.prediction_id || index} className="grid grid-cols-1 gap-3 py-4 md:grid-cols-[2fr_1.4fr_1fr_1.6fr_auto] md:items-center md:gap-4">
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium text-card-foreground">{buyerLabel(row)}</p>
                    {!companyScopeFilteringActive && row._scopeMatch ? (
                      <p className="text-[11px] text-cyan-300/90">Scope match</p>
                    ) : null}
                    <p className="text-xs text-muted-foreground">{row.country || row.region || '—'}</p>
                  </div>
                  <div className="text-sm text-slate-300">{formatRenewalWindow(row)}</div>
                  <div className="text-sm font-medium text-slate-200">{predictionConfidencePercent(row)}%</div>
                  <div className="truncate text-xs text-muted-foreground">{cycleReference(row)}</div>
                  <div className="flex items-center gap-1 md:justify-self-end">
                    <button
                      type="button"
                      onClick={() => researchBuyer(row)}
                      disabled={agentLoading[row.id || row.prediction_id]}
                      className="inline-flex items-center gap-1.5 rounded-md border border-emerald-500/30 bg-emerald-500/[0.08] px-2.5 py-1 text-[11px] font-medium text-emerald-400 hover:bg-emerald-500/[0.15] hover:text-emerald-300 disabled:opacity-50 transition-colors"
                    >
                      {agentLoading[row.id || row.prediction_id] ? (
                        <Loader2 className="h-3 w-3 animate-spin" />
                      ) : (
                        <svg className="h-3 w-3" viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="7" stroke="currentColor" strokeWidth="1.5"/><path d="M8 4v4l2.5 2.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/></svg>
                      )}
                      Civant Agent
                    </button>
                    <Button variant="ghost" size="sm" asChild>
                      <Link to={createPageUrl(`search?buyer=${encodeURIComponent(buyerLabel(row))}`)}>
                        View
                      </Link>
                    </Button>
                  </div>
                </div>
                {agentBriefs[row.id || row.prediction_id] ? (
                  <div className="col-span-full -mt-2 mb-2 ml-0 md:ml-4 rounded-lg border border-emerald-500/20 bg-emerald-500/[0.04] px-4 py-3 space-y-2">
                    <div className="flex items-center justify-between">
                      <p className="text-[10px] font-semibold uppercase tracking-wider text-emerald-400/80">Civant Agent Intelligence Brief</p>
                      <button type="button" onClick={() => setAgentBriefs((prev) => { const next = { ...prev }; delete next[row.id || row.prediction_id]; return next; })} className="text-[10px] text-muted-foreground hover:text-slate-300">&times; Close</button>
                    </div>
                    <p className="text-sm text-slate-200 leading-relaxed">{agentBriefs[row.id || row.prediction_id]?.summary}</p>
                    {agentBriefs[row.id || row.prediction_id]?.opportunity_score ? (
                      <div className="flex items-center gap-3 pt-1">
                        <span className="text-[10px] text-muted-foreground">Opportunity Score</span>
                        <span className="text-sm font-semibold text-emerald-400">{agentBriefs[row.id || row.prediction_id].opportunity_score}/100</span>
                        {agentBriefs[row.id || row.prediction_id]?.procurement_intent?.confidence ? (
                          <span className="text-[10px] text-muted-foreground">Intent: {agentBriefs[row.id || row.prediction_id].procurement_intent.confidence}</span>
                        ) : null}
                      </div>
                    ) : null}
                    {agentBriefs[row.id || row.prediction_id]?.sources?.length > 0 ? (
                      <div className="flex flex-wrap gap-2 pt-1">
                        {agentBriefs[row.id || row.prediction_id].sources.slice(0, 3).map((s, si) => (
                          <a key={si} href={s.url} target="_blank" rel="noopener noreferrer" className="text-[10px] text-cyan-300/70 hover:text-cyan-300 underline truncate max-w-[200px]">{s.title || s.url}</a>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              ))}
            </div>
          </section>
        ) : null}

        <HomePlatformFooter
          version={import.meta.env.VITE_APP_VERSION || 'v0.9.3'}
          lastDataRefresh={lastDataRefresh}
          supportTo={createPageUrl('Company?section=support')}
          legalTo={createPageUrl('Company?section=legal')}
        />
      </PageBody>
    </Page>
  );
}
