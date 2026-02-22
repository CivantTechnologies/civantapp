import React, { useEffect, useMemo, useState, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { format } from 'date-fns';
import {
  AlertTriangle,
  Calendar,
  ChevronDown,
  ChevronUp,
  CircleAlert,
  Clock,
  Filter,
  Loader2,
  Target,
  Timer,
  TrendingUp,
  Users,
} from 'lucide-react';
import { supabase } from '@/lib/supabaseClient';
import { useTenant } from '@/lib/tenant';
import { createPageUrl } from '../utils';
import {
  Badge,
  Button,
  Card,
  CardContent,
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

const COUNTRY_FLAGS = { IE: '🇮🇪', FR: '🇫🇷', ES: '🇪🇸' };

const COUNTRY_OPTIONS = [
  { value: 'all', label: 'All Countries' },
  { value: 'IE', label: '🇮🇪 Ireland' },
  { value: 'FR', label: '🇫🇷 France' },
  { value: 'ES', label: '🇪🇸 Spain' },
];

const URGENCY_OPTIONS = [
  { value: 'all', label: 'All Urgency' },
  { value: 'overdue', label: 'Overdue' },
  { value: 'upcoming', label: 'Upcoming' },
  { value: 'horizon', label: 'Horizon' },
  { value: 'distant', label: 'Distant' },
];

const SOURCE_OPTIONS = [
  { value: 'all', label: 'All Sources' },
  { value: 'renewal', label: 'Renewal Signals' },
  { value: 'engine', label: 'Pattern Engine' },
];

const SIGNAL_TYPE_LABELS = {
  framework_expiry: 'Framework Expiry',
  repeat_buyer_cycle: 'Repeat Buyer',
  contract_renewal: 'Contract Renewal',
};

/**
 * Maps prediction category values to the profile cluster IDs.
 * The company profile wizard uses cluster IDs like cluster_it_software,
 * but predictions store categories in multiple formats.
 */
const CATEGORY_TO_PROFILE_CLUSTER = {
  'cluster_digital': 'cluster_it_software',
  'cluster_it_software': 'cluster_it_software',
  'cluster_professional_services': 'cluster_consulting',
  'cluster_consulting': 'cluster_consulting',
  'cluster_construction': 'cluster_construction',
  'cluster_facilities': 'cluster_facilities_maintenance',
  'cluster_facilities_maintenance': 'cluster_facilities_maintenance',
  'cluster_health': 'cluster_health_medical',
  'cluster_health_medical': 'cluster_health_medical',
  'cluster_education': 'cluster_education_training',
  'cluster_education_training': 'cluster_education_training',
  'cluster_transport': 'cluster_transport',
  'cluster_food': 'cluster_food_catering',
  'cluster_food_catering': 'cluster_food_catering',
  'cluster_energy': 'cluster_energy_environment',
  'cluster_energy_environment': 'cluster_energy_environment',
  'cluster_environment': 'cluster_energy_environment',
  'cluster_communications': 'cluster_communications_media',
  'cluster_communications_media': 'cluster_communications_media',
  'cluster_finance': 'cluster_financial_legal',
  'cluster_financial_legal': 'cluster_financial_legal',
  'cluster_legal': 'cluster_financial_legal',
  'cluster_manufacturing': 'cluster_manufacturing',
  'cluster_defence': 'cluster_defence_security',
  'cluster_defence_security': 'cluster_defence_security',
  'cluster_security': 'cluster_defence_security',
  'cluster_research': 'cluster_research',
  'Digital': 'cluster_it_software',
  'IT': 'cluster_it_software',
  'Software': 'cluster_it_software',
  'Telecommunications': 'cluster_it_software',
  'Telecoms': 'cluster_it_software',
  'Professional Services': 'cluster_consulting',
  'Consulting': 'cluster_consulting',
  'Construction': 'cluster_construction',
  'Construction Equipment': 'cluster_construction',
  'Construction Materials': 'cluster_construction',
  'Maintenance': 'cluster_facilities_maintenance',
  'Furniture': 'cluster_facilities_maintenance',
  'Installation': 'cluster_facilities_maintenance',
  'Electrical': 'cluster_facilities_maintenance',
  'Healthcare': 'cluster_health_medical',
  'Medical': 'cluster_health_medical',
  'Education': 'cluster_education_training',
  'Education Equipment': 'cluster_education_training',
  'Transport': 'cluster_transport',
  'Transport Equipment': 'cluster_transport',
  'Vehicles': 'cluster_transport',
  'Food': 'cluster_food_catering',
  'Hospitality': 'cluster_food_catering',
  'Energy': 'cluster_energy_environment',
  'Environmental': 'cluster_energy_environment',
  'Agriculture': 'cluster_energy_environment',
  'Utilities': 'cluster_energy_environment',
  'Financial': 'cluster_financial_legal',
  'Legal': 'cluster_financial_legal',
  'Culture': 'cluster_communications_media',
  'Printing': 'cluster_communications_media',
  'Publishing': 'cluster_communications_media',
  'Industrial Equipment': 'cluster_manufacturing',
  'Chemicals': 'cluster_manufacturing',
  'Textiles': 'cluster_manufacturing',
  'Mining': 'cluster_manufacturing',
  'Security': 'cluster_defence_security',
  'Research': 'cluster_research',
  'Scientific': 'cluster_research',
};

function normaliseCategory(category) {
  if (!category) return null;
  return CATEGORY_TO_PROFILE_CLUSTER[category] || CATEGORY_TO_PROFILE_CLUSTER[category.toLowerCase()] || null;
}

/* ─────────────────────────── helpers ─────────────────────────── */

function urgencyStyle(urgency) {
  switch (String(urgency || '').toLowerCase()) {
    case 'overdue':
      return { bg: 'bg-red-500/15', border: 'border-red-400/40', text: 'text-red-300', Icon: CircleAlert, pulse: true, label: 'Overdue' };
    case 'upcoming':
      return { bg: 'bg-amber-500/12', border: 'border-amber-400/30', text: 'text-amber-300', Icon: AlertTriangle, pulse: false, label: 'Upcoming' };
    case 'horizon':
      return { bg: 'bg-blue-500/12', border: 'border-blue-400/30', text: 'text-blue-300', Icon: Timer, pulse: false, label: 'Horizon' };
    case 'distant':
      return { bg: 'bg-slate-500/10', border: 'border-slate-400/25', text: 'text-slate-400', Icon: Calendar, pulse: false, label: 'Distant' };
    default:
      return { bg: 'bg-slate-500/10', border: 'border-slate-400/20', text: 'text-slate-400', Icon: Clock, pulse: false, label: urgency || '' };
  }
}

function signalBadgeClass(signalType) {
  switch (signalType) {
    case 'framework_expiry':   return 'bg-purple-500/12 text-purple-300 border-purple-400/30';
    case 'repeat_buyer_cycle': return 'bg-civant-teal/12 text-civant-teal border-civant-teal/30';
    case 'contract_renewal':   return 'bg-blue-500/12 text-blue-300 border-blue-400/30';
    default:                   return 'bg-slate-500/10 text-slate-400 border-slate-400/20';
  }
}

const pct    = (v) => `${Math.round(Number(v || 0) * 100)}%`;
const fDate  = (v) => { if (!v) return '—'; const d = new Date(v); return Number.isNaN(d.getTime()) ? String(v) : format(d, 'MMM d, yyyy'); };
const fVal   = (n) => { n = Number(n || 0); return n >= 1e6 ? `€${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `€${(n / 1e3).toFixed(0)}K` : n > 0 ? `€${n.toLocaleString()}` : '—'; };

function daysLabel(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return null;
  const days = Math.ceil((d - new Date()) / 864e5);
  if (days < -30) return { text: `${Math.abs(days)}d overdue`, cls: 'text-red-400' };
  if (days < 0) return { text: `${Math.abs(days)}d overdue`, cls: 'text-red-300' };
  if (days === 0) return { text: 'Today', cls: 'text-red-300' };
  if (days <= 30) return { text: `${days}d away`, cls: 'text-amber-400' };
  if (days <= 90) return { text: `${days}d away`, cls: 'text-blue-300' };
  return { text: `${days}d away`, cls: 'text-slate-500' };
}

const isRenewal = (row) => !!row.signal_type;

function sortPredictions(rows) {
  const urgencyRank = { overdue: 0, upcoming: 1, horizon: 2, distant: 3 };
  return [...rows].sort((a, b) => {
    const uA = urgencyRank[a.urgency] ?? 4;
    const uB = urgencyRank[b.urgency] ?? 4;
    if (uA !== uB) return uA - uB;
    const dateA = a.predicted_tender_date || '';
    const dateB = b.predicted_tender_date || '';
    if (dateA !== dateB) return dateA.localeCompare(dateB);
    return Number(b.probability || 0) - Number(a.probability || 0);
  });
}

/* ──────────────────── Prediction Card ──────────────────── */

function PredictionCard({ row }) {
  const [open, setOpen] = useState(false);
  const renewal = isRenewal(row);
  const us = urgencyStyle(row.urgency);
  const dl = daysLabel(row.predicted_tender_date);

  const src = row.renewal_source || {};
  const incumbents = src.incumbent_suppliers || [];
  const expiring = src.expiring_contracts || [];
  const drivers = row.top_drivers || [];

  const buyerName = row.buyer_name || row.buyer_display_name || row.buyer_entity_id || row.buyer_id || 'Unknown Buyer';
  const category = row.category || row.cpv_cluster_label || row.cpv_family || '';
  const country = row.country || row.region || '';

  return (
    <Card className="bg-white/[0.02] border border-white/[0.06] hover:border-white/[0.12] transition-all duration-200">
      <div className="flex items-start gap-4 p-5 cursor-pointer select-none" onClick={() => setOpen(v => !v)}>
        {renewal ? (
          <div className={`mt-0.5 shrink-0 w-10 h-10 rounded-xl border flex items-center justify-center ${us.bg} ${us.border}`}>
            <us.Icon className={`w-5 h-5 ${us.text} ${us.pulse ? 'animate-pulse' : ''}`} />
          </div>
        ) : (
          <div className="mt-0.5 shrink-0 w-10 h-10 rounded-xl border border-white/[0.06] bg-white/[0.015] flex items-center justify-center">
            <TrendingUp className="w-5 h-5 text-civant-teal" />
          </div>
        )}

        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-center gap-1.5 mb-1.5">
            <span className="text-lg leading-none">{COUNTRY_FLAGS[country] || '🌍'}</span>
            {renewal && (
              <Badge className={`text-[11px] font-medium border ${signalBadgeClass(row.signal_type)}`}>
                {SIGNAL_TYPE_LABELS[row.signal_type] || row.signal_type}
              </Badge>
            )}
            {renewal && row.urgency && (
              <Badge className={`text-[11px] font-medium border ${us.bg} ${us.border} ${us.text}`}>
                {us.label}
              </Badge>
            )}
            {!renewal && (
              <Badge variant="outline" className="text-[11px] border-white/[0.08] text-slate-500">Pattern engine</Badge>
            )}
          </div>
          <h3 className="text-base font-semibold text-slate-100 truncate">{buyerName}</h3>
          <p className="text-sm text-slate-500 mt-0.5">{category}{category && country ? ' · ' : ''}{country}</p>
        </div>

        <div className="hidden sm:flex items-center gap-6 shrink-0 text-right">
          <div>
            <p className="text-[11px] uppercase tracking-wider text-slate-500">
              {row.urgency === 'overdue' ? 'Expected' : 'Predicted'}
            </p>
            <p className="text-sm font-semibold text-slate-200">{fDate(row.predicted_tender_date)}</p>
            {dl && <p className={`text-xs mt-0.5 ${dl.cls}`}>{dl.text}</p>}
          </div>
          {renewal && (
            <div>
              <p className="text-[11px] uppercase tracking-wider text-slate-500">Value</p>
              <p className="text-sm font-semibold text-civant-teal">{fVal(row.total_value_eur)}</p>
            </div>
          )}
          <div>
            <p className="text-[11px] uppercase tracking-wider text-slate-500">Confidence</p>
            <p className="text-sm font-semibold text-slate-200">{pct(row.probability || row.confidence)}</p>
          </div>
          {open ? <ChevronUp className="w-4 h-4 text-slate-500" /> : <ChevronDown className="w-4 h-4 text-slate-500" />}
        </div>
      </div>

      {/* Mobile metrics */}
      <div className="flex sm:hidden items-center gap-4 px-5 pb-3 text-xs">
        <span className="text-slate-400">{fDate(row.predicted_tender_date)}</span>
        {dl && <span className={dl.cls}>{dl.text}</span>}
        {renewal && <span className="text-civant-teal font-medium">{fVal(row.total_value_eur)}</span>}
        <span className="text-slate-400">{pct(row.probability || row.confidence)}</span>
      </div>

      {/* Expanded detail */}
      {open && (
        <div className="px-5 pb-5 space-y-4 border-t border-white/[0.04]">
          {renewal && (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-4">
                <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
                  <p className="text-[11px] text-slate-500 uppercase tracking-wider">Incumbents</p>
                  <p className="text-lg font-semibold text-slate-200">{src.distinct_suppliers || incumbents.length || '—'}</p>
                </div>
                <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
                  <p className="text-[11px] text-slate-500 uppercase tracking-wider">Expiring</p>
                  <p className="text-lg font-semibold text-slate-200">
                    {src.expiring_count || expiring.length || '—'}
                    <span className="text-xs font-normal text-slate-500 ml-1">contracts</span>
                  </p>
                </div>
                <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
                  <p className="text-[11px] text-slate-500 uppercase tracking-wider">Avg Duration</p>
                  <p className="text-lg font-semibold text-slate-200">
                    {src.avg_duration_months ? `${Math.round(src.avg_duration_months)}mo` : '—'}
                  </p>
                </div>
                <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
                  <p className="text-[11px] text-slate-500 uppercase tracking-wider">Framework</p>
                  <p className={`text-lg font-semibold ${src.has_frameworks ? 'text-civant-teal' : 'text-slate-200'}`}>
                    {src.has_frameworks ? 'Yes' : 'No'}
                  </p>
                </div>
              </div>

              {incumbents.length > 0 && (
                <div>
                  <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500 mb-2 flex items-center gap-1.5">
                    <Users className="w-3.5 h-3.5" /> Incumbent Suppliers
                  </p>
                  <div className="flex flex-wrap gap-1.5">
                    {incumbents.map((name, i) => (
                      <Badge key={i} className="bg-white/[0.04] text-slate-300 border border-white/[0.08] text-xs">{name}</Badge>
                    ))}
                  </div>
                </div>
              )}

              {expiring.length > 0 && (
                <div>
                  <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500 mb-2">Expiring Contracts</p>
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
                        {expiring.slice(0, 5).map((c, i) => (
                          <tr key={i} className="border-b border-white/[0.03] last:border-0">
                            <td className="px-3 py-2 text-slate-300">{c.supplier}</td>
                            <td className="px-3 py-2 text-right text-slate-400">{fVal(c.value_eur)}</td>
                            <td className="px-3 py-2 text-right text-slate-400">{fDate(c.end_date)}</td>
                            <td className="px-3 py-2 text-center">
                              {c.is_framework
                                ? <Badge className="bg-purple-500/10 text-purple-300 border-purple-400/25 text-[10px]">Framework</Badge>
                                : <span className="text-slate-500 text-xs">Standard</span>}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}

          {!renewal && (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-4">
              {[
                ['Probability', pct(row.probability)],
                ['Forecast Score', Number(row.forecast_score || 0)],
                ['Fallback Tier', Number(row.fallback_tier || 0)],
                ['Drivers', `${Number(row.drivers_count || 0)} drivers`],
              ].map(([label, value], i) => (
                <div key={i} className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-3">
                  <p className="text-[11px] text-slate-500 uppercase tracking-wider">{label}</p>
                  <p className="text-lg font-semibold text-slate-200">{value}</p>
                </div>
              ))}
            </div>
          )}

          {drivers.length > 0 && (
            <div>
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500 mb-2">Prediction Drivers</p>
              <div className="space-y-1.5">
                {drivers.slice(0, 5).map((d, i) => (
                  <div key={i} className="text-sm text-slate-400 flex items-start gap-2">
                    <span className="text-slate-600 mt-0.5">•</span>
                    <span>{typeof d === 'string' ? d : d.label || d.narrative || JSON.stringify(d)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div className="flex items-center justify-between pt-1">
            <div className="text-xs text-slate-500 flex items-center gap-2">
              <Calendar className="w-3.5 h-3.5" />
              {fDate(row.generated_at || row.last_computed_at)}
              <span className="text-slate-600">·</span>
              {row.model_version || 'engine-v1'}
            </div>
            <Button variant="outline" className="border-white/[0.08] text-xs" asChild>
              <Link to={createPageUrl(`search?buyer=${encodeURIComponent(buyerName)}`)}>
                View related tenders
              </Link>
            </Button>
          </div>
        </div>
      )}
    </Card>
  );
}

/* ════════════════════════════ MAIN PAGE ════════════════════════════ */

export default function Predictions() {
  const { activeTenantId, isLoadingTenants } = useTenant();

  const [countryFilter, setCountryFilter] = useState('all');
  const [urgencyFilter, setUrgencyFilter] = useState('all');
  const [sourceFilter, setSourceFilter] = useState('all');

  const [allPredictions, setAllPredictions] = useState([]);
  const [companyProfile, setCompanyProfile] = useState(null);
  const [loading, setLoading] = useState(true);

  const loadProfile = useCallback(async () => {
    if (!activeTenantId) return null;
    try {
      const { data } = await supabase
        .from('company_profiles')
        .select('target_cpv_clusters,target_countries,target_buyer_types,contract_size_min_eur,contract_size_max_eur')
        .eq('tenant_id', activeTenantId)
        .maybeSingle();
      setCompanyProfile(data || null);
      return data || null;
    } catch (e) {
      console.error('Failed to load company profile:', e);
      return null;
    }
  }, [activeTenantId]);

  const loadPredictions = useCallback(async () => {
    if (!activeTenantId) return;
    setLoading(true);
    try {
      await loadProfile();

      const { data, error } = await supabase
        .rpc('get_tenant_predictions', { p_tenant_id: activeTenantId })
        .limit(500);

      if (error) {
        console.warn('get_tenant_predictions RPC unavailable:', error.message);
        const { data: fallback, error: fbErr } = await supabase
          .from('predictions')
          .select('*')
          .eq('tenant_id', activeTenantId)
          .order('generated_at', { ascending: false })
          .limit(500);
        if (fbErr) throw fbErr;
        setAllPredictions(fallback || []);
      } else {
        setAllPredictions(data || []);
      }
    } catch (e) {
      console.error('Failed to load predictions:', e);
      setAllPredictions([]);
    } finally {
      setLoading(false);
    }
  }, [activeTenantId, loadProfile]);

  useEffect(() => {
    if (!isLoadingTenants && activeTenantId) loadPredictions();
  }, [activeTenantId, isLoadingTenants, loadPredictions]);

  const filtered = useMemo(() => {
    let rows = [...allPredictions];

    if (companyProfile) {
      const clusters = companyProfile.target_cpv_clusters || [];
      const countries = companyProfile.target_countries || [];
      const minVal = companyProfile.contract_size_min_eur || 0;
      const maxVal = companyProfile.contract_size_max_eur || 0;

      rows = rows.filter((r) => {
        if (countries.length > 0) {
          const rc = r.country || r.region;
          if (rc && !countries.includes(rc)) return false;
        }
        if (clusters.length > 0 && r.category) {
          const mapped = normaliseCategory(r.category);
          if (mapped && !clusters.includes(mapped)) return false;
        }
        const val = Number(r.total_value_eur || 0);
        if (minVal > 0 && val > 0 && val < minVal) return false;
        if (maxVal > 0 && val > 0 && val > maxVal) return false;
        return true;
      });
    }

    if (countryFilter !== 'all') rows = rows.filter(r => (r.country || r.region) === countryFilter);
    if (urgencyFilter !== 'all') rows = rows.filter(r => r.urgency === urgencyFilter);
    if (sourceFilter === 'renewal') rows = rows.filter(isRenewal);
    else if (sourceFilter === 'engine') rows = rows.filter(r => !isRenewal(r));

    return sortPredictions(rows);
  }, [allPredictions, companyProfile, countryFilter, urgencyFilter, sourceFilter]);

  const stats = useMemo(() => {
    const renewals = filtered.filter(isRenewal);
    return {
      total: filtered.length,
      renewals: renewals.length,
      engine: filtered.length - renewals.length,
      overdue: renewals.filter(r => r.urgency === 'overdue').length,
      upcoming: renewals.filter(r => r.urgency === 'upcoming').length,
      totalValue: renewals.reduce((s, r) => s + Number(r.total_value_eur || 0), 0),
    };
  }, [filtered]);

  return (
    <Page>
      <PageHeader>
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <PageTitle>Tender Panorama</PageTitle>
            <PageDescription>
              Personalized procurement forecast — contract renewals and publication patterns, filtered to your profile.
            </PageDescription>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Select value={sourceFilter} onValueChange={setSourceFilter}>
              <SelectTrigger className="w-[150px] bg-white/[0.02] border-white/[0.08] text-slate-300 text-sm">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {SOURCE_OPTIONS.map(o => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}
              </SelectContent>
            </Select>
            <Select value={countryFilter} onValueChange={setCountryFilter}>
              <SelectTrigger className="w-[150px] bg-white/[0.02] border-white/[0.08] text-slate-300 text-sm">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {COUNTRY_OPTIONS.map(o => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}
              </SelectContent>
            </Select>
            <Select value={urgencyFilter} onValueChange={setUrgencyFilter}>
              <SelectTrigger className="w-[140px] bg-white/[0.02] border-white/[0.08] text-slate-300 text-sm">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {URGENCY_OPTIONS.map(o => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
        </div>
      </PageHeader>

      <PageBody className="space-y-6">
        {companyProfile && (
          <div className="flex items-center gap-2 text-xs text-slate-500 px-1">
            <Filter className="w-3.5 h-3.5 text-civant-teal" />
            <span>
              Filtered by profile:
              {' '}{(companyProfile.target_countries || []).map(c => COUNTRY_FLAGS[c] || c).join(' ')}
              {' · '}{(companyProfile.target_cpv_clusters || []).length} categories
              {companyProfile.contract_size_min_eur > 0 && ` · Min ${fVal(companyProfile.contract_size_min_eur)}`}
            </span>
            <Link to="/companyprofile" className="text-civant-teal hover:underline ml-1">Edit</Link>
          </div>
        )}

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="bg-white/[0.02] border border-white/[0.06]">
            <CardContent className="pt-5 flex items-center justify-between">
              <div>
                <p className="text-[11px] text-slate-500 uppercase tracking-wider">Total Forecasts</p>
                <p className="text-3xl font-bold text-slate-100">{stats.total}</p>
                <p className="text-xs text-slate-500 mt-0.5">{stats.renewals} renewal · {stats.engine} pattern</p>
              </div>
              <Target className="w-7 h-7 text-civant-teal opacity-60" />
            </CardContent>
          </Card>
          <Card className="bg-white/[0.02] border border-white/[0.06]">
            <CardContent className="pt-5 flex items-center justify-between">
              <div>
                <p className="text-[11px] text-slate-500 uppercase tracking-wider">Overdue</p>
                <p className="text-3xl font-bold text-red-400">{stats.overdue}</p>
                <p className="text-xs text-red-400/60 mt-0.5">Likely active now</p>
              </div>
              <CircleAlert className="w-7 h-7 text-red-400 opacity-60" />
            </CardContent>
          </Card>
          <Card className="bg-white/[0.02] border border-white/[0.06]">
            <CardContent className="pt-5 flex items-center justify-between">
              <div>
                <p className="text-[11px] text-slate-500 uppercase tracking-wider">Upcoming</p>
                <p className="text-3xl font-bold text-amber-400">{stats.upcoming}</p>
              </div>
              <AlertTriangle className="w-7 h-7 text-amber-400 opacity-60" />
            </CardContent>
          </Card>
          <Card className="bg-white/[0.02] border border-white/[0.06]">
            <CardContent className="pt-5 flex items-center justify-between">
              <div>
                <p className="text-[11px] text-slate-500 uppercase tracking-wider">Pipeline Value</p>
                <p className="text-3xl font-bold text-civant-teal">{fVal(stats.totalValue)}</p>
              </div>
              <TrendingUp className="w-7 h-7 text-civant-teal opacity-60" />
            </CardContent>
          </Card>
        </div>

        {loading && (
          <Card className="bg-white/[0.02] border border-white/[0.06]">
            <CardContent className="py-12 flex items-center justify-center gap-3 text-slate-400">
              <Loader2 className="w-5 h-5 animate-spin" />
              Loading forecast…
            </CardContent>
          </Card>
        )}

        {!loading && filtered.length === 0 && (
          <Card className="bg-white/[0.02] border border-white/[0.06]">
            <CardContent className="py-12 text-center space-y-2">
              <p className="text-slate-400">No predictions match your current filters.</p>
              <p className="text-xs text-slate-500">
                Try adjusting filters or check your{' '}
                <Link to="/companyprofile" className="text-civant-teal hover:underline">company profile</Link>.
              </p>
            </CardContent>
          </Card>
        )}

        {!loading && filtered.length > 0 && (
          <div className="space-y-3">
            {filtered.map((row, idx) => (
              <PredictionCard key={row.id || row.prediction_id || idx} row={row} />
            ))}
          </div>
        )}
      </PageBody>
    </Page>
  );
}
