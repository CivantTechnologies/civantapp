import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { civant } from '@/api/civantClient';
import { supabase } from '@/lib/supabaseClient';
import { useTenant } from '@/lib/tenant';
import { addMonths, differenceInCalendarDays, format, startOfMonth } from 'date-fns';
import { ArrowLeft, Edit2, Loader2, MapPin, Trash2, Users } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import SupplierAutocomplete from '@/components/SupplierAutocomplete';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Textarea } from '@/components/ui/textarea';
import { createPageUrl } from '@/utils';
import CompetitiveExposureSnapshot from '@/components/competitors/CompetitiveExposureSnapshot';
import RenewalExposureTimeline from '@/components/competitors/RenewalExposureTimeline';

const fmtEur = (v) => {
  if (!v) return '€0';
  if (v >= 1e9) return `€${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `€${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `€${(v / 1e3).toFixed(0)}K`;
  return `€${Number(v).toLocaleString()}`;
};

const fmtCluster = (c) => {
  if (!c) return 'Unknown';
  return c.replace('cluster_', '').split('_').map((w) => w[0].toUpperCase() + w.slice(1)).join(' ');
};

const countryLabel = (country) => {
  if (!country) return null;
  if (country === 'both') return 'IE & FR';
  if (country === 'FR') return 'France';
  if (country === 'ES') return 'Spain';
  if (country === 'IE') return 'Ireland';
  return country;
};

function parseDateOrNull(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function resolveRenewalDate(renewal) {
  const explicit = parseDateOrNull(renewal?.end_date);
  if (explicit) return explicit;

  const daysUntil = Number(renewal?.days_until_expiry);
  if (Number.isFinite(daysUntil)) {
    const fallback = new Date();
    fallback.setDate(fallback.getDate() + Math.max(0, Math.round(daysUntil)));
    return fallback;
  }
  return null;
}

function normalizeEntityName(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function entityTokens(value) {
  const stop = new Set(['ltd', 'limited', 'sa', 'sarl', 'llc', 'gmbh', 'inc', 'spa', 'sl', 'bv', 'plc', 'sas', 'the']);
  return normalizeEntityName(value)
    .split(' ')
    .map((token) => token.trim())
    .filter((token) => token.length >= 3 && !stop.has(token));
}

function matchesLegalEntityContract(contract, legalEntityName) {
  const supplier = String(contract?.supplier_raw || contract?.supplier_name_raw || contract?.supplier_name || '').trim();
  if (!supplier) return false;

  const normalizedSupplier = normalizeEntityName(supplier);
  const normalizedLegal = normalizeEntityName(legalEntityName);
  if (!normalizedSupplier || !normalizedLegal) return false;
  if (normalizedSupplier.includes(normalizedLegal) || normalizedLegal.includes(normalizedSupplier)) return true;

  const supplierTokens = new Set(entityTokens(supplier));
  const legalTokens = entityTokens(legalEntityName);
  if (supplierTokens.size === 0 || legalTokens.length === 0) return false;

  const overlap = legalTokens.filter((token) => supplierTokens.has(token)).length;
  return overlap >= Math.min(2, legalTokens.length);
}

function computeContractSignals(contracts = []) {
  const now = new Date();
  const last12Start = addMonths(now, -12);
  const prior12Start = addMonths(now, -24);
  const recent90Start = new Date(now);
  recent90Start.setDate(recent90Start.getDate() - 90);
  const prior90Start = new Date(now);
  prior90Start.setDate(prior90Start.getDate() - 180);

  let valueLast12 = 0;
  let valuePrior12 = 0;
  let winsRecent90 = 0;
  let winsPrior90 = 0;

  contracts.forEach((contract) => {
    const awardDate = parseDateOrNull(contract?.award_date);
    if (!awardDate) return;

    const value = Number(contract?.value_eur || 0);
    if (awardDate >= last12Start) valueLast12 += value;
    else if (awardDate >= prior12Start && awardDate < last12Start) valuePrior12 += value;

    if (awardDate >= recent90Start) winsRecent90 += 1;
    else if (awardDate >= prior90Start && awardDate < recent90Start) winsPrior90 += 1;
  });

  return {
    contractValueTrendDeclining: valuePrior12 > 0 && valueLast12 < (valuePrior12 * 0.9),
    momentumPositive: winsRecent90 > (winsPrior90 * 1.1) && winsRecent90 > 0
  };
}

function computeBuyerConcentrationTop3Pct(buyers = []) {
  const values = buyers
    .map((buyer) => Number(buyer?.total_value || 0))
    .filter((value) => Number.isFinite(value) && value > 0)
    .sort((a, b) => b - a);

  if (values.length === 0) return 0;
  const total = values.reduce((sum, value) => sum + value, 0);
  if (total <= 0) return 0;
  const top3 = values.slice(0, 3).reduce((sum, value) => sum + value, 0);
    return (top3 / total) * 100;
}

function computeLowLockInFromRenewals(renewals = []) {
  let hasRepeatWinsData = false;
  let count = 0;

  renewals.forEach((renewal) => {
    const repeatWins = Number(renewal?.repeat_wins ?? renewal?.incumbent_repeat_wins ?? renewal?.repeat_win_count);
    if (!Number.isFinite(repeatWins)) return;
    hasRepeatWinsData = true;
    if (repeatWins <= 1) count += 1;
  });

  return { count, hasRepeatWinsData };
}

function computeLowLockInFromContracts(contracts = []) {
  const now = new Date();
  const horizon = addMonths(now, 12);
  const buyerWinCount = new Map();

  contracts.forEach((contract) => {
    const buyerName = String(contract?.buyer_name || '').trim();
    if (!buyerName) return;
    buyerWinCount.set(buyerName, (buyerWinCount.get(buyerName) || 0) + 1);
  });

  return contracts.filter((contract) => {
    const buyerName = String(contract?.buyer_name || '').trim();
    if (!buyerName) return false;
    const endDate = parseDateOrNull(contract?.end_date);
    if (!endDate || endDate < now || endDate > horizon) return false;
    return (buyerWinCount.get(buyerName) || 0) <= 1;
  }).length;
}

function buildStrategicInsights({
  renewalExposureValue = 0,
  renewalExposureCount = 0,
  lowLockInCount = 0,
  buyerConcentrationTop3Pct = 0,
  contractValueTrendDeclining = false,
  momentumPositive = false
}) {
  const insights = [];

  if (renewalExposureCount > 0) {
    insights.push(`${fmtEur(renewalExposureValue)} across ${renewalExposureCount} contract${renewalExposureCount === 1 ? '' : 's'} expiring in next 12 months.`);
  }

  if (lowLockInCount > 0) {
    insights.push(`${lowLockInCount} renewal window${lowLockInCount === 1 ? '' : 's'} show low incumbent lock-in - potential displacement opportunity.`);
  }

  if (buyerConcentrationTop3Pct > 60) {
    insights.push(`Top 3 buyers represent ${buyerConcentrationTop3Pct.toFixed(1)}% of total contract value - concentration exposure.`);
  }

  if (contractValueTrendDeclining) {
    insights.push('Contract value trend declining over last 12 months.');
  }

  if (momentumPositive) {
    insights.push('Recent contract momentum increasing.');
  }

  if (insights.length === 0) {
    insights.push('No significant exposure or concentration risks detected.');
  }

  return insights.slice(0, 5);
}

function buildCompetitorAnalysis(companyName, data) {
  const summary = data?.summary;
  const categories = data?.category_breakdown || [];
  const buyers = data?.buyer_relationships || [];
  const renewals = data?.renewal_opportunities || [];
  const trendData = data?.yearly_trend || [];

  let overallTrend = 'stable';
  if (trendData.length >= 3) {
    const recent = trendData.slice(-2);
    const earlier = trendData.slice(-4, -2);
    const recentAvg = recent.reduce((s, t) => s + t.awards, 0) / recent.length;
    const earlierAvg = earlier.length > 0 ? earlier.reduce((s, t) => s + t.awards, 0) / earlier.length : recentAvg;
    if (recentAvg > earlierAvg * 1.2) overallTrend = 'growing';
    else if (recentAvg < earlierAvg * 0.8) overallTrend = 'declining';
  }

  const strengths = [];
  if (summary?.total_awards > 20) strengths.push(`${summary.total_awards} contracts over ${summary.years_active} years`);
  else if (summary?.total_awards > 5) strengths.push(`${summary.total_awards} public contracts awarded`);
  if (summary?.has_frameworks > 0) strengths.push(`${summary.has_frameworks} framework agreements`);

  const strongBuyers = buyers.filter((b) => b.relationship_strength === 'strong');
  if (strongBuyers.length > 0) {
    strengths.push(`Strong buyer ties: ${strongBuyers.slice(0, 3).map((b) => b.buyer_name).join(', ')}`);
  }

  if (summary?.active_contracts > 3) strengths.push(`${summary.active_contracts} active contracts`);
  if (categories.length > 2) strengths.push(`Diversified across ${categories.length} categories`);

  return {
    success: true,
    company_name: companyName,
    found_tenders: summary?.total_awards || 0,
    summary,
    renewal_opportunities: renewals,
    buyer_relationships: buyers,
    category_breakdown: categories,
    yearly_trend: trendData,
    recent_contracts: data?.recent_contracts || [],
    trading_names: data?.trading_names || [],
    trend: overallTrend,
    analysis: {
      strengths
    }
  };
}

function MetricTile({ label, value, hint }) {
  return (
    <Card className="h-full border border-white/[0.05] bg-white/[0.015] shadow-none">
      <CardContent className="flex h-full flex-col items-center justify-center px-3.5 py-4 min-[900px]:px-4 min-[900px]:py-5 text-center">
        <p className="whitespace-nowrap text-[11px] uppercase tracking-[0.08em] leading-none text-muted-foreground">{label}</p>
        <p className="mt-2.5 text-[clamp(1.7rem,2.9vw,2.1rem)] font-semibold leading-none tracking-[-0.01em] text-card-foreground tabular-nums">{value}</p>
        {hint ? <p className="mt-2 text-xs leading-none text-muted-foreground">{hint}</p> : null}
      </CardContent>
    </Card>
  );
}

function CompetitorDossier({
  competitor,
  competitors,
  analysis,
  loading,
  onBack,
  onSwitch,
  onEdit,
  researchCompetitor,
  agentBrief,
  agentLoading,
  onDelete
}) {
  const summary = analysis?.summary;
  const [scopeMode, setScopeMode] = useState('group');

  useEffect(() => {
    setScopeMode('group');
  }, [competitor?.id]);

  const groupStrengths = analysis?.analysis?.strengths || [];
  const groupCategories = [...(analysis?.category_breakdown || [])]
    .sort((a, b) => Number(b.award_count || 0) - Number(a.award_count || 0))
    .slice(0, 5);
  const groupBuyers = analysis?.buyer_relationships || [];
  const groupContracts = analysis?.recent_contracts || [];
  const groupRenewalRows = analysis?.renewal_opportunities || [];

  const groupRenewalExposureValue = useMemo(
    () => groupRenewalRows.reduce((sum, row) => sum + Number(row?.value_eur || 0), 0),
    [groupRenewalRows]
  );
  const groupLowLockInSummary = useMemo(() => computeLowLockInFromRenewals(groupRenewalRows), [groupRenewalRows]);
  const groupLowLockInCount = useMemo(
    () => (groupLowLockInSummary.hasRepeatWinsData ? groupLowLockInSummary.count : computeLowLockInFromContracts(groupContracts)),
    [groupContracts, groupLowLockInSummary]
  );
  const groupBuyerConcentrationTop3Pct = useMemo(
    () => computeBuyerConcentrationTop3Pct(groupBuyers),
    [groupBuyers]
  );
  const groupContractSignals = useMemo(() => computeContractSignals(groupContracts), [groupContracts]);

  const legalScope = useMemo(() => {
    if (!Array.isArray(groupContracts) || groupContracts.length === 0) {
      return {
        summary: {
          total_awards: 0,
          total_value_eur: 0,
          avg_contract_value_eur: 0,
          distinct_buyers: 0,
          active_contracts: 0,
          has_frameworks: 0,
          max_contract_value_eur: 0,
          years_active: 0
        },
        strengths: [],
        categories: [],
        buyers: [],
        contracts: [],
        renewalExposureCount: 0,
        renewalExposureValue: 0
      };
    }

    const hasSupplierRaw = groupContracts.some((contract) => String(contract?.supplier_raw || '').trim().length > 0);
    const matchedContracts = hasSupplierRaw
      ? groupContracts.filter((contract) => matchesLegalEntityContract(contract, competitor?.company_name || ''))
      : groupContracts;

    const now = new Date();
    const horizon = addMonths(now, 12);
    const totalValue = matchedContracts.reduce((sum, contract) => sum + Number(contract?.value_eur || 0), 0);
    const maxValue = matchedContracts.reduce((max, contract) => Math.max(max, Number(contract?.value_eur || 0)), 0);
    const buyerSet = new Set(matchedContracts.map((contract) => String(contract?.buyer_name || '').trim()).filter(Boolean));
    const activeContracts = matchedContracts.filter((contract) => {
      const endDate = parseDateOrNull(contract?.end_date);
      return endDate ? endDate > now : false;
    }).length;
    const frameworkCount = matchedContracts.filter((contract) => Boolean(contract?.framework_flag)).length;

    const awardDates = matchedContracts
      .map((contract) => parseDateOrNull(contract?.award_date))
      .filter(Boolean);
    const yearsActive = awardDates.length > 0
      ? Math.max(1, Math.max(...awardDates.map((date) => date.getFullYear())) - Math.min(...awardDates.map((date) => date.getFullYear())) + 1)
      : 0;

    const renewalContracts = matchedContracts.filter((contract) => {
      const endDate = parseDateOrNull(contract?.end_date);
      return endDate ? endDate >= now && endDate <= horizon : false;
    });
    const renewalExposureValue = renewalContracts.reduce((sum, contract) => sum + Number(contract?.value_eur || 0), 0);

    const categoryMap = new Map();
    matchedContracts.forEach((contract) => {
      const cluster = contract?.cpv_cluster || 'Unknown';
      const existing = categoryMap.get(cluster) || {
        cluster,
        award_count: 0,
        total_value: 0,
        distinctBuyerSet: new Set(),
        active_contracts: 0
      };
      existing.award_count += 1;
      existing.total_value += Number(contract?.value_eur || 0);
      const buyer = String(contract?.buyer_name || '').trim();
      if (buyer) existing.distinctBuyerSet.add(buyer);
      const endDate = parseDateOrNull(contract?.end_date);
      if (endDate && endDate > now) existing.active_contracts += 1;
      categoryMap.set(cluster, existing);
    });

    const categories = [...categoryMap.values()]
      .map((entry) => ({
        cluster: entry.cluster,
        award_count: entry.award_count,
        total_value: entry.total_value,
        distinct_buyers: entry.distinctBuyerSet.size,
        active_contracts: entry.active_contracts
      }))
      .sort((a, b) => Number(b.total_value || 0) - Number(a.total_value || 0))
      .slice(0, 5);

    const buyerMap = new Map();
    matchedContracts.forEach((contract) => {
      const buyerName = String(contract?.buyer_name || '').trim();
      if (!buyerName) return;
      const existing = buyerMap.get(buyerName) || {
        buyer_name: buyerName,
        award_count: 0,
        total_value: 0,
        active_contracts: 0
      };
      existing.award_count += 1;
      existing.total_value += Number(contract?.value_eur || 0);
      const endDate = parseDateOrNull(contract?.end_date);
      if (endDate && endDate > now) existing.active_contracts += 1;
      buyerMap.set(buyerName, existing);
    });

    const buyers = [...buyerMap.values()]
      .map((buyer) => ({
        ...buyer,
        relationship_strength: buyer.award_count >= 5 ? 'strong' : buyer.award_count >= 3 ? 'moderate' : 'emerging'
      }))
      .sort((a, b) => Number(b.total_value || 0) - Number(a.total_value || 0));

    const strengths = [];
    if (matchedContracts.length > 0) strengths.push(`${matchedContracts.length} contracts attributed to this legal entity`);
    if (frameworkCount > 0) strengths.push(`${frameworkCount} framework agreement${frameworkCount === 1 ? '' : 's'} in force`);
    if (buyerSet.size > 0) strengths.push(`Active across ${buyerSet.size} public bod${buyerSet.size === 1 ? 'y' : 'ies'}`);
    if (renewalContracts.length > 0) strengths.push(`${renewalContracts.length} renewal window${renewalContracts.length === 1 ? '' : 's'} within 12 months`);
    if (categories.length > 1) strengths.push(`Category footprint concentrated in ${categories.length} sectors`);
    const lowLockInCount = computeLowLockInFromContracts(matchedContracts);
    const buyerConcentrationTop3Pct = computeBuyerConcentrationTop3Pct(buyers);
    const contractSignals = computeContractSignals(matchedContracts);

    return {
      summary: {
        total_awards: matchedContracts.length,
        total_value_eur: totalValue,
        avg_contract_value_eur: matchedContracts.length > 0 ? totalValue / matchedContracts.length : 0,
        distinct_buyers: buyerSet.size,
        active_contracts: activeContracts,
        has_frameworks: frameworkCount,
        max_contract_value_eur: maxValue,
        years_active: yearsActive
      },
      strengths,
      categories,
      buyers,
      contracts: matchedContracts,
      renewalExposureCount: renewalContracts.length,
      renewalExposureValue,
      lowLockInCount,
      buyerConcentrationTop3Pct,
      contractValueTrendDeclining: contractSignals.contractValueTrendDeclining,
      momentumPositive: contractSignals.momentumPositive
    };
  }, [competitor?.company_name, groupContracts]);

  const groupScope = useMemo(() => ({
    summary: summary || {
      total_awards: 0,
      total_value_eur: 0,
      avg_contract_value_eur: 0,
      distinct_buyers: 0,
      active_contracts: 0,
      has_frameworks: 0,
      max_contract_value_eur: 0,
      years_active: 0
    },
    strengths: groupStrengths,
    categories: groupCategories,
    buyers: groupBuyers,
    contracts: groupContracts,
    renewalExposureCount: groupRenewalRows.length,
    renewalExposureValue: groupRenewalExposureValue,
    lowLockInCount: groupLowLockInCount,
    buyerConcentrationTop3Pct: groupBuyerConcentrationTop3Pct,
    contractValueTrendDeclining: groupContractSignals.contractValueTrendDeclining,
    momentumPositive: groupContractSignals.momentumPositive
  }), [
    groupBuyerConcentrationTop3Pct,
    groupBuyers,
    groupCategories,
    groupContractSignals.contractValueTrendDeclining,
    groupContractSignals.momentumPositive,
    groupContracts,
    groupLowLockInCount,
    groupRenewalExposureValue,
    groupRenewalRows.length,
    groupStrengths,
    summary
  ]);

  const scoped = scopeMode === 'legal' ? legalScope : groupScope;
  const scopedSummary = scoped.summary;
  const scopedStrengths = scoped.strengths || [];
  const scopedCategories = scoped.categories || [];
  const scopedBuyers = scoped.buyers || [];
  const scopedContracts = scoped.contracts || [];
  const strategicInsights = buildStrategicInsights({
    renewalExposureValue: Number(scoped.renewalExposureValue || 0),
    renewalExposureCount: Number(scoped.renewalExposureCount || 0),
    lowLockInCount: Number(scoped.lowLockInCount || 0),
    buyerConcentrationTop3Pct: Number(scoped.buyerConcentrationTop3Pct || 0),
    contractValueTrendDeclining: Boolean(scoped.contractValueTrendDeclining),
    momentumPositive: Boolean(scoped.momentumPositive)
  });

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3 md:gap-4">
        <div className="min-w-0 flex-1 space-y-1">
          <Button variant="ghost" size="sm" className="-ml-2 text-slate-300" onClick={onBack}>
            <ArrowLeft className="mr-2 h-4 w-4" />
            Competitors / {competitor.company_name}
          </Button>
          <h1 className="max-w-full truncate whitespace-nowrap text-[clamp(1.9rem,3.1vw,2.6rem)] font-semibold leading-tight tracking-tight text-slate-100">
            {competitor.company_name}
          </h1>
          {scopeMode === 'legal' ? (
            <p className="text-xs text-muted-foreground">Viewing: Legal Entity Only</p>
          ) : null}
        </div>

        <div className="ml-auto flex w-full flex-wrap items-center gap-2 sm:w-auto sm:justify-end">
          <div className="inline-flex items-center rounded-lg bg-white/[0.03] p-1">
            <button
              type="button"
              className={`rounded-md px-2.5 py-1.5 text-xs transition-colors min-[900px]:px-3 ${
                scopeMode === 'group'
                  ? 'bg-white/[0.08] text-slate-100'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
              onClick={() => setScopeMode('group')}
            >
              Group View
            </button>
            <button
              type="button"
              className={`rounded-md px-2.5 py-1.5 text-xs transition-colors min-[900px]:px-3 ${
                scopeMode === 'legal'
                  ? 'bg-white/[0.08] text-slate-100'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
              onClick={() => setScopeMode('legal')}
            >
              Legal Entity
            </button>
          </div>
          {competitors.length > 1 ? (
            <Select value={String(competitor.id)} onValueChange={onSwitch}>
              <SelectTrigger className="h-9 w-full min-w-[180px] min-[600px]:w-[220px] min-[900px]:w-[260px] border-white/[0.08] bg-white/[0.02] text-sm text-slate-300">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {competitors.map((item) => (
                  <SelectItem key={item.id} value={String(item.id)}>{item.company_name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          ) : null}
          <Button variant="ghost" size="icon" onClick={() => onEdit(competitor)}><Edit2 className="h-4 w-4 text-slate-400" /></Button>
          <Button variant="ghost" size="icon" onClick={() => onDelete(competitor.id)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
        </div>
      </div>

      <section className="rounded-2xl border border-white/[0.06] bg-white/[0.015] p-3.5 min-[900px]:p-5">
        {loading ? (
          <div className="flex min-h-[220px] items-center justify-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Building competitor dossier...
          </div>
        ) : !scopedSummary ? (
          <div className="space-y-1 py-8 text-center text-sm text-muted-foreground">
            <p>No dossier data available for this competitor yet.</p>
            <p>Try another competitor or refresh after new awards are indexed.</p>
          </div>
        ) : (
          <Tabs defaultValue="overview" className="w-full">
            <TabsList className="bg-slate-900/60 border border-white/[0.06]">
              <TabsTrigger value="overview" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">Overview</TabsTrigger>
              <TabsTrigger value="accounts" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">Accounts</TabsTrigger>
              <TabsTrigger value="contracts" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">Contracts</TabsTrigger>
            </TabsList>

            <TabsContent value="overview" className="mt-4 space-y-4">
              <div className="grid grid-cols-1 gap-3 min-[600px]:grid-cols-2 min-[900px]:grid-cols-3 min-[1200px]:grid-cols-5">
                <MetricTile label="Contracts" value={scopedSummary.total_awards || 0} hint={`${scopedSummary.years_active || 0} years active`} />
                <MetricTile label="Total Value" value={fmtEur(scopedSummary.total_value_eur)} hint={`avg ${fmtEur(scopedSummary.avg_contract_value_eur)}`} />
                <MetricTile label="Public Bodies" value={scopedSummary.distinct_buyers || 0} hint={`${scopedSummary.active_contracts || 0} active`} />
                <MetricTile label="Renewals (12 Months)" value={fmtEur(scoped.renewalExposureValue)} hint={`${scoped.renewalExposureCount || 0} window${scoped.renewalExposureCount === 1 ? '' : 's'}`} />
                <MetricTile label="Frameworks" value={scopedSummary.has_frameworks || 0} hint={`largest ${fmtEur(scopedSummary.max_contract_value_eur)}`} />
              </div>

              <section className="bg-white/[0.02] px-3.5 py-3.5 min-[900px]:px-5 min-[900px]:py-4">
                <h3 className="text-sm font-medium text-slate-200">Strategic Insights</h3>
                <ul className="mt-3 space-y-2">
                  {strategicInsights.map((insight, index) => (
                    <li key={`${insight}-${index}`} className="flex items-start gap-2 text-sm text-slate-300">
                      <span className="mt-[7px] h-1.5 w-1.5 rounded-full bg-slate-500" />
                      <span>{insight}</span>
                    </li>
                  ))}
                </ul>
              </section>

              <section className="rounded-xl border border-emerald-500/20 bg-emerald-500/[0.03] px-5 py-4 space-y-3">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-medium text-emerald-400/90">Civant Agent's Brief</h3>
                  <button
                    type="button"
                    onClick={researchCompetitor}
                    disabled={agentLoading}
                    className="inline-flex items-center gap-1.5 rounded-md border border-emerald-500/30 bg-emerald-500/[0.08] px-3 py-1.5 text-[11px] font-medium text-emerald-400 hover:bg-emerald-500/[0.15] hover:text-emerald-300 disabled:opacity-50 transition-colors"
                  >
                    {agentLoading ? (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    ) : (
                      <svg className="h-3 w-3" viewBox="0 0 16 16" fill="none"><path d="M8 1l1.5 4.5L14 7l-4.5 1.5L8 13l-1.5-4.5L2 7l4.5-1.5L8 1z" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" fill="currentColor" fillOpacity="0.15"/><path d="M13 1l.5 1.5L15 3l-1.5.5L13 5l-.5-1.5L11 3l1.5-.5L13 1z" stroke="currentColor" strokeWidth="0.8" strokeLinejoin="round" fill="currentColor" fillOpacity="0.2"/></svg>
                    )}
                    {agentBrief ? 'Refresh Analysis' : 'Analyze Competitor'}
                  </button>
                </div>
                {agentBrief ? (
                  <div className="space-y-3">
                    <p className="text-sm text-slate-200 leading-relaxed">{agentBrief.summary}</p>
                    {agentBrief.organizational_context?.trajectory || agentBrief.organizational_context?.threat_level ? (
                      <div className="flex items-center gap-4">
                        {agentBrief.organizational_context?.trajectory ? (
                          <div className="flex items-center gap-1.5">
                            <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Trajectory</span>
                            <span className={`text-xs font-medium ${agentBrief.organizational_context.trajectory === 'growing' ? 'text-red-400' : agentBrief.organizational_context.trajectory === 'declining' ? 'text-emerald-400' : 'text-slate-300'}`}>
                              {agentBrief.organizational_context.trajectory === 'growing' ? '\u2191 Growing' : agentBrief.organizational_context.trajectory === 'declining' ? '\u2193 Declining' : '\u2192 Stable'}
                            </span>
                          </div>
                        ) : null}
                        {agentBrief.organizational_context?.threat_level ? (
                          <div className="flex items-center gap-1.5">
                            <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Threat</span>
                            <span className={`text-xs font-medium ${agentBrief.organizational_context.threat_level === 'high' ? 'text-red-400' : agentBrief.organizational_context.threat_level === 'low' ? 'text-emerald-400' : 'text-amber-400'}`}>
                              {agentBrief.organizational_context.threat_level.charAt(0).toUpperCase() + agentBrief.organizational_context.threat_level.slice(1)}
                            </span>
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                    {agentBrief.risk_factors?.length > 0 ? (
                      <div>
                        <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-1">Vulnerabilities</p>
                        <ul className="space-y-1">
                          {agentBrief.risk_factors.slice(0, 3).map((v, i) => (
                            <li key={i} className="flex items-start gap-2 text-xs text-slate-300">
                              <span className="mt-1 h-1 w-1 rounded-full bg-emerald-400/60 shrink-0" />
                              <span>{v}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                    {agentBrief.procurement_intent?.notes ? (
                      <div className="pt-1 border-t border-emerald-500/10">
                        <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-1">Recommendation</p>
                        <p className="text-xs text-slate-300">{agentBrief.procurement_intent.notes}</p>
                      </div>
                    ) : null}
                    <p className="text-[10px] text-muted-foreground/50">
                      {agentBrief.researched_at ? `Last updated: ${new Date(agentBrief.researched_at).toLocaleDateString()}` : ''}
                    </p>
                  </div>
                ) : (
                  <p className="text-xs text-muted-foreground">Click "Analyze Competitor" to get AI-powered competitive intelligence including market trajectory, recent activity, and vulnerabilities.</p>
                )}
              </section>

              <div className="grid gap-4 min-[1100px]:grid-cols-2">
                <Card className="border border-white/[0.05] bg-white/[0.01] shadow-none">
                  <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-slate-300">Strengths</CardTitle></CardHeader>
                  <CardContent>
                    {scopedStrengths.length > 0 ? (
                      <ul className="space-y-2 text-sm text-slate-300">
                        {scopedStrengths.slice(0, 5).map((item, index) => (
                          <li key={`${item}-${index}`} className="flex items-start gap-2"><span className="mt-1 text-civant-teal">•</span><span>{item}</span></li>
                        ))}
                      </ul>
                    ) : (
                      <p className="text-sm text-muted-foreground">No strengths extracted yet.</p>
                    )}
                  </CardContent>
                </Card>

                <Card className="border border-white/[0.05] bg-white/[0.01] shadow-none">
                  <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-slate-300">Preferred Categories / Where They Win</CardTitle></CardHeader>
                  <CardContent>
                    {scopedCategories.length > 0 ? (
                      <div className="space-y-2">
                        {scopedCategories.map((cat, index) => (
                          <div key={`${cat.cluster}-${index}`} className="flex items-center justify-between gap-3 text-sm">
                            <div className="flex min-w-0 items-center gap-2 text-slate-300">
                              <span className="w-5 text-xs text-muted-foreground">{index + 1}</span>
                              <span className="truncate font-medium text-slate-100">{fmtCluster(cat.cluster)}</span>
                            </div>
                            <div className="shrink-0 text-right text-xs text-muted-foreground">
                              <p>{cat.award_count || 0} awards</p>
                              <p>{cat.distinct_buyers || 0} buyers · {fmtEur(cat.total_value)}</p>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="text-sm text-muted-foreground">No category concentration available.</p>
                    )}
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="accounts" className="mt-4">
              <Card className="border border-white/[0.05] bg-white/[0.01] shadow-none">
                <CardContent className="pt-4">
                  {scopedBuyers.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-white/[0.05] text-xs uppercase tracking-[0.08em] text-slate-500">
                            <th className="py-3 pr-4 text-left">Buyer</th>
                            <th className="px-4 py-3 text-right">Awards</th>
                            <th className="px-4 py-3 text-right">Total Value</th>
                            <th className="py-3 pl-4 text-left">Relationship</th>
                          </tr>
                        </thead>
                        <tbody>
                          {scopedBuyers.map((buyer, index) => (
                            <tr key={`${buyer.buyer_name}-${index}`} className="border-b border-white/[0.04]">
                              <td className="py-3 pr-4 font-medium text-slate-100">{buyer.buyer_name}</td>
                              <td className="px-4 py-3 text-right text-slate-300">{buyer.award_count || 0}</td>
                              <td className="px-4 py-3 text-right text-slate-300">{fmtEur(buyer.total_value)}</td>
                              <td className="py-3 pl-4 text-slate-300">{buyer.relationship_strength || '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="py-8 text-center text-sm text-muted-foreground">No buyer account data available.</p>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="contracts" className="mt-4">
              <Card className="border border-white/[0.05] bg-white/[0.01] shadow-none">
                <CardHeader className="pb-3"><CardTitle className="text-sm font-medium text-slate-300">Recent Contracts</CardTitle></CardHeader>
                <CardContent>
                  {scopedContracts.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-white/[0.05] text-xs uppercase tracking-[0.08em] text-slate-500">
                            <th className="py-3 pr-4 text-left">Buyer</th>
                            <th className="px-4 py-3 text-right">Value</th>
                            <th className="px-4 py-3 text-left">Category</th>
                            <th className="px-4 py-3 text-left">Awarded</th>
                            <th className="py-3 pl-4 text-left">Ends</th>
                          </tr>
                        </thead>
                        <tbody>
                          {scopedContracts.map((contract, index) => (
                            <tr key={`${contract.buyer_name}-${index}`} className="border-b border-white/[0.04]">
                              <td className="py-3 pr-4 font-medium text-slate-100">{contract.buyer_name}</td>
                              <td className="px-4 py-3 text-right text-slate-300">{fmtEur(contract.value_eur)}</td>
                              <td className="px-4 py-3 text-slate-300">{fmtCluster(contract.cpv_cluster)}</td>
                              <td className="px-4 py-3 text-slate-300">{contract.award_date || '—'}</td>
                              <td className="py-3 pl-4 text-slate-300">{contract.end_date || '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="py-8 text-center text-sm text-muted-foreground">No recent contracts available.</p>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        )}
      </section>
    </div>
  );
}

export default function Competitors() {
  const location = useLocation();
  const navigate = useNavigate();
  const { competitorId } = useParams();
  const { activeTenantId } = useTenant();

  const [user, setUser] = useState(null);
  const [competitors, setCompetitors] = useState([]);
  const [companyProfile, setCompanyProfile] = useState(null);
  const [loading, setLoading] = useState(true);

  const [search, setSearch] = useState('');
  const [showForm, setShowForm] = useState(false);
  const [editingCompetitor, setEditingCompetitor] = useState(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState('');
  const [analyzingId, setAnalyzingId] = useState(null);
  const [snapshotLoading, setSnapshotLoading] = useState(false);
  const [analysisByCompetitorId, setAnalysisByCompetitorId] = useState({});
  const [agentBrief, setAgentBrief] = useState(null);
  const [agentLoading, setAgentLoading] = useState(false);
  const prefetchingPortfolioRef = useRef(false);

  const [formData, setFormData] = useState({
    company_name: '',
    website: '',
    search_terms: '',
    country: '',
    industry_sectors: '',
    notes: '',
    active: true
  });

  const resetForm = useCallback(() => {
    setFormData({ company_name: '',
    website: '',
    search_terms: '', country: '', industry_sectors: '', notes: '', active: true });
  }, []);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const userData = await civant.auth.me();
      setUser(userData);
      const competitorsData = await civant.entities.Competitors.filter({ tracked_by_user: userData.email });
      setCompetitors(competitorsData || []);

      if (activeTenantId) {
        const profileRows = await civant.entities.company_profiles.filter(
          { tenant_id: activeTenantId },
          '-updated_at',
          1,
          'known_competitors,contract_size_min_eur'
        );
        setCompanyProfile(Array.isArray(profileRows) && profileRows.length > 0 ? profileRows[0] : null);
      } else {
        setCompanyProfile(null);
      }
    } catch (error) {
      console.error('Error loading competitors:', error);
      setCompetitors([]);
      setCompanyProfile(null);
    } finally {
      setLoading(false);
    }
  }, [activeTenantId]);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    if (params.get('quickAdd') === '1') {
      setEditingCompetitor(null);
      resetForm();
      setShowForm(true);
    }
  }, [location.search, resetForm]);

  const selectedCompetitor = useMemo(
    () => competitors.find((c) => String(c.id) === String(competitorId || '')) || null,
    [competitorId, competitors]
  );

  const researchCompetitor = async () => {
    if (agentLoading) return;
    setAgentLoading(true);
    try {
      const { data: { session } } = await supabase.auth.getSession();
      const res = await fetch(
        `${import.meta.env.VITE_SUPABASE_URL || 'https://ossoggqkqifdkihybbew.supabase.co'}/functions/v1/research-buyer`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${session?.access_token || import.meta.env.VITE_SUPABASE_ANON_KEY}` },
          body: JSON.stringify({
            buyer_name: selectedCompetitor?.company_name || selectedCompetitor?.name,
            country: selectedCompetitor?.country || 'IE',
            context: 'competitor',
            tenant_id: activeTenantId,
          }),
        }
      );
      const data = await res.json();
      if (data.brief) setAgentBrief(data.brief);
    } catch (err) {
      console.error('Civant Agent error:', err);
      setAgentBrief({ summary: 'Research unavailable. Please try again.', _error: true });
    } finally {
      setAgentLoading(false);
    }
  };

  useEffect(() => {
    if (!selectedCompetitor?.company_name) return;
    setAgentBrief(null);
    const loadCached = async () => {
      try {
        const { data: { session } } = await supabase.auth.getSession();
        const res = await fetch(
          `${import.meta.env.VITE_SUPABASE_URL || 'https://ossoggqkqifdkihybbew.supabase.co'}/functions/v1/research-buyer`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${session?.access_token || import.meta.env.VITE_SUPABASE_ANON_KEY}` },
            body: JSON.stringify({
              buyer_name: selectedCompetitor?.company_name || selectedCompetitor?.name,
              country: selectedCompetitor?.country || 'IE',
              context: 'competitor',
              tenant_id: activeTenantId,
            }),
          }
        );
        const data = await res.json();
        if (data.source === 'cache' && data.brief) setAgentBrief(data.brief);
      } catch (_) {}
    };
    loadCached();
  }, [selectedCompetitor?.company_name]);

  const analyzeCompetitor = useCallback(async (competitor) => {
    if (!competitor || !activeTenantId) return null;

    setAnalyzingId(competitor.id);
    try {
      const { data: response, error: rpcError } = await supabase.rpc('get_competitor_intelligence', {
        p_tenant_id: activeTenantId,
        p_search_term: (competitor.search_terms || competitor.company_name).split(",")[0].trim()
      });
      if (rpcError) throw new Error(rpcError.message);

      const payload = response?.data || response;
      if (payload?.error) throw new Error(payload.error);
      if (!payload || !payload.success) {
        throw new Error(payload?.message || 'No awards found for this competitor');
      }

      const dossierData = buildCompetitorAnalysis(competitor.company_name, payload);
      setAnalysisByCompetitorId((prev) => ({ ...prev, [competitor.id]: dossierData }));
      return dossierData;
    } catch (error) {
      console.error('Competitor analysis failed:', error);
      setAnalysisByCompetitorId((prev) => ({
        ...prev,
        [competitor.id]: {
          success: false,
          company_name: competitor.company_name,
          message: error.message
        }
      }));
      return null;
    } finally {
      setAnalyzingId(null);
    }
  }, [activeTenantId]);

  useEffect(() => {
    if (!selectedCompetitor || loading) return;
    const existing = analysisByCompetitorId[selectedCompetitor.id];
    if (!existing && analyzingId !== selectedCompetitor.id) {
      void analyzeCompetitor(selectedCompetitor);
    }
  }, [analyzeCompetitor, analyzingId, analysisByCompetitorId, loading, selectedCompetitor]);

  useEffect(() => {
    if (competitorId || loading || competitors.length === 0 || !activeTenantId || prefetchingPortfolioRef.current) return;

    const missing = competitors.filter((competitor) => !analysisByCompetitorId[competitor.id]);
    if (missing.length === 0) {
      setSnapshotLoading(false);
      return;
    }

    let cancelled = false;
    prefetchingPortfolioRef.current = true;
    setSnapshotLoading(true);

    const run = async () => {
      for (const competitor of missing) {
        if (cancelled) return;
        await analyzeCompetitor(competitor);
      }
      if (!cancelled) setSnapshotLoading(false);
      prefetchingPortfolioRef.current = false;
    };

    void run();
    return () => {
      cancelled = true;
      prefetchingPortfolioRef.current = false;
    };
  }, [activeTenantId, analysisByCompetitorId, analyzeCompetitor, competitorId, competitors, loading]);

  const handleSubmit = async (event) => {
    event.preventDefault();
    setSaving(true);
    setSaveError('');
    try {
      const payload = { ...formData, tracked_by_user: user?.email, active: true };
      if (editingCompetitor) {
        await civant.entities.Competitors.update(editingCompetitor.id, payload);
      } else {
        await civant.entities.Competitors.create(payload);
      }
      setShowForm(false);
      setEditingCompetitor(null);
      resetForm();
      await loadData();
    } catch (error) {
      setSaveError(error.message || 'Failed to save competitor.');
    } finally {
      setSaving(false);
    }
  };

  const handleEdit = (competitor) => {
    setEditingCompetitor(competitor);
    setFormData({
      company_name: competitor.company_name || '',
      website: competitor.website || '',
      search_terms: competitor.search_terms || '',
      country: competitor.country || '',
      industry_sectors: competitor.industry_sectors || '',
      notes: competitor.notes || '',
      active: competitor.active !== false
    });
    setShowForm(true);
  };

  const handleDelete = async (id) => {
    if (!confirm('Remove this competitor?')) return;
    try {
      await civant.entities.Competitors.delete(id);
      setAnalysisByCompetitorId((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      await loadData();
      if (String(competitorId || '') === String(id)) {
        navigate(createPageUrl('competitors'));
      }
    } catch (error) {
      console.error('Delete failed:', error);
    }
  };

  const filteredCompetitors = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return competitors;
    return competitors.filter((competitor) => {
      const name = String(competitor.company_name || '').toLowerCase();
      const sector = String(competitor.industry_sectors || '').toLowerCase();
      const country = String(competitor.country || '').toLowerCase();
      return name.includes(needle) || sector.includes(needle) || country.includes(needle);
    });
  }, [competitors, search]);

  const exposurePortfolio = useMemo(
    () => competitors
      .map((competitor) => ({
        competitor,
        dossier: analysisByCompetitorId[competitor.id]
      }))
      .filter((entry) => entry.dossier?.success && entry.dossier?.summary),
    [analysisByCompetitorId, competitors]
  );

  const renewalExposureRows = useMemo(
    () => exposurePortfolio.flatMap((entry) => (entry.dossier?.renewal_opportunities || []).map((renewal) => ({
      ...renewal,
      sourceCompetitor: entry.competitor.company_name
    }))),
    [exposurePortfolio]
  );

  const highValueThreshold = useMemo(() => {
    const raw = Number(companyProfile?.contract_size_min_eur ?? 0);
    return Number.isFinite(raw) && raw > 0 ? raw : 1_000_000;
  }, [companyProfile]);

  const exposureSnapshot = useMemo(() => {
    const renewalExposure = renewalExposureRows.length;
    const highValueWindows = renewalExposureRows.filter((row) => Number(row?.value_eur || 0) >= highValueThreshold).length;

    const buyerFrequency = new Map();
    const buyerSetsByCompetitor = exposurePortfolio.map((entry) => new Set(
      (entry.dossier?.buyer_relationships || [])
        .map((buyer) => String(buyer?.buyer_name || '').trim())
        .filter(Boolean)
    ));
    buyerSetsByCompetitor.forEach((set) => {
      set.forEach((buyerName) => {
        buyerFrequency.set(buyerName, (buyerFrequency.get(buyerName) || 0) + 1);
      });
    });

    const totalUniqueBuyers = buyerFrequency.size;
    const sharedBuyers = [...buyerFrequency.values()].filter((count) => count > 1).length;
    const sharedBuyerOverlapPct = totalUniqueBuyers > 0 ? Math.round((sharedBuyers / totalUniqueBuyers) * 100) : 0;

    const now = new Date();
    const recentBoundary = new Date(now);
    recentBoundary.setDate(recentBoundary.getDate() - 90);
    const priorBoundary = new Date(now);
    priorBoundary.setDate(priorBoundary.getDate() - 180);

    let recentWins = 0;
    let priorWins = 0;
    exposurePortfolio.forEach((entry) => {
      (entry.dossier?.recent_contracts || []).forEach((contract) => {
        const awardDate = parseDateOrNull(contract?.award_date);
        if (!awardDate) return;
        if (awardDate >= recentBoundary) recentWins += 1;
        else if (awardDate >= priorBoundary && awardDate < recentBoundary) priorWins += 1;
      });
    });

    let momentumLabel = 'Flat';
    if (recentWins > priorWins * 1.1) momentumLabel = 'Up';
    else if (recentWins < priorWins * 0.9) momentumLabel = 'Down';

    return {
      renewalExposure,
      highValueWindows,
      sharedBuyerOverlapPct,
      momentumLabel,
      recentWins,
      priorWins
    };
  }, [exposurePortfolio, highValueThreshold, renewalExposureRows]);

  const exposureTimeline = useMemo(() => {
    const start = startOfMonth(new Date());
    const end = addMonths(start, 12);
    const months = Array.from({ length: 12 }, (_, index) => {
      const monthDate = addMonths(start, index);
      return {
        key: format(monthDate, 'yyyy-MM'),
        label: format(monthDate, 'MMM'),
        competitorExposure: 0
      };
    });
    const bucketByMonth = new Map(months.map((month) => [month.key, month]));

    renewalExposureRows.forEach((renewal) => {
      const renewalDate = resolveRenewalDate(renewal);
      if (!renewalDate || renewalDate < start || renewalDate >= end) return;
      const key = format(startOfMonth(renewalDate), 'yyyy-MM');
      const bucket = bucketByMonth.get(key);
      if (bucket) bucket.competitorExposure += 1;
    });

    return months;
  }, [renewalExposureRows]);

  const emergingCompetitors = useMemo(() => {
    const tracked = new Set(competitors.map((competitor) => String(competitor.company_name || '').trim().toLowerCase()));
    const byName = new Map();

    exposurePortfolio.forEach((entry) => {
      const buyerCount = (entry.dossier?.buyer_relationships || []).length;
      const cpvOverlap = (entry.dossier?.category_breakdown || []).slice(0, 3).length;
      const latestAwardMs = (entry.dossier?.recent_contracts || [])
        .map((contract) => parseDateOrNull(contract?.award_date))
        .filter(Boolean)
        .map((date) => date.getTime())
        .sort((a, b) => b - a)[0];
      const recentAwardDays = Number.isFinite(latestAwardMs)
        ? Math.max(0, differenceInCalendarDays(new Date(), new Date(latestAwardMs)))
        : null;

      (entry.dossier?.trading_names || []).forEach((candidate) => {
        const rawName = String(candidate?.name || '').trim();
        const normalized = rawName.toLowerCase();
        if (!rawName || tracked.has(normalized)) return;

        const entryScore = Number(candidate?.award_count || 0) * 3 + Math.min(buyerCount, 20) + (cpvOverlap * 2);
        const existing = byName.get(normalized) || {
          name: rawName,
          score: 0,
          sharedBuyers: 0,
          cpvOverlap: 0,
          recentAwardDays
        };
        existing.score += entryScore;
        existing.sharedBuyers = Math.max(existing.sharedBuyers, buyerCount);
        existing.cpvOverlap = Math.max(existing.cpvOverlap, cpvOverlap);
        if (recentAwardDays !== null) {
          existing.recentAwardDays = existing.recentAwardDays === null
            ? recentAwardDays
            : Math.min(existing.recentAwardDays, recentAwardDays);
        }
        byName.set(normalized, existing);
      });
    });

    if (byName.size === 0 && Array.isArray(companyProfile?.known_competitors)) {
      companyProfile.known_competitors.forEach((name) => {
        const trimmed = String(name || '').trim();
        const normalized = trimmed.toLowerCase();
        if (!trimmed || tracked.has(normalized)) return;
        byName.set(normalized, {
          name: trimmed,
          score: 1,
          sharedBuyers: 0,
          cpvOverlap: 0,
          recentAwardDays: null
        });
      });
    }

    return [...byName.values()]
      .sort((a, b) => b.score - a.score)
      .slice(0, 5);
  }, [companyProfile?.known_competitors, competitors, exposurePortfolio]);

  if (loading) {
    return (
      <div className="flex h-64 items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-civant-teal" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {!competitorId ? (
        <>
          <div className="space-y-4">
            <div className="space-y-1 pb-2">
              <h1 className="text-4xl font-semibold tracking-tight text-card-foreground md:text-5xl">Competitors</h1>
              <p className="text-base text-muted-foreground md:text-lg">Competitive exposure portfolio</p>
            </div>

            <div className="max-w-md flex gap-2">
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' && search.trim()) {
                    setEditingCompetitor(null);
                    setFormData(prev => ({ ...prev, company_name: search.trim() }));
                    setShowForm(true);
                  }
                }}
                placeholder="Add a competitor by name..."
                className="h-10 border-white/[0.08] bg-white/[0.02]"
              />
              <Button
                variant="outline"
                className="h-10 shrink-0"
                onClick={() => {
                  setEditingCompetitor(null);
                  resetForm();
                  if (search.trim()) setFormData(prev => ({ ...prev, company_name: search.trim() }));
                  setShowForm(true);
                }}
              >
                + Add
              </Button>
            </div>
          </div>

          <CompetitiveExposureSnapshot
            loading={snapshotLoading}
            metrics={[
              {
                label: 'Renewal Exposure (Next 12 Months)',
                value: exposureSnapshot.renewalExposure.toLocaleString(),
                hint: 'Total renewal windows across tracked competitors'
              },
              {
                label: 'High-Value Windows',
                value: exposureSnapshot.highValueWindows.toLocaleString(),
                hint: `Value at or above ${fmtEur(highValueThreshold)}`
              },
              {
                label: 'Shared Buyer Overlap',
                value: `${exposureSnapshot.sharedBuyerOverlapPct}%`,
                hint: 'Buyer overlap across tracked competitors'
              },
              {
                label: 'Competitive Momentum (90d)',
                value: exposureSnapshot.momentumLabel,
                hint: `${exposureSnapshot.recentWins} wins vs ${exposureSnapshot.priorWins} in prior 90d`
              }
            ]}
          />

          <RenewalExposureTimeline data={exposureTimeline} loading={snapshotLoading} />

          <section className="space-y-3">
            <h3 className="text-base font-semibold text-card-foreground">Emerging Competitors</h3>
            {snapshotLoading ? (
              <div className="py-4 text-sm text-muted-foreground">Scanning emerging competitors...</div>
            ) : emergingCompetitors.length > 0 ? (
              <div className="divide-y divide-white/[0.06] rounded-2xl bg-white/[0.015] px-4">
                {emergingCompetitors.map((item) => (
                  <div key={item.name} className="flex flex-wrap items-center justify-between gap-3 py-3">
                    <p className="text-sm font-medium text-slate-100">{item.name}</p>
                    <p className="text-xs text-muted-foreground">
                      Shared buyers {item.sharedBuyers} · CPV overlap {item.cpvOverlap}
                      {item.recentAwardDays !== null ? ` · ${item.recentAwardDays}d since recent award` : ''}
                    </p>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">
                No emerging competitors detected yet from current overlap signals.
              </p>
            )}
          </section>

          <section className="space-y-3">
            <h3 className="text-base font-semibold text-card-foreground">Tracked Competitors</h3>
          <div className="grid gap-4">
            {filteredCompetitors.length === 0 ? (
              <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                <CardContent className="py-12 text-center">
                  <Users className="mx-auto mb-4 h-12 w-12 text-slate-300" />
                  <h3 className="mb-2 text-lg font-semibold text-slate-100">No competitors tracked yet</h3>
                  <p className="mb-4 text-slate-400">Use the global + action to add competitors to your portfolio.</p>
                </CardContent>
              </Card>
            ) : (
              filteredCompetitors.map((competitor) => (
                <Card key={competitor.id} className="border border-white/[0.06] bg-white/[0.02] shadow-none transition-colors hover:bg-slate-900/60">
                  <CardContent className="p-5">
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div className="min-w-0 flex-1 space-y-2">
                        <h3 className="truncate text-lg font-semibold text-slate-100">{competitor.company_name}</h3>
                        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
                          {countryLabel(competitor.country) ? (
                            <span className="inline-flex items-center gap-1"><MapPin className="h-3.5 w-3.5" />{countryLabel(competitor.country)}</span>
                          ) : null}
                          {competitor.industry_sectors ? <span>{competitor.industry_sectors}</span> : null}
                        </div>
                        {competitor.notes ? <p className="text-sm text-slate-300">{competitor.notes}</p> : null}
                      </div>

                      <div className="flex items-center gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          className="border-white/[0.1]"
                          onClick={() => navigate(createPageUrl(`competitors/${competitor.id}`))}
                        >
                          Open Dossier
                        </Button>
                        <Button variant="ghost" size="icon" onClick={() => handleEdit(competitor)}><Edit2 className="h-4 w-4 text-slate-400" /></Button>
                        <Button variant="ghost" size="icon" onClick={() => handleDelete(competitor.id)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))
            )}
          </div>
          </section>
        </>
      ) : selectedCompetitor ? (
        <CompetitorDossier
          competitor={selectedCompetitor}
          competitors={competitors}
          analysis={analysisByCompetitorId[selectedCompetitor.id]}
          loading={analyzingId === selectedCompetitor.id || !analysisByCompetitorId[selectedCompetitor.id]}
          onBack={() => navigate(createPageUrl('competitors'))}
          onSwitch={(value) => navigate(createPageUrl(`competitors/${value}`))}
          onEdit={handleEdit}
          onDelete={handleDelete}
          researchCompetitor={researchCompetitor}
          agentBrief={agentBrief}
          agentLoading={agentLoading}
        />
      ) : (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardContent className="space-y-3 py-10 text-center">
            <p className="text-sm text-muted-foreground">Competitor not found in your tracked list.</p>
            <Button variant="outline" onClick={() => navigate(createPageUrl('competitors'))}>Back to Competitors</Button>
          </CardContent>
        </Card>
      )}

      <Dialog open={showForm} onOpenChange={setShowForm}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingCompetitor ? 'Edit Competitor' : 'Add Competitor'}</DialogTitle>
            <DialogDescription>Track a competitor to build an account-level dossier.</DialogDescription>
          </DialogHeader>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <Label htmlFor="company_name">Company Name *</Label>
              <SupplierAutocomplete
                value={formData.company_name}
                onChange={(value) => setFormData({ ...formData, company_name: value })}
                placeholder="Start typing to search suppliers..."
              />
            </div>
            <div>
              <Label htmlFor="country">Primary Country</Label>
              <Select value={formData.country} onValueChange={(value) => setFormData({ ...formData, country: value })}>
                <SelectTrigger><SelectValue placeholder="Select country" /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="IE">Ireland</SelectItem>
                  <SelectItem value="FR">France</SelectItem>
                  <SelectItem value="ES">Spain</SelectItem>
                  <SelectItem value="both">Multiple</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label htmlFor="industry_sectors">Industry Sectors</Label>
              <Input
                id="industry_sectors"
                value={formData.industry_sectors}
                onChange={(event) => setFormData({ ...formData, industry_sectors: event.target.value })}
                placeholder="e.g. IT Services, Construction"
              />
            </div>
            <div>
              <Label htmlFor="website">Website</Label>
              <Input
                id="website"
                value={formData.website}
                onChange={(event) => setFormData({ ...formData, website: event.target.value })}
                placeholder="e.g. https://www.bam.com"
              />
            </div>
            <div>
              <Label htmlFor="search_terms">Search Terms</Label>
              <Input
                id="search_terms"
                value={formData.search_terms}
                onChange={(event) => setFormData({ ...formData, search_terms: event.target.value })}
                placeholder="e.g. BAM, BAM FM, BAM PPP"
              />
              <p className="mt-1 text-[11px] text-muted-foreground">Comma-separated names to match across award data. Defaults to company name if empty.</p>
            </div>
            <div>
              <Label htmlFor="notes">Notes</Label>
              <Textarea
                id="notes"
                value={formData.notes}
                onChange={(event) => setFormData({ ...formData, notes: event.target.value })}
                placeholder="Any notes about this competitor..."
                rows={3}
              />
            </div>
            {saveError ? <p className="text-sm text-red-400">{saveError}</p> : null}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => { setShowForm(false); setEditingCompetitor(null); resetForm(); }}>Cancel</Button>
              <Button type="submit" className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90" disabled={saving}>
                {saving ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Saving...</> : editingCompetitor ? 'Update' : 'Add Competitor'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
