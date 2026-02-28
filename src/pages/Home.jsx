import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useTenant } from '@/lib/tenant';
import { supabase } from '@/lib/supabaseClient';
import { civant } from '@/api/civantClient';
import {
  isCompanyScopeFilterTemporarilyDisabled,
  setCompanyScopeFilterTemporarilyDisabled
} from '@/lib/companyScopeSession';
import { ArrowRight, Loader2 } from 'lucide-react';
import { Page, PageBody } from '@/components/ui';
import { format, parseISO } from 'date-fns';

const FLAG = { IE: '\u{1F1EE}\u{1F1EA}', FR: '\u{1F1EB}\u{1F1F7}', ES: '\u{1F1EA}\u{1F1F8}' };

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */
const fmtValue = (v) => {
  if (!v || v <= 0) return '-';
  if (v >= 1_000_000) return `\u20AC${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `\u20AC${Math.round(v / 1_000)}K`;
  return `\u20AC${v.toLocaleString()}`;
};

const fitLabel = (score) => score >= 70 ? 'High' : score >= 55 ? 'Strong' : 'Moderate';
const fitStyle = (score) => score >= 70 ? 'text-civant-teal' : score >= 55 ? 'text-slate-200' : 'text-muted-foreground';
const lockLabel = (pct) => pct >= 70 ? 'High' : pct >= 35 ? 'Medium' : 'Low';
const lockStyle = (pct) => pct >= 70 ? 'text-red-400' : pct >= 35 ? 'text-amber-400' : 'text-emerald-400';

const windowLabel = (dateStr) => {
  const d = parseISO(dateStr);
  const q = Math.ceil((d.getMonth() + 1) / 3);
  return `Q${q} ${d.getFullYear()}`;
};

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */
export default function Home() {
  const [data, setData] = useState(null);
  const [pulse, setPulse] = useState(null);
  const [pipeline, setPipeline] = useState(null);
  const [oppHorizon, setOppHorizon] = useState('6m');
  const [companyProfile, setCompanyProfile] = useState(null);
  const [scopeFilterTemporarilyDisabled, setScopeFilterTemporarilyDisabledState] = useState(false);
  const [loading, setLoading] = useState(true);
  const { activeTenantId, isLoadingTenants } = useTenant();

  useEffect(() => {
    setScopeFilterTemporarilyDisabledState(isCompanyScopeFilterTemporarilyDisabled(activeTenantId));
  }, [activeTenantId]);

  const persistedScopeFilterEnabled = companyProfile?.company_scope_filter_enabled !== false;
  const companyScopeFilteringActive = persistedScopeFilterEnabled && !scopeFilterTemporarilyDisabled;

  const clearScopeFilterTemporarily = useCallback(() => {
    setCompanyScopeFilterTemporarilyDisabled(activeTenantId, true);
    setScopeFilterTemporarilyDisabledState(true);
  }, [activeTenantId]);

  const restoreScopeFilter = useCallback(() => {
    setCompanyScopeFilterTemporarilyDisabled(activeTenantId, false);
    setScopeFilterTemporarilyDisabledState(false);
  }, [activeTenantId]);

  const loadData = useCallback(async () => {
    if (!activeTenantId) return;
    setLoading(true);
    try {
      let profile = null;
      try {
        const rows = await civant.entities.company_profiles.filter(
          { tenant_id: activeTenantId },
          '-updated_at', 1,
          'target_cpv_clusters,target_countries,company_scope_filter_enabled'
        );
        profile = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
      } catch (e) { console.error('Profile load error:', e); }
      setCompanyProfile(profile);

      const scopeEnabled = profile?.company_scope_filter_enabled !== false && !scopeFilterTemporarilyDisabled;
      const clusters = scopeEnabled && Array.isArray(profile?.target_cpv_clusters) && profile.target_cpv_clusters.length > 0
        ? profile.target_cpv_clusters : null;
      const countries = scopeEnabled && Array.isArray(profile?.target_countries) && profile.target_countries.length > 0
        ? profile.target_countries : null;

      const scopeParams = {};
      if (clusters) scopeParams.p_cpv_clusters = clusters;
      if (countries) scopeParams.p_countries = countries;

      const [oppsRes, pulseRes, pipelineRes] = await Promise.allSettled([
        supabase.rpc('get_home_top_opportunities', { p_tenant_id: activeTenantId, ...scopeParams, p_horizon: oppHorizon, p_limit: 5 }),
        supabase.rpc('get_home_pulse', { p_tenant_id: activeTenantId }),
        supabase.rpc('get_home_pipeline_snapshot', { p_tenant_id: activeTenantId }),
      ]);
      if (oppsRes.status === 'fulfilled' && !oppsRes.value.error) setData(oppsRes.value.data);
      if (pulseRes.status === 'fulfilled' && !pulseRes.value.error) setPulse(pulseRes.value.data);
      if (pipelineRes.status === 'fulfilled' && !pipelineRes.value.error) setPipeline(pipelineRes.value.data);
    } catch (e) {
      console.error('Panorama load error:', e);
    } finally {
      setLoading(false);
    }
  }, [activeTenantId, scopeFilterTemporarilyDisabled, oppHorizon]);

  useEffect(() => {
    if (!isLoadingTenants && activeTenantId) loadData();
  }, [activeTenantId, isLoadingTenants, loadData]);

  if (loading || isLoadingTenants) {
    return (
      <Page><PageBody>
        <div className="flex items-center justify-center h-64">
          <Loader2 className="h-5 w-5 animate-spin text-civant-teal/50" />
        </div>
      </PageBody></Page>
    );
  }

  const opps = data?.opportunities || [];
  const exposure = data?.exposure || {};
  const acc = pipeline?.accuracy || {};
  const p = pulse || {};

  return (
    <Page>
      <PageBody className="max-w-4xl mx-auto space-y-10 pb-16">

        {/* ============================================================ */}
        {/*  HEADER                                                      */}
        {/* ============================================================ */}
        <div className="pt-2">
          <h1 className="text-3xl font-semibold tracking-tight text-card-foreground">Panorama</h1>
          <p className="text-sm text-muted-foreground mt-1">Strategic procurement intelligence for your scope.</p>

          {companyProfile && companyScopeFilteringActive ? (
            <div className="flex items-center gap-3 mt-3 text-xs text-muted-foreground">
              <span className="inline-block w-1.5 h-1.5 rounded-full bg-civant-teal" />
              <span>Scope active</span>
              <Link to={createPageUrl('Company?tab=personalization')} className="text-civant-teal hover:underline">Edit</Link>
              <button type="button" onClick={clearScopeFilterTemporarily} className="text-civant-teal hover:underline">Clear</button>
            </div>
          ) : null}
          {companyProfile && persistedScopeFilterEnabled && scopeFilterTemporarilyDisabled ? (
            <div className="flex items-center gap-3 mt-3 text-xs text-muted-foreground">
              <span className="inline-block w-1.5 h-1.5 rounded-full bg-amber-500" />
              <span>Scope paused</span>
              <button type="button" onClick={restoreScopeFilter} className="text-civant-teal hover:underline">Resume</button>
            </div>
          ) : null}
        </div>

        {/* ============================================================ */}
        {/*  1. RUNWAY OPPORTUNITIES                                     */}
        {/* ============================================================ */}
        <section>
          <div className="flex items-center justify-between mb-1">
            <h2 className="text-sm font-semibold tracking-wide text-card-foreground">Runway Opportunities</h2>
            <div className="flex items-center gap-0.5">
              {['1m', '6m', '1y'].map(h => (
                <button
                  key={h} type="button"
                  onClick={() => setOppHorizon(h)}
                  className={`px-2.5 py-1 rounded text-[10px] font-medium transition-colors ${
                    oppHorizon === h ? 'bg-civant-teal/12 text-civant-teal' : 'text-muted-foreground hover:text-slate-300'
                  }`}
                >
                  {h === '1m' ? '1M' : h === '6m' ? '6M' : '1Y'}
                </button>
              ))}
            </div>
          </div>
          <p className="text-xs text-muted-foreground mb-4">Best-fit renewal windows within your tracked scope.</p>

          {opps.length > 0 ? (
            <div className="divide-y divide-white/[0.04]">
              {/* Header row */}
              <div className="hidden md:grid grid-cols-[1fr_72px_88px_52px_52px_52px_72px] gap-2 pb-2 text-[9px] uppercase tracking-wider text-muted-foreground">
                <span>Buyer</span>
                <span className="text-right">Window</span>
                <span className="text-right">Est. Value</span>
                <span className="text-right">Win %</span>
                <span className="text-right">Fit</span>
                <span className="text-right">Lock-in</span>
                <span />
              </div>

              {opps.map((opp, i) => (
                <div
                  key={opp.prediction_id || i}
                  className="md:grid md:grid-cols-[1fr_72px_88px_52px_52px_52px_72px] gap-2 py-3 items-center group"
                >
                  <div className="min-w-0">
                    <p className="text-[13px] text-card-foreground truncate leading-snug">
                      {FLAG[opp.region] || ''} {opp.buyer}
                    </p>
                    <p className="text-[10px] text-muted-foreground truncate">{opp.category}</p>
                  </div>

                  <span className="text-xs text-muted-foreground md:text-right tabular-nums">{windowLabel(opp.expected_date)}</span>
                  <span className="text-xs font-semibold text-card-foreground md:text-right tabular-nums">{fmtValue(opp.est_value_eur)}</span>
                  <span className="text-xs text-civant-teal md:text-right tabular-nums">{opp.confidence}%</span>
                  <span className={`text-[11px] md:text-right ${fitStyle(opp.opportunity_score)}`}>{fitLabel(opp.opportunity_score)}</span>
                  <span className={`text-[11px] md:text-right ${lockStyle(opp.incumbency_pct)}`}>{lockLabel(opp.incumbency_pct)}</span>

                  <Link
                    to={`/workbench/search?buyer=${encodeURIComponent(opp.buyer)}`}
                    className="text-[10px] text-civant-teal hover:underline md:text-right opacity-50 group-hover:opacity-100 transition-opacity"
                  >
                    Prepare Now &rarr;
                  </Link>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground py-8 text-center">No high-fit opportunities in this timeframe.</p>
          )}
        </section>

        {/* ============================================================ */}
        {/*  2. COMPETITIVE EXPOSURE                                     */}
        {/* ============================================================ */}
        <section>
          <h2 className="text-sm font-semibold tracking-wide text-card-foreground mb-3">Competitive Exposure</h2>
          <div className="space-y-2 text-[13px] text-slate-300 leading-relaxed">
            {exposure.total_value_eur > 0 ? (
              <p>{fmtValue(exposure.total_value_eur)} in estimated contract value across {exposure.total_opportunities || 0} renewal windows in the next 12 months.</p>
            ) : null}
            {exposure.low_lockin_count > 0 ? (
              <p>{exposure.low_lockin_count} windows with low incumbent lock-in identified within your scope.</p>
            ) : null}
            {exposure.unique_incumbents > 10 ? (
              <p>{exposure.unique_incumbents} distinct incumbents across your tracked categories. Concentration risk remains distributed.</p>
            ) : null}
          </div>
        </section>

        {/* ============================================================ */}
        {/*  3. INTELLIGENCE PULSE                                       */}
        {/* ============================================================ */}
        <section>
          <h2 className="text-sm font-semibold tracking-wide text-card-foreground mb-3">Intelligence Pulse</h2>
          <div className="space-y-2 text-[13px] text-slate-300 leading-relaxed">
            {(p.predictions_entering_window_7d || 0) > 0 ? (
              <p><span className="text-civant-teal font-medium">{(p.predictions_entering_window_7d).toLocaleString()}</span> renewal windows opened this week across your tracked markets.</p>
            ) : null}
            {(p.new_tenders_7d || 0) > 0 ? (
              <p><span className="text-card-foreground font-medium">{(p.new_tenders_7d).toLocaleString()}</span> new tenders published in the last 7 days.</p>
            ) : null}
            {(p.hits_confirmed_30d || 0) > 0 ? (
              <p><span className="text-civant-teal font-medium">{(p.hits_confirmed_30d).toLocaleString()}</span> forecasts confirmed in the last 30 days.</p>
            ) : null}
            {(p.monitoring_total || 0) > 0 ? (
              <p>{(p.monitoring_total).toLocaleString()} active signals currently under monitoring.</p>
            ) : null}
          </div>
        </section>

        {/* ============================================================ */}
        {/*  4. NAVIGATION                                               */}
        {/* ============================================================ */}
        <section className="flex flex-wrap gap-4 text-xs">
          <Link to="/forecast" className="flex items-center gap-1.5 text-civant-teal hover:underline">Open Forecast <ArrowRight className="h-3 w-3" /></Link>
          <Link to="/workbench/search" className="flex items-center gap-1.5 text-civant-teal hover:underline">Finder <ArrowRight className="h-3 w-3" /></Link>
          <Link to="/competitors" className="flex items-center gap-1.5 text-civant-teal hover:underline">Competitors <ArrowRight className="h-3 w-3" /></Link>
          <Link to="/reports" className="flex items-center gap-1.5 text-civant-teal hover:underline">Reports <ArrowRight className="h-3 w-3" /></Link>
        </section>

        {/* ============================================================ */}
        {/*  5. TRUST FOOTER                                             */}
        {/* ============================================================ */}
        <footer className="border-t border-white/[0.04] pt-4 flex flex-wrap gap-x-6 gap-y-1 text-[10px] text-muted-foreground">
          <span>Accuracy: <span className="text-slate-300 tabular-nums">{acc.rate || 0}%</span></span>
          <span>Confirmed: <span className="text-slate-300 tabular-nums">{(acc.confirmed || 0).toLocaleString()}</span> of <span className="tabular-nums">{(acc.total_resolved || 0).toLocaleString()}</span></span>
          {pipeline?.median_timing_days ? <span>Median timing: <span className="text-slate-300 tabular-nums">&plusmn;{pipeline.median_timing_days}d</span></span> : null}
          <span>Data: <span className="text-slate-300">Live</span></span>
        </footer>

      </PageBody>
    </Page>
  );
}
