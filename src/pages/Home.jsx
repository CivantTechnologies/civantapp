import React, { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useTenant } from '@/lib/tenant';
import { supabase } from '@/lib/supabaseClient';
import { civant } from '@/api/civantClient';
import {
  isCompanyScopeFilterTemporarilyDisabled,
  setCompanyScopeFilterTemporarilyDisabled
} from '@/lib/companyScopeSession';
import { Loader2 } from 'lucide-react';
import { Page, PageBody } from '@/components/ui';
import { parseISO } from 'date-fns';

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
const fitColor = (score) => score >= 70
  ? 'bg-civant-teal/15 text-civant-teal border-civant-teal/20'
  : score >= 55
    ? 'bg-civant-teal/10 text-civant-teal/80 border-civant-teal/15'
    : 'bg-white/[0.04] text-muted-foreground border-white/[0.06]';

const windowLabel = (dateStr) => {
  const d = parseISO(dateStr);
  const q = Math.ceil((d.getMonth() + 1) / 3);
  return `Q${q} ${d.getFullYear()}`;
};

/* ------------------------------------------------------------------ */
/*  Static tips (rotate weekly via date seed)                          */
/* ------------------------------------------------------------------ */
const ALL_TIPS = [
  'Engage buyers at least 6 months before renewal windows open.',
  'Framework incumbents retain contracts 68% of the time. Build early differentiation.',
  'Budget increases in Facilities & Maintenance cluster suggest Q2 expansion.',
  'Low lock-in windows are your highest-leverage pursuit targets.',
  'Multi-lot frameworks often split across 3+ suppliers. Position for Lot 2.',
  'Irish public bodies publish PIN notices 40 days before formal tenders on average.',
  'French BOAMP notices give a 52-day average lead time from publication to deadline.',
  'Track repeat buyers. 73% of procurement officers re-tender with the same scope.',
  'Spanish PLACSP tenders above \u20AC140K require 30+ day submission windows.',
];

function getTips() {
  const weekSeed = Math.floor(Date.now() / (7 * 86400000));
  const start = (weekSeed * 3) % ALL_TIPS.length;
  const tips = [];
  for (let i = 0; i < 3; i++) tips.push(ALL_TIPS[(start + i) % ALL_TIPS.length]);
  return tips;
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */
export default function Home() {
  const [data, setData] = useState(null);
  const [pulse, setPulse] = useState(null);
  const [pipeline, setPipeline] = useState(null);
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
        supabase.rpc('get_home_top_opportunities', { p_tenant_id: activeTenantId, ...scopeParams, p_horizon: '6m', p_limit: 5 }),
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
  }, [activeTenantId, scopeFilterTemporarilyDisabled]);

  useEffect(() => {
    if (!isLoadingTenants && activeTenantId) loadData();
  }, [activeTenantId, isLoadingTenants, loadData]);

  const tips = getTips();

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

  return (
    <Page>
      <PageBody className="space-y-6 pb-12">

        {/* ============================================================ */}
        {/*  HEADER                                                      */}
        {/* ============================================================ */}
        <div>
          <h1 className="text-4xl font-semibold tracking-tight text-card-foreground">Panorama</h1>
          <p className="text-base text-muted-foreground mt-1">Your strategic runway, at a glance</p>

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
        {/*  MAIN GRID: Left (Runway + Exposure) | Right (Tips + Trust)  */}
        {/* ============================================================ */}
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-[1fr_320px]">

          {/* ---------------------------------------------------------- */}
          {/*  LEFT COLUMN                                                */}
          {/* ---------------------------------------------------------- */}
          <div className="space-y-5">

            {/* RUNWAY OPPORTUNITIES */}
            <section className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-5">
              <h2 className="text-lg font-semibold text-card-foreground">Runway Opportunities</h2>
              <p className="text-sm text-muted-foreground mt-0.5 mb-5">Best-fit renewal windows within your tracked scope.</p>

              {opps.length > 0 ? (
                <div>
                  {/* Table header */}
                  <div className="grid grid-cols-[1fr_88px_88px_96px_64px_80px] gap-3 pb-2 border-b border-white/[0.06] text-[10px] uppercase tracking-wider text-muted-foreground">
                    <span>Buyer</span>
                    <span>Window</span>
                    <span>Est. Value</span>
                    <span>Win Probability</span>
                    <span>Fit</span>
                    <span />
                  </div>

                  {/* Table rows */}
                  {opps.map((opp, i) => (
                    <div
                      key={opp.prediction_id || i}
                      className="grid grid-cols-[1fr_88px_88px_96px_64px_80px] gap-3 py-3.5 items-center border-b border-white/[0.04] last:border-0 group"
                    >
                      {/* Buyer */}
                      <div className="min-w-0 flex items-center gap-2">
                        <span className="text-sm shrink-0">{FLAG[opp.region] || ''}</span>
                        <span className="text-[13px] text-card-foreground truncate font-medium">{opp.buyer}</span>
                      </div>

                      {/* Window */}
                      <span className="text-[13px] text-muted-foreground tabular-nums">{windowLabel(opp.expected_date)}</span>

                      {/* Est. Value */}
                      <span className="text-[13px] font-semibold text-card-foreground tabular-nums">{fmtValue(opp.est_value_eur)}</span>

                      {/* Win Probability */}
                      <span className="text-[13px] text-card-foreground tabular-nums">{opp.confidence}%</span>

                      {/* Fit */}
                      <span className={`text-[10px] font-medium px-2 py-0.5 rounded border inline-flex items-center justify-center ${fitColor(opp.opportunity_score)}`}>
                        {fitLabel(opp.opportunity_score)}
                      </span>

                      {/* CTA */}
                      <Link
                        to={`/workbench/search?buyer=${encodeURIComponent(opp.buyer)}`}
                        className="text-[11px] text-muted-foreground border border-white/[0.08] rounded px-2.5 py-1 text-center hover:border-civant-teal/30 hover:text-civant-teal transition-colors"
                      >
                        Prepare Now
                      </Link>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-muted-foreground py-6 text-center">No high-fit opportunities in this timeframe.</p>
              )}
            </section>

            {/* COMPETITIVE EXPOSURE */}
            <section className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-5">
              <h2 className="text-lg font-semibold text-card-foreground mb-3">Competitive Exposure</h2>
              <div className="space-y-2.5">
                {exposure.total_value_eur > 0 ? (
                  <div className="flex items-start gap-2.5">
                    <span className="inline-block w-1.5 h-1.5 rounded-full bg-muted-foreground/40 mt-1.5 shrink-0" />
                    <p className="text-[13px] text-slate-300 leading-relaxed">
                      <span className="font-semibold text-card-foreground">{fmtValue(exposure.total_value_eur)}</span> competitor exposure in the next 12 months.
                    </p>
                  </div>
                ) : null}
                {exposure.low_lockin_count > 0 ? (
                  <div className="flex items-start gap-2.5">
                    <span className="inline-block w-1.5 h-1.5 rounded-full bg-muted-foreground/40 mt-1.5 shrink-0" />
                    <p className="text-[13px] text-slate-300 leading-relaxed">
                      {exposure.low_lockin_count} renewal windows with low incumbent lock-in.
                    </p>
                  </div>
                ) : null}
                {exposure.unique_incumbents > 3 ? (
                  <div className="flex items-start gap-2.5">
                    <span className="inline-block w-1.5 h-1.5 rounded-full bg-muted-foreground/40 mt-1.5 shrink-0" />
                    <p className="text-[13px] text-slate-300 leading-relaxed">
                      Concentration risk emerging at {exposure.unique_incumbents} institutions.
                    </p>
                  </div>
                ) : null}
              </div>
            </section>
          </div>

          {/* ---------------------------------------------------------- */}
          {/*  RIGHT COLUMN                                               */}
          {/* ---------------------------------------------------------- */}
          <div className="space-y-5">

            {/* TODAY'S TENDER TIPS */}
            <section className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-5">
              <h2 className="text-lg font-semibold text-card-foreground mb-4">Today's Tender Tips</h2>
              <div className="space-y-4">
                {tips.map((tip, i) => (
                  <div key={i} className="flex items-start gap-2.5">
                    <span className="inline-block w-2 h-2 rounded-full bg-civant-teal mt-1 shrink-0" />
                    <p className="text-[13px] text-slate-300 leading-relaxed">{tip}</p>
                  </div>
                ))}
              </div>
            </section>

            {/* TRUST */}
            <section className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-5">
              <h2 className="text-lg font-semibold text-card-foreground mb-3">Trust</h2>
              <div className="space-y-2.5">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-civant-teal text-sm">&#9670;</span>
                    <span className="text-[13px] text-slate-300">Accuracy</span>
                  </div>
                  <span className="text-lg font-semibold text-card-foreground tabular-nums">{acc.rate || 0}%</span>
                </div>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-civant-teal text-sm">&#9670;</span>
                    <span className="text-[13px] text-slate-300">Median timing</span>
                  </div>
                  <span className="text-lg font-semibold text-civant-teal tabular-nums">&plusmn;{pipeline?.median_timing_days || 0}d</span>
                </div>
              </div>
            </section>
          </div>
        </div>

        {/* ============================================================ */}
        {/*  FOOTER                                                      */}
        {/* ============================================================ */}
        <footer className="flex items-center justify-between text-[11px] text-muted-foreground pt-2">
          <span>Last data refresh: <span className="text-slate-400">live</span></span>
          <div className="flex items-center gap-4">
            <Link to={createPageUrl('Company?section=support')} className="hover:text-slate-300">Support</Link>
            <Link to={createPageUrl('Company?section=legal')} className="text-civant-teal hover:underline">Privacy / Legal</Link>
          </div>
        </footer>

      </PageBody>
    </Page>
  );
}
