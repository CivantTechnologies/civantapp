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
import {
  Loader2, ArrowRight, Users, Bell, Building2, Radio,
  TrendingUp, FileText, Clock, Award
} from 'lucide-react';
import { Page, PageBody } from '@/components/ui';
import { parseISO, formatDistanceToNow } from 'date-fns';

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

const windowLabel = (dateStr) => {
  const d = parseISO(dateStr);
  const q = Math.ceil((d.getMonth() + 1) / 3);
  return `Q${q} ${d.getFullYear()}`;
};

// Fit: green = high, amber = strong, red = moderate
const fitDot = (score) => score >= 70 ? 'bg-emerald-400' : score >= 55 ? 'bg-amber-400' : 'bg-red-400';
const fitTip = (score) => score >= 70 ? 'High fit' : score >= 55 ? 'Strong fit' : 'Moderate fit';

// Lock-in: green = low (opportunity), amber = medium, red = high (entrenched)
const lockDot = (pct) => pct >= 70 ? 'bg-red-400' : pct >= 35 ? 'bg-amber-400' : 'bg-emerald-400';
const lockTip = (pct) => pct >= 70 ? 'High lock-in' : pct >= 35 ? 'Medium lock-in' : 'Low lock-in';

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */
export default function Home() {
  const [data, setData] = useState(null);
  const [opsStatus, setOpsStatus] = useState(null);
  const [pipeline, setPipeline] = useState(null);
  const [feed, setFeed] = useState(null);
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

      const [oppsRes, opsRes, pipelineRes, feedRes] = await Promise.allSettled([
        supabase.rpc('get_home_top_opportunities', { p_tenant_id: activeTenantId, ...scopeParams, p_horizon: '6m', p_limit: 5 }),
        supabase.rpc('get_operational_status', { p_tenant_id: activeTenantId }),
        supabase.rpc('get_home_pipeline_snapshot', { p_tenant_id: activeTenantId }),
        supabase.rpc('get_home_feed', { p_tenant_id: activeTenantId, p_limit: 6 }),
      ]);
      if (oppsRes.status === 'fulfilled' && !oppsRes.value.error) setData(oppsRes.value.data);
      if (opsRes.status === 'fulfilled' && !opsRes.value.error) setOpsStatus(opsRes.value.data);
      if (pipelineRes.status === 'fulfilled' && !pipelineRes.value.error) setPipeline(pipelineRes.value.data);
      if (feedRes.status === 'fulfilled' && !feedRes.value.error) setFeed(feedRes.value.data);
    } catch (e) {
      console.error('Panorama load error:', e);
    } finally {
      setLoading(false);
    }
  }, [activeTenantId, scopeFilterTemporarilyDisabled]);

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
  const ops = opsStatus || {};
  const activeBids = ops.active_bids || {};
  const questionsOpen = ops.questions_open || {};
  const feedItems = (feed || []).slice(0, 4);

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
        {/*  OPERATIONAL STATUS                                          */}
        {/* ============================================================ */}
        <div>
          <h2 className="text-sm font-medium text-muted-foreground mb-3">Operational Status</h2>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            {/* Active Submissions */}
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] px-4 py-3.5">
              <div className="flex items-center gap-2 mb-2">
                <FileText className="h-4 w-4 text-muted-foreground" />
                <span className="text-xs text-muted-foreground">Active Submissions</span>
              </div>
              <p className="text-2xl font-semibold text-card-foreground tabular-nums leading-none">{activeBids.count || 0}</p>
              {activeBids.next_deadline_days != null && activeBids.next_deadline_days > 0 ? (
                <p className="text-[11px] text-amber-400 mt-1">{activeBids.next_deadline_days} days to next deadline</p>
              ) : activeBids.count > 0 ? (
                <p className="text-[11px] text-civant-teal mt-1">All on track</p>
              ) : (
                <p className="text-[11px] text-muted-foreground mt-1">No active bids</p>
              )}
            </div>

            {/* Questions Window */}
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] px-4 py-3.5">
              <div className="flex items-center gap-2 mb-2">
                <Clock className="h-4 w-4 text-muted-foreground" />
                <span className="text-xs text-muted-foreground">Questions Window Open</span>
              </div>
              <p className="text-2xl font-semibold text-card-foreground tabular-nums leading-none">{questionsOpen.count || 0}</p>
              {questionsOpen.next_close_days != null && questionsOpen.next_close_days > 0 ? (
                <p className="text-[11px] text-civant-teal mt-1">Closes in {questionsOpen.next_close_days} days</p>
              ) : (
                <p className="text-[11px] text-muted-foreground mt-1">No open windows</p>
              )}
            </div>

            {/* Awards Announced */}
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] px-4 py-3.5">
              <div className="flex items-center gap-2 mb-2">
                <Award className="h-4 w-4 text-muted-foreground" />
                <span className="text-xs text-muted-foreground">Awards Announced</span>
              </div>
              <p className="text-2xl font-semibold text-card-foreground tabular-nums leading-none">{(ops.awards_30d || 0).toLocaleString()}</p>
              <p className="text-[11px] text-civant-teal mt-1">Last 30 days</p>
            </div>

            {/* Forecast Accuracy */}
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] px-4 py-3.5">
              <div className="flex items-center gap-2 mb-2">
                <TrendingUp className="h-4 w-4 text-muted-foreground" />
                <span className="text-xs text-muted-foreground">Forecast Accuracy</span>
              </div>
              <p className="text-2xl font-semibold text-card-foreground tabular-nums leading-none">{acc.rate || 0}%</p>
              <p className="text-[11px] text-civant-teal mt-1">&plusmn;{pipeline?.median_timing_days || 0}d median timing</p>
            </div>
          </div>
        </div>

        {/* ============================================================ */}
        {/*  MAIN GRID                                                   */}
        {/* ============================================================ */}
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-[1fr_300px]">

          {/* LEFT: RUNWAY OPPORTUNITIES */}
          <section className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-5">
            <div className="flex items-start justify-between mb-1">
              <div>
                <h2 className="text-lg font-semibold text-card-foreground">Runway Opportunities</h2>
                <p className="text-sm text-muted-foreground mt-0.5">Top match renewal windows</p>
              </div>
              <Link
                to="/forecast"
                className="flex items-center gap-1.5 text-xs text-muted-foreground border border-white/[0.08] rounded-lg px-3 py-1.5 hover:border-civant-teal/30 hover:text-civant-teal transition-colors shrink-0"
              >
                View All Opportunities <ArrowRight className="h-3 w-3" />
              </Link>
            </div>

            {opps.length > 0 ? (
              <div className="mt-4">
                {/* Header */}
                <div className="grid grid-cols-[1fr_80px_80px_32px_32px_88px] gap-3 pb-2 border-b border-white/[0.06] text-[10px] uppercase tracking-wider text-muted-foreground">
                  <span>Buyer</span>
                  <span>Window</span>
                  <span>Value</span>
                  <span className="text-center" title="Fit level">Fit</span>
                  <span className="text-center" title="Incumbent lock-in">Lock</span>
                  <span>Action</span>
                </div>

                {/* Rows */}
                {opps.map((opp, i) => (
                  <div
                    key={opp.prediction_id || i}
                    className="grid grid-cols-[1fr_80px_80px_32px_32px_88px] gap-3 py-3 items-center border-b border-white/[0.04] last:border-0 group"
                  >
                    <div className="min-w-0 flex items-center gap-2.5">
                      <span className="text-sm shrink-0">{FLAG[opp.region] || ''}</span>
                      <span className="text-[13px] text-card-foreground truncate">{opp.buyer}</span>
                    </div>
                    <span className="text-[13px] text-muted-foreground tabular-nums">{windowLabel(opp.expected_date)}</span>
                    <span className="text-[13px] font-semibold text-card-foreground tabular-nums">{fmtValue(opp.est_value_eur)}</span>
                    <div className="flex justify-center" title={fitTip(opp.opportunity_score)}>
                      <span className={`inline-block w-2.5 h-2.5 rounded-full ${fitDot(opp.opportunity_score)}`} />
                    </div>
                    <div className="flex justify-center" title={lockTip(opp.incumbency_pct)}>
                      <span className={`inline-block w-2.5 h-2.5 rounded-full ${lockDot(opp.incumbency_pct)}`} />
                    </div>
                    <Link
                      to={`/workbench/search?buyer=${encodeURIComponent(opp.buyer)}`}
                      className="text-[11px] font-medium text-civant-teal border border-civant-teal/25 rounded-md px-2.5 py-1 text-center hover:bg-civant-teal/10 transition-colors"
                    >
                      View Details
                    </Link>
                  </div>
                ))}

                {/* Legend */}
                <div className="flex items-center gap-4 mt-3 text-[9px] text-muted-foreground">
                  <span className="flex items-center gap-1"><span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-400" /> High</span>
                  <span className="flex items-center gap-1"><span className="inline-block w-1.5 h-1.5 rounded-full bg-amber-400" /> Medium</span>
                  <span className="flex items-center gap-1"><span className="inline-block w-1.5 h-1.5 rounded-full bg-red-400" /> Low / Risk</span>
                </div>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground py-8 text-center mt-4">No high-fit opportunities in this timeframe.</p>
            )}
          </section>

          {/* RIGHT COLUMN */}
          <div className="space-y-5">

            {/* TRACKING SNAPSHOT */}
            <section className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-5">
              <h2 className="text-base font-semibold text-card-foreground mb-4">Tracking Snapshot</h2>
              <div className="space-y-3.5">
                {[
                  { icon: <Users className="h-3.5 w-3.5 text-muted-foreground" />, label: 'Competitors Tracked', value: 5 },
                  { icon: <Bell className="h-3.5 w-3.5 text-muted-foreground" />, label: 'Opportunity Alerts', value: exposure.total_opportunities || 0, dot: 'bg-red-400' },
                  { icon: <Building2 className="h-3.5 w-3.5 text-muted-foreground" />, label: 'Institutions Monitored', value: exposure.unique_incumbents || 0 },
                  { icon: <Radio className="h-3.5 w-3.5 text-muted-foreground" />, label: 'Signals (7d)', value: activeBids.count || 0, dot: 'bg-civant-teal' },
                ].map((row, i) => (
                  <div key={i} className="flex items-center justify-between">
                    <div className="flex items-center gap-2.5">
                      {row.icon}
                      <span className="text-[13px] text-slate-300">{row.label}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      {row.dot ? <span className={`inline-block w-1.5 h-1.5 rounded-full ${row.dot}`} /> : null}
                      <span className="text-base font-semibold text-card-foreground tabular-nums">{row.value}</span>
                    </div>
                  </div>
                ))}
              </div>
            </section>

            {/* LATEST STRATEGIC CHANGES */}
            <section className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-5">
              <h2 className="text-base font-semibold text-card-foreground mb-4">Latest Strategic Changes</h2>
              <div className="space-y-4">
                {feedItems.length > 0 ? feedItems.map((item, i) => {
                  const isConfirmed = item.event_type === 'hit_confirmed';
                  const isOpening = item.event_type === 'window_opening';
                  const dotColor = isConfirmed ? 'bg-emerald-400' : isOpening ? 'bg-civant-teal' : 'bg-amber-400';
                  const label = isConfirmed ? 'Signal Confirmed' : isOpening ? 'Window Opening' : 'Tender Published';
                  return (
                    <div key={`${item.event_type}-${item.ref_id}-${i}`} className="flex items-start gap-2.5">
                      <span className={`inline-block w-2 h-2 rounded-full ${dotColor} mt-1 shrink-0`} />
                      <div className="min-w-0">
                        <p className="text-[13px] font-medium text-card-foreground leading-snug">{label}</p>
                        <p className="text-[11px] text-muted-foreground truncate">
                          {FLAG[item.country] || ''} {item.buyer_name || item.title || '-'}
                        </p>
                      </div>
                    </div>
                  );
                }) : (
                  <p className="text-[13px] text-muted-foreground">No recent changes.</p>
                )}
              </div>
            </section>
          </div>
        </div>

        {/* ============================================================ */}
        {/*  FOOTER                                                      */}
        {/* ============================================================ */}
        <footer className="border-t border-white/[0.04] pt-4 flex flex-wrap items-center justify-between text-[11px] text-muted-foreground">
          <div className="flex items-center gap-4">
            <span>Accuracy <span className="text-slate-300 font-medium tabular-nums">{acc.rate || 0}%</span></span>
            <span>Timing <span className="text-civant-teal font-medium tabular-nums">&plusmn;{pipeline?.median_timing_days || 0}d</span></span>
            <span>Data: <span className="text-slate-400">live</span></span>
          </div>
          <div className="flex items-center gap-4">
            <Link to={createPageUrl('Company?section=support')} className="hover:text-slate-300">Support</Link>
            <Link to={createPageUrl('Company?section=legal')} className="text-civant-teal hover:underline">Privacy / Legal</Link>
          </div>
        </footer>

      </PageBody>
    </Page>
  );
}
