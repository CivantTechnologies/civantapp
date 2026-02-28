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
import HomePlatformFooter from '@/components/home/HomePlatformFooter';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip as RechartsTooltip,
  ResponsiveContainer, Cell
} from 'recharts';
import {
  Zap, FileText, Target, CheckCircle2, ArrowRight,
  Loader2, Calendar
} from 'lucide-react';
import { Page, PageBody, Card, CardContent, Button } from '@/components/ui';
import { formatDistanceToNow, format, parseISO } from 'date-fns';

const FLAG = { IE: '\u{1F1EE}\u{1F1EA}', FR: '\u{1F1EB}\u{1F1F7}', ES: '\u{1F1EA}\u{1F1F8}' };

/* ------------------------------------------------------------------ */
/*  Feed components                                                    */
/* ------------------------------------------------------------------ */
function FeedIcon({ type }) {
  if (type === 'hit_confirmed') return <CheckCircle2 className="h-3.5 w-3.5 text-civant-teal" />;
  if (type === 'window_opening') return <Target className="h-3.5 w-3.5 text-civant-teal/70" />;
  return <FileText className="h-3.5 w-3.5 text-slate-500" />;
}

function FeedCard({ item }) {
  const dateLabel = item.event_date
    ? formatDistanceToNow(new Date(item.event_date), { addSuffix: true })
    : '';
  const linkTo = item.event_type === 'tender_published' || item.event_type === 'hit_confirmed'
    ? createPageUrl(`TenderDetail?id=${item.ref_id}`)
    : createPageUrl('Forecast');

  const headline = item.event_type === 'hit_confirmed'
    ? `${item.buyer_name}`
    : item.event_type === 'window_opening'
      ? item.buyer_name
      : item.title
        ? (item.title.length > 80 ? item.title.slice(0, 77) + '...' : item.title)
        : item.buyer_name;

  const tag = item.event_type === 'hit_confirmed' ? 'Confirmed'
    : item.event_type === 'window_opening' ? 'Window Open'
    : 'Published';

  return (
    <Link to={linkTo} className="group flex items-start gap-3 px-4 py-3 transition-colors hover:bg-white/[0.025] border-b border-white/[0.03] last:border-0">
      <div className="mt-0.5 shrink-0"><FeedIcon type={item.event_type} /></div>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2 mb-0.5">
          <span className={`text-[9px] font-semibold uppercase tracking-wider px-1.5 py-px rounded ${
            item.event_type === 'hit_confirmed' ? 'bg-civant-teal/15 text-civant-teal' 
            : item.event_type === 'window_opening' ? 'bg-civant-teal/8 text-civant-teal/70'
            : 'bg-white/[0.04] text-slate-400'
          }`}>{tag}</span>
          <span className="text-[10px] text-muted-foreground">{dateLabel}</span>
        </div>
        <p className="text-[13px] font-medium text-card-foreground leading-snug truncate group-hover:text-white">{headline}</p>
        <div className="flex items-center gap-1.5 mt-0.5 text-[11px] text-muted-foreground">
          <span>{FLAG[item.country] || ''} {item.country}</span>
          {item.category && item.category !== 'Unknown' ? <><span className="opacity-30">/</span><span>{item.category}</span></> : null}
          {item.event_type === 'hit_confirmed' && item.delta_days != null ? <><span className="opacity-30">/</span><span className="text-civant-teal">{item.delta_days}d accuracy</span></> : null}
          {item.event_type === 'window_opening' && item.delta_days != null ? <><span className="opacity-30">/</span><span className="text-civant-teal/70">{item.delta_days}%</span></> : null}
        </div>
      </div>
    </Link>
  );
}

/* ------------------------------------------------------------------ */
/*  Custom Opportunity bar tooltip                                     */
/* ------------------------------------------------------------------ */
function OpportunityTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload;
  if (!d) return null;
  const fmtVal = (v) => {
    if (!v || v <= 0) return 'Unknown';
    if (v >= 1_000_000) return `\u20AC${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000) return `\u20AC${(v / 1_000).toFixed(0)}K`;
    return `\u20AC${v.toLocaleString()}`;
  };
  const incumbencyLabel = d.incumbency_pct >= 75 ? 'Strong' : d.incumbency_pct >= 40 ? 'Moderate' : 'Weak';
  const incumbencyColor = d.incumbency_pct >= 75 ? 'text-red-400' : d.incumbency_pct >= 40 ? 'text-amber-400' : 'text-emerald-400';
  return (
    <div className="rounded-lg bg-[#0d1b2a] border border-white/10 px-3 py-2.5 text-xs shadow-xl max-w-[300px] z-50">
      <p className="font-medium text-white mb-1.5">{FLAG[d.region] || ''} {d.buyer}</p>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-slate-300">
        <span>Est. value</span><span className="text-white font-medium text-right">{fmtVal(d.est_value_eur)}</span>
        <span>Confidence</span><span className="text-civant-teal font-medium text-right">{d.confidence}%</span>
        <span>Window</span><span className="text-right">{d.dateLabel}</span>
        <span>Incumbency</span><span className={`text-right font-medium ${incumbencyColor}`}>{incumbencyLabel} ({d.incumbency_pct}%)</span>
        {d.incumbent_name ? (
          <><span>Incumbent</span><span className="text-slate-200 text-right truncate max-w-[140px]">{d.incumbent_short}</span></>
        ) : null}
      </div>
      <p className="text-muted-foreground mt-1.5">{d.category} &middot; Score: {d.opportunity_score}/100</p>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */
export default function Home() {
  const [pulse, setPulse] = useState(null);
  const [feed, setFeed] = useState(null);
  const [pipeline, setPipeline] = useState(null);
  const [topOpps, setTopOpps] = useState(null);
  const [feedFilter, setFeedFilter] = useState('all');
  const [oppHorizon, setOppHorizon] = useState('6m');
  const [scopeActive, setScopeActive] = useState(false);
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
      // Load company profile for scope filtering
      let profile = null;
      try {
        const rows = await civant.entities.company_profiles.filter(
          { tenant_id: activeTenantId },
          '-updated_at',
          1,
          'target_cpv_clusters,target_countries,company_scope_filter_enabled'
        );
        profile = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
      } catch (e) {
        console.error('Failed to load company profile:', e);
      }
      setCompanyProfile(profile);

      const scopeEnabled = profile?.company_scope_filter_enabled !== false && !scopeFilterTemporarilyDisabled;
      const clusters = scopeEnabled && Array.isArray(profile?.target_cpv_clusters) && profile.target_cpv_clusters.length > 0
        ? profile.target_cpv_clusters : null;
      const countries = scopeEnabled && Array.isArray(profile?.target_countries) && profile.target_countries.length > 0
        ? profile.target_countries : null;
      setScopeActive(clusters !== null || countries !== null);

      const scopeParams = {};
      if (clusters) scopeParams.p_cpv_clusters = clusters;
      if (countries) scopeParams.p_countries = countries;

      const [pulseRes, feedRes, pipelineRes, oppsRes] = await Promise.allSettled([
        supabase.rpc('get_home_pulse', { p_tenant_id: activeTenantId }),
        supabase.rpc('get_home_feed', { p_tenant_id: activeTenantId, p_limit: 30 }),
        supabase.rpc('get_home_pipeline_snapshot', { p_tenant_id: activeTenantId }),
        supabase.rpc('get_home_top_opportunities', { p_tenant_id: activeTenantId, ...scopeParams, p_horizon: oppHorizon, p_limit: 10 }),
      ]);
      if (pulseRes.status === 'fulfilled' && !pulseRes.value.error) setPulse(pulseRes.value.data);
      if (feedRes.status === 'fulfilled' && !feedRes.value.error) setFeed(feedRes.value.data);
      if (pipelineRes.status === 'fulfilled' && !pipelineRes.value.error) setPipeline(pipelineRes.value.data);
      if (oppsRes.status === 'fulfilled' && !oppsRes.value.error) setTopOpps(oppsRes.value.data);
    } catch (e) {
      console.error('Home load error:', e);
    } finally {
      setLoading(false);
    }
  }, [activeTenantId, scopeFilterTemporarilyDisabled, oppHorizon]);

  useEffect(() => {
    if (!isLoadingTenants && activeTenantId) loadData();
  }, [activeTenantId, isLoadingTenants, loadData]);

  const barData = useMemo(() => {
    if (!Array.isArray(topOpps)) return [];
    return topOpps.map(o => {
      // Truncate long buyer names
      const shortBuyer = o.buyer.length > 28 ? o.buyer.slice(0, 26) + '...' : o.buyer;
      const incumbentShort = o.incumbent_name && o.incumbent_name.length > 30 ? o.incumbent_name.slice(0, 28) + '...' : o.incumbent_name;
      return {
        ...o,
        shortBuyer,
        incumbent_short: incumbentShort,
        dateLabel: format(parseISO(o.expected_date), 'MMM d'),
        barColor: o.incumbency_pct >= 75 ? 'hsl(0, 60%, 55%)' : o.incumbency_pct >= 40 ? 'hsl(38, 80%, 55%)' : 'hsl(174, 71%, 43%)',
      };
    });
  }, [topOpps]);

  const fmtValue = (v) => {
    if (!v || v <= 0) return '';
    if (v >= 1_000_000) return `\u20AC${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000) return `\u20AC${Math.round(v / 1_000)}K`;
    return `\u20AC${v}`;
  };

  const filteredFeed = useMemo(() => {
    const items = feed || [];
    if (feedFilter === 'all') return items;
    return items.filter(i => i.event_type === feedFilter);
  }, [feed, feedFilter]);

  if (loading || isLoadingTenants) {
    return (
      <Page><PageBody>
        <div className="flex items-center justify-center h-64">
          <Loader2 className="h-6 w-6 animate-spin text-civant-teal" />
        </div>
      </PageBody></Page>
    );
  }

  const p = pulse || {};
  const pi = pipeline || {};
  const tw = pi.this_week || {};
  const tm = pi.this_month || {};
  const nq = pi.next_quarter || {};
  const acc = pi.accuracy || {};

  return (
    <Page>
      <PageBody className="space-y-5">

        {/* ============================================================ */}
        {/*  PAGE HEADER                                                  */}
        {/* ============================================================ */}
        <div className="pb-6">
          <h1 className="text-4xl font-semibold tracking-tight text-card-foreground md:text-5xl">Panorama</h1>
          <p className="text-base text-muted-foreground md:text-lg mt-1">Your procurement intelligence at a glance</p>
        </div>

        {/* Scope filter strip */}
        {companyProfile && companyScopeFilteringActive ? (
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground -mt-3 mb-1">
            <span>Filtered by Company Scope</span>
            <Link to={createPageUrl('Company?tab=personalization')} className="text-civant-teal hover:underline">Edit scope</Link>
            <button type="button" onClick={clearScopeFilterTemporarily} className="text-civant-teal hover:underline">Clear temporarily</button>
          </div>
        ) : null}
        {companyProfile && persistedScopeFilterEnabled && scopeFilterTemporarilyDisabled ? (
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground -mt-3 mb-1">
            <span>Company scope filter temporarily cleared.</span>
            <button type="button" onClick={restoreScopeFilter} className="text-civant-teal hover:underline">Turn back on</button>
          </div>
        ) : null}

        {/* ============================================================ */}
        {/*  HERO: Opportunity Landscape + Stats Ticker                    */}
        {/* ============================================================ */}
        <div className="rounded-2xl border border-white/[0.06] bg-gradient-to-br from-white/[0.02] to-transparent overflow-hidden">
          {/* Stats grid - responsive, no overflow */}
          <div className="grid grid-cols-3 gap-px bg-white/[0.04] md:grid-cols-5">
            {[
              { val: (p.predictions_entering_window_7d || 0).toLocaleString(), label: 'Windows opening', accent: false },
              { val: (p.new_tenders_7d || 0).toLocaleString(), label: 'New tenders (7d)', accent: false },
              { val: `${acc.rate || 0}%`, label: 'Forecast accuracy', accent: true },
              { val: (p.hits_confirmed_30d || 0).toLocaleString(), label: 'Confirmed (30d)', accent: false },
              { val: (p.monitoring_total || 0).toLocaleString(), label: 'Monitoring', accent: false },
            ].map((s, i) => (
              <div key={i} className="bg-[hsl(223,47%,11%)] px-4 py-3 text-center">
                <p className={`text-lg font-semibold tabular-nums md:text-xl ${s.accent ? 'text-civant-teal' : 'text-card-foreground'}`}>{s.val}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">{s.label}</p>
              </div>
            ))}
          </div>

          {/* Top opportunities chart */}
          <div className="px-4 py-3">
            <div className="flex items-center justify-between mb-2">
              <p className="text-[10px] uppercase tracking-[0.08em] text-muted-foreground">
                Top Opportunities{companyScopeFilteringActive ? <span className="text-civant-teal/60 ml-1.5 normal-case tracking-normal">Filtered by your scope</span> : null}
              </p>
              <div className="flex items-center gap-1">
                {['1m', '6m', '1y'].map(h => (
                  <button
                    key={h}
                    type="button"
                    onClick={() => setOppHorizon(h)}
                    className={`px-2 py-0.5 rounded text-[10px] transition-colors ${oppHorizon === h ? 'bg-civant-teal/20 text-civant-teal' : 'text-muted-foreground hover:text-slate-300'}`}
                  >
                    {h === '1m' ? '1 Month' : h === '6m' ? '6 Months' : '1 Year'}
                  </button>
                ))}
              </div>
            </div>
            {barData.length > 0 ? (
              <ResponsiveContainer width="100%" height={barData.length * 38 + 10}>
                <BarChart data={barData} layout="vertical" margin={{ top: 0, right: 10, bottom: 0, left: 0 }} barCategoryGap="20%">
                  <XAxis type="number" hide domain={[0, 100]} />
                  <YAxis
                    dataKey="shortBuyer"
                    type="category"
                    tick={{ fill: 'hsl(220,10%,65%)', fontSize: 10 }}
                    axisLine={false}
                    tickLine={false}
                    width={160}
                  />
                  <RechartsTooltip content={<OpportunityTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                  <Bar
                    dataKey="opportunity_score"
                    radius={[0, 4, 4, 0]}
                    cursor="pointer"
                    onClick={(d) => {
                      if (d?.buyer) window.location.href = `/workbench/search?buyer=${encodeURIComponent(d.buyer)}`;
                    }}
                  >
                    {barData.map((entry, i) => (
                      <Cell key={i} fill={entry.barColor} fillOpacity={0.85} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-[200px] flex items-center justify-center text-sm text-muted-foreground">No opportunity data available</div>
            )}
            {/* Legend */}
            <div className="flex flex-wrap items-center gap-3 mt-1 text-[9px] text-muted-foreground">
              <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-civant-teal" />Weak incumbency</span>
              <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-amber-500" />Moderate</span>
              <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-red-500" />Strong incumbency</span>
              <span className="ml-auto">Bar = opportunity score</span>
            </div>
          </div>
        </div>

        {/* ============================================================ */}
        {/*  MAIN: Feed + Pipeline sidebar                               */}
        {/* ============================================================ */}
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-[1fr_340px] lg:items-stretch">

          {/* Left: Activity Feed */}
          <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] overflow-hidden">
            <div className="flex items-center justify-between px-4 py-3 border-b border-white/[0.04]">
              <div className="flex items-center gap-2">
                <Zap className="h-3.5 w-3.5 text-civant-teal" />
                <span className="text-sm font-semibold text-card-foreground">Activity</span>
              </div>
              {/* Filter tabs */}
              <div className="flex items-center gap-0.5 bg-white/[0.03] rounded-lg p-0.5">
                {[
                  { key: 'all', label: 'All' },
                  { key: 'hit_confirmed', label: 'Confirmed' },
                  { key: 'window_opening', label: 'Opening' },
                  { key: 'tender_published', label: 'Published' },
                ].map(f => (
                  <button key={f.key} onClick={() => setFeedFilter(f.key)}
                    className={`text-[10px] px-2.5 py-1 rounded-md transition-colors ${
                      feedFilter === f.key
                        ? 'bg-civant-teal/15 text-civant-teal font-medium'
                        : 'text-muted-foreground hover:text-slate-300'
                    }`}>
                    {f.label}
                  </button>
                ))}
              </div>
            </div>
            <div className="max-h-[520px] overflow-y-auto">
              {filteredFeed.length === 0 ? (
                <div className="px-4 py-10 text-sm text-muted-foreground text-center">
                  No activity matching this filter.
                </div>
              ) : (
                filteredFeed.slice(0, 20).map((item, i) => (
                  <FeedCard key={`${item.event_type}-${item.ref_id}-${i}`} item={item} />
                ))
              )}
            </div>
            {filteredFeed.length > 20 ? (
              <div className="border-t border-white/[0.04] px-4 py-2.5 text-center">
                <Link to="/forecast" className="text-[11px] text-civant-teal hover:underline">
                  View all &rarr;
                </Link>
              </div>
            ) : null}
          </div>

          {/* Right sidebar */}
          <div className="flex flex-col gap-4">

            {/* Accuracy + Timing */}
            <div className="rounded-xl border border-civant-teal/15 bg-civant-teal/[0.03] p-4">
              <div className="flex items-baseline justify-between mb-1">
                <span className="text-3xl font-bold text-civant-teal tabular-nums">{acc.rate || 0}%</span>
                <span className="text-lg font-semibold text-card-foreground tabular-nums">&plusmn;{pi.median_timing_days || 0}d</span>
              </div>
              <div className="flex items-baseline justify-between">
                <span className="text-[10px] text-civant-teal/70 uppercase tracking-wider">Accuracy</span>
                <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Median timing</span>
              </div>
              <p className="text-[10px] text-muted-foreground mt-2">
                {(acc.confirmed || 0).toLocaleString()} confirmed of {(acc.total_resolved || 0).toLocaleString()} resolved predictions
              </p>
            </div>

            {/* Pipeline */}
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.015] p-4 flex-1 flex flex-col">
              <div className="flex items-center gap-2 mb-1">
                <Calendar className="h-3.5 w-3.5 text-civant-teal" />
                <span className="text-xs font-semibold text-card-foreground">Pipeline</span>
              </div>

              <div className="flex-1 flex flex-col justify-between space-y-3">

              {/* Top buyers this week */}
              {tw.top_buyers?.length > 0 ? (
                <div className="pb-2 border-b border-white/[0.04]">
                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground mb-1.5">Top signals this week</p>
                  {tw.top_buyers.map((b, i) => (
                    <div key={i} className="flex items-center justify-between py-1 text-[11px]">
                      <span className="text-slate-300 truncate max-w-[190px]">{FLAG[b.country] || ''} {b.name}</span>
                      <span className="text-civant-teal/70 tabular-nums shrink-0 ml-2">{b.confidence}%</span>
                    </div>
                  ))}
                </div>
              ) : null}

              {/* Country breakdown */}
              {tm.by_country?.length > 0 ? (
                <div className="flex flex-wrap gap-2 pb-2 border-b border-white/[0.04]">
                  {tm.by_country.map((c, i) => (
                    <span key={i} className="text-[10px] text-muted-foreground">
                      {FLAG[c.country] || ''} <span className="text-slate-300 tabular-nums">{c.cnt.toLocaleString()}</span>
                    </span>
                  ))}
                </div>
              ) : null}

              {/* Timeline bars */}
              {[
                { label: 'Next 90 Days', count: nq.count || 0, accent: false },
                { label: 'This Month', count: tm.count || 0, accent: false },
                { label: 'This Week', count: tw.count || 0, accent: true },
              ].map((row, i) => {
                const maxCount = Math.max(tw.count || 0, tm.count || 0, nq.count || 0, 1);
                const pct = Math.max(4, (row.count / maxCount) * 100);
                return (
                  <div key={i}>
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-[10px] text-muted-foreground uppercase tracking-wider">{row.label}</span>
                      <span className={`text-sm font-semibold tabular-nums ${row.accent ? 'text-civant-teal' : 'text-card-foreground'}`}>
                        {row.count.toLocaleString()}
                      </span>
                    </div>
                    <div className="h-1.5 bg-white/[0.04] rounded-full overflow-hidden">
                      <div className={`h-full rounded-full transition-all duration-700 ${row.accent ? 'bg-civant-teal' : 'bg-white/[0.12]'}`}
                        style={{ width: `${pct}%` }} />
                    </div>
                  </div>
                );
              })}

              <div className="flex gap-2 pt-1 mt-auto">
                <Link to="/forecast" className="flex-1">
                  <Button variant="outline" size="sm" className="w-full text-[11px] border-civant-teal/20 text-civant-teal hover:bg-civant-teal/[0.06] gap-1.5">
                    Open Forecast <ArrowRight className="h-3 w-3" />
                  </Button>
                </Link>
                <Link to="/competitors">
                  <Button variant="outline" size="sm" className="text-[11px] border-white/[0.06] hover:border-civant-teal/20">
                    Competitors
                  </Button>
                </Link>
              </div>
              </div>
            </div>
          </div>
        </div>

        <HomePlatformFooter
          version={import.meta.env.VITE_APP_VERSION || 'v0.9.3'}
          lastDataRefresh={null}
          supportTo={createPageUrl('Company?section=support')}
          legalTo={createPageUrl('Company?section=legal')}
        />
      </PageBody>
    </Page>
  );
}
