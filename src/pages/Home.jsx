import React, { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useTenant } from '@/lib/tenant';
import { supabase } from '@/lib/supabaseClient';
import HomePlatformFooter from '@/components/home/HomePlatformFooter';
import {
  Zap,
  FileText,
  Target,
  CheckCircle2,
  Eye,
  ArrowRight,
  Loader2,
  TrendingUp,
  Calendar,
  Activity,
} from 'lucide-react';
import { Page, PageBody, Card, CardContent, CardHeader, CardTitle, Button } from '@/components/ui';
import { formatDistanceToNow } from 'date-fns';

const FLAG = { IE: '\u{1F1EE}\u{1F1EA}', FR: '\u{1F1EB}\u{1F1F7}', ES: '\u{1F1EA}\u{1F1F8}' };

function FeedIcon({ type }) {
  if (type === 'hit_confirmed') return <CheckCircle2 className="h-4 w-4 text-emerald-400" />;
  if (type === 'window_opening') return <Target className="h-4 w-4 text-cyan-400" />;
  return <FileText className="h-4 w-4 text-slate-400" />;
}

function FeedLabel({ type }) {
  if (type === 'hit_confirmed') return <span className="text-[10px] font-medium uppercase tracking-wider text-emerald-400">Prediction Confirmed</span>;
  if (type === 'window_opening') return <span className="text-[10px] font-medium uppercase tracking-wider text-cyan-400">Window Opening</span>;
  return <span className="text-[10px] font-medium uppercase tracking-wider text-slate-400">New Tender</span>;
}

function FeedCard({ item }) {
  const dateLabel = item.event_date
    ? formatDistanceToNow(new Date(item.event_date), { addSuffix: true })
    : '';

  const linkTo = item.event_type === 'tender_published' || item.event_type === 'hit_confirmed'
    ? createPageUrl(`TenderDetail?id=${item.ref_id}`)
    : createPageUrl('Forecast');

  return (
    <Link to={linkTo} className="block transition-colors hover:bg-white/[0.02]">
      <div className="flex gap-3 px-4 py-3.5">
        <div className="mt-0.5 shrink-0">
          <FeedIcon type={item.event_type} />
        </div>
        <div className="min-w-0 flex-1 space-y-1">
          <FeedLabel type={item.event_type} />
          <p className="text-sm font-medium text-card-foreground leading-snug">
            {item.event_type === 'hit_confirmed'
              ? `${item.buyer_name} confirmed \u2013 ${item.category}`
              : item.event_type === 'window_opening'
                ? `Renewal window opens: ${item.buyer_name}`
                : item.title
                  ? (item.title.length > 90 ? item.title.slice(0, 87) + '...' : item.title)
                  : `New tender from ${item.buyer_name}`
            }
          </p>
          <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-xs text-muted-foreground">
            <span>{FLAG[item.country] || ''} {item.country}</span>
            {item.event_type !== 'tender_published' && item.category ? (
              <><span className="text-white/[0.12]">·</span><span>{item.category}</span></>
            ) : null}
            {item.event_type === 'hit_confirmed' && item.delta_days != null ? (
              <><span className="text-white/[0.12]">·</span><span className="text-emerald-400/80">{item.delta_days}d from prediction</span></>
            ) : null}
            {item.event_type === 'window_opening' && item.delta_days != null ? (
              <><span className="text-white/[0.12]">·</span><span className="text-cyan-400/80">{item.delta_days}% confidence</span></>
            ) : null}
            {dateLabel ? (
              <><span className="text-white/[0.12]">·</span><span>{dateLabel}</span></>
            ) : null}
          </div>
        </div>
        <span className="mt-1 shrink-0 text-xs text-muted-foreground/50">&rarr;</span>
      </div>
    </Link>
  );
}

export default function Home() {
  const [pulse, setPulse] = useState(null);
  const [feed, setFeed] = useState(null);
  const [pipeline, setPipeline] = useState(null);
  const [loading, setLoading] = useState(true);
  const { activeTenantId, isLoadingTenants } = useTenant();

  const loadData = useCallback(async () => {
    if (!activeTenantId) return;
    setLoading(true);
    try {
      const [pulseRes, feedRes, pipelineRes] = await Promise.allSettled([
        supabase.rpc('get_home_pulse', { p_tenant_id: activeTenantId }),
        supabase.rpc('get_home_feed', { p_tenant_id: activeTenantId, p_limit: 25 }),
        supabase.rpc('get_home_pipeline_snapshot', { p_tenant_id: activeTenantId }),
      ]);
      if (pulseRes.status === 'fulfilled' && !pulseRes.value.error) setPulse(pulseRes.value.data);
      if (feedRes.status === 'fulfilled' && !feedRes.value.error) setFeed(feedRes.value.data);
      if (pipelineRes.status === 'fulfilled' && !pipelineRes.value.error) setPipeline(pipelineRes.value.data);
    } catch (e) {
      console.error('Home load error:', e);
    } finally {
      setLoading(false);
    }
  }, [activeTenantId]);

  useEffect(() => {
    if (!isLoadingTenants && activeTenantId) loadData();
  }, [activeTenantId, isLoadingTenants, loadData]);

  if (loading || isLoadingTenants) {
    return (
      <Page>
        <PageBody>
          <div className="flex items-center justify-center h-64">
            <Loader2 className="h-6 w-6 animate-spin text-teal-400" />
          </div>
        </PageBody>
      </Page>
    );
  }

  const p = pulse || {};
  const pi = pipeline || {};
  const tw = pi.this_week || {};
  const tm = pi.this_month || {};
  const nq = pi.next_quarter || {};
  const acc = pi.accuracy || {};
  const feedItems = feed || [];

  return (
    <Page>
      <PageBody className="space-y-6">

        {/* ---- Pulse Strip ---- */}
        <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] px-5 py-4">
          <div className="flex items-center gap-2 mb-3">
            <Activity className="h-3.5 w-3.5 text-teal-400" />
            <span className="text-[10px] uppercase tracking-[0.1em] font-medium text-teal-400/80">Live Pulse</span>
          </div>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <Link to="/forecast" className="group">
              <div className="rounded-lg bg-white/[0.02] border border-white/[0.04] px-3 py-2.5 transition-colors group-hover:border-cyan-500/20 group-hover:bg-cyan-500/[0.03]">
                <p className="text-2xl font-semibold text-card-foreground tabular-nums">{(p.predictions_entering_window_7d || 0).toLocaleString()}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">predictions entering window this week</p>
              </div>
            </Link>
            <Link to="/workbench/search" className="group">
              <div className="rounded-lg bg-white/[0.02] border border-white/[0.04] px-3 py-2.5 transition-colors group-hover:border-white/[0.1]">
                <p className="text-2xl font-semibold text-card-foreground tabular-nums">{(p.new_tenders_7d || 0).toLocaleString()}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">new tenders this week</p>
              </div>
            </Link>
            <div className="rounded-lg bg-teal-500/[0.05] border border-teal-500/15 px-3 py-2.5">
              <p className="text-2xl font-semibold text-emerald-400 tabular-nums">{(p.hits_confirmed_30d || 0).toLocaleString()}</p>
              <p className="text-[10px] text-muted-foreground mt-0.5">predictions confirmed (30d)</p>
            </div>
            <div className="rounded-lg bg-white/[0.02] border border-white/[0.04] px-3 py-2.5">
              <p className="text-2xl font-semibold text-card-foreground tabular-nums">{(p.monitoring_total || 0).toLocaleString()}</p>
              <p className="text-[10px] text-muted-foreground mt-0.5">predictions monitoring</p>
            </div>
          </div>
        </div>

        {/* ---- Main Layout: Feed + Pipeline ---- */}
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-[1fr_380px]">

          {/* Left: Activity Feed */}
          <Card className="border border-white/[0.06] bg-white/[0.015] shadow-none">
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Zap className="h-4 w-4 text-teal-400" /> Activity Feed
                </CardTitle>
                <div className="flex items-center gap-2 text-[10px] text-muted-foreground">
                  <span className="flex items-center gap-1"><CheckCircle2 className="h-3 w-3 text-emerald-400" /> Confirmed</span>
                  <span className="flex items-center gap-1"><Target className="h-3 w-3 text-cyan-400" /> Opening</span>
                  <span className="flex items-center gap-1"><FileText className="h-3 w-3 text-slate-400" /> Published</span>
                </div>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              <div className="divide-y divide-white/[0.04]">
                {feedItems.length === 0 ? (
                  <div className="px-4 py-8 text-sm text-muted-foreground text-center">
                    No recent activity. Feed will populate as tenders are published and predictions enter their windows.
                  </div>
                ) : (
                  feedItems.slice(0, 15).map((item, i) => (
                    <FeedCard key={`${item.event_type}-${item.ref_id}-${i}`} item={item} />
                  ))
                )}
              </div>
              {feedItems.length > 15 ? (
                <div className="border-t border-white/[0.04] px-4 py-3 text-center">
                  <Link to="/forecast" className="text-xs text-emerald-400 hover:text-teal-300">
                    View all activity <ArrowRight className="inline h-3 w-3 ml-0.5" />
                  </Link>
                </div>
              ) : null}
            </CardContent>
          </Card>

          {/* Right: Pipeline Snapshot */}
          <div className="space-y-4">

            {/* Accuracy Badge */}
            <Card className="border border-teal-500/20 bg-teal-500/[0.04] shadow-none">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-[10px] uppercase tracking-wider text-teal-400/80">Prediction Accuracy</p>
                    <p className="text-3xl font-bold text-emerald-400 tabular-nums mt-1">{acc.rate || 0}%</p>
                    <p className="text-[10px] text-muted-foreground mt-1">
                      {(acc.confirmed || 0).toLocaleString()} confirmed of {(acc.total_resolved || 0).toLocaleString()} resolved
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Median Timing</p>
                    <p className="text-xl font-semibold text-card-foreground mt-1">&plusmn;{pi.median_timing_days || 0}d</p>
                    <p className="text-[10px] text-muted-foreground mt-1">from predicted</p>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Pipeline Timeline */}
            <Card className="border border-white/[0.06] bg-white/[0.015] shadow-none">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Calendar className="h-4 w-4 text-cyan-400" /> Pipeline at a Glance
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* This Week */}
                <div className="rounded-lg bg-cyan-500/[0.05] border border-cyan-500/15 px-3 py-3">
                  <div className="flex items-center justify-between">
                    <p className="text-[10px] uppercase tracking-wider text-cyan-400/80">This Week</p>
                    <p className="text-lg font-semibold text-cyan-400 tabular-nums">{(tw.count || 0).toLocaleString()}</p>
                  </div>
                  {tw.top_buyers?.length > 0 ? (
                    <div className="mt-2 space-y-1">
                      {tw.top_buyers.map((b, i) => (
                        <div key={i} className="flex items-center justify-between text-xs">
                          <span className="text-slate-300 truncate max-w-[200px]">{FLAG[b.country] || ''} {b.name}</span>
                          <span className="text-muted-foreground tabular-nums">{b.confidence}%</span>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>

                {/* This Month */}
                <div className="rounded-lg bg-white/[0.02] border border-white/[0.04] px-3 py-3">
                  <div className="flex items-center justify-between">
                    <p className="text-[10px] uppercase tracking-wider text-muted-foreground">This Month</p>
                    <p className="text-lg font-semibold text-card-foreground tabular-nums">{(tm.count || 0).toLocaleString()}</p>
                  </div>
                  {tm.by_country?.length > 0 ? (
                    <div className="mt-2 flex flex-wrap gap-3">
                      {tm.by_country.map((c, i) => (
                        <span key={i} className="text-xs text-muted-foreground">
                          {FLAG[c.country] || ''} {c.country}: <span className="text-slate-300 tabular-nums">{c.cnt.toLocaleString()}</span>
                        </span>
                      ))}
                    </div>
                  ) : null}
                </div>

                {/* Next Quarter */}
                <div className="rounded-lg bg-white/[0.02] border border-white/[0.04] px-3 py-3">
                  <div className="flex items-center justify-between">
                    <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Next 90 Days</p>
                    <p className="text-lg font-semibold text-card-foreground tabular-nums">{(nq.count || 0).toLocaleString()}</p>
                  </div>
                </div>

                <Link to="/forecast" className="block">
                  <Button variant="outline" size="sm" className="w-full text-xs border-white/[0.08] hover:bg-white/[0.04]">
                    View Full Forecast <ArrowRight className="ml-1.5 h-3 w-3" />
                  </Button>
                </Link>
              </CardContent>
            </Card>

            {/* Quick Links */}
            <div className="grid grid-cols-2 gap-3">
              <Link to="/workbench/search" className="group">
                <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none transition-colors group-hover:border-white/[0.1]">
                  <CardContent className="p-3 flex items-center gap-2">
                    <Eye className="h-4 w-4 text-muted-foreground group-hover:text-slate-300" />
                    <span className="text-xs text-muted-foreground group-hover:text-slate-300">Search Tenders</span>
                  </CardContent>
                </Card>
              </Link>
              <Link to="/reports" className="group">
                <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none transition-colors group-hover:border-white/[0.1]">
                  <CardContent className="p-3 flex items-center gap-2">
                    <TrendingUp className="h-4 w-4 text-muted-foreground group-hover:text-slate-300" />
                    <span className="text-xs text-muted-foreground group-hover:text-slate-300">View Reports</span>
                  </CardContent>
                </Card>
              </Link>
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
