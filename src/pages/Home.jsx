import React, { useState, useEffect, useMemo } from 'react';
import { civant } from '@/api/civantClient';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useTenant } from '@/lib/tenant';
import { supabase } from '@/lib/supabaseClient';
import { 
    FileText, 
    Clock, 
    Bell, 
    TrendingUp,
    ArrowRight,
    CheckCircle2,
    AlertCircle,
    Loader2,
    Calendar,
    Building2
} from 'lucide-react';
import { Page, PageHero, PageTitle, PageDescription, PageBody, Card, CardContent, CardHeader, CardTitle, Button, Badge } from '@/components/ui';
import { format, formatDistanceToNow, subDays, isAfter, startOfDay, addMonths } from 'date-fns';

export default function Home() {
    const [stats, setStats] = useState(null);
    const [latestTenders, setLatestTenders] = useState([]);
    const [connectorHealth, setConnectorHealth] = useState([]);
    const [predictionRows, setPredictionRows] = useState([]);
    const [loading, setLoading] = useState(true);
    const [loadError, setLoadError] = useState('');
    const { activeTenantId, isLoadingTenants } = useTenant();
    
    useEffect(() => {
        if (isLoadingTenants) return;
        if (!activeTenantId) return;
        setLoading(true);
        void loadDashboardData();
    }, [activeTenantId, isLoadingTenants]);

    const getTenderPublicationDate = (tender) => tender.publication_date || tender.published_at || tender.first_seen_at || tender.updated_at;
    const getTenderFirstSeen = (tender) => tender.first_seen_at || tender.published_at || tender.publication_date || tender.updated_at;
    const parseDate = (value) => {
        if (!value) return null;
        const date = new Date(value);
        return Number.isNaN(date.getTime()) ? null : date;
    };
    const confidencePercent = (value) => {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) return null;
        if (numeric <= 1) {
            return Math.max(0, Math.min(100, Math.round(numeric * 100)));
        }
        return Math.max(0, Math.min(100, Math.round(numeric)));
    };
    const getPredictionDate = (row) =>
        row?.predicted_tender_date
        || row?.predicted_window_start
        || row?.contract_end_date
        || row?.generated_at
        || null;
    const getPredictionConfidence = (row) =>
        confidencePercent(
            row?.probability
            ?? row?.confidence
            ?? row?.confidence_score
            ?? row?.forecast_score
        );

    const mapRunToSource = (run) => {
        const candidates = [
            run?.source,
            run?.connector_key,
            run?.connector_id
        ].map((value) => String(value || '').trim()).filter(Boolean);

        for (const candidate of candidates) {
            const upper = candidate.toUpperCase();
            if (upper.startsWith('ETENDERS_IE_INCREMENTAL')) return 'ETENDERS_IE';
            if (upper.startsWith('ETENDERS_IE')) return 'ETENDERS_IE';
            if (upper.startsWith('BOAMP_FR')) return 'BOAMP_FR';
            if (upper.startsWith('PLACSP_ES')) return 'PLACSP_ES';
            if (upper === 'TED' || upper.startsWith('TED_')) return 'TED';
        }

        return '';
    };

    const readRunFetchedCount = (run) => {
        const metadata = run?.metadata && typeof run.metadata === 'object' ? run.metadata : {};
        const value = run?.fetched_count ?? metadata?.fetched_count ?? metadata?.rows_fetched ?? 0;
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : 0;
    };
    
    const loadDashboardData = async () => {
        try {
            setLoadError('');
            // Load a small slice for "Latest tenders" UI.
            const allTenders = await civant.entities.TendersCurrent.list('-published_at', 250);

            // Fast, tenant-scoped aggregates from the API (avoids pulling large lists for stats).
            // If the DB helper function isn't deployed yet, fall back to slice-based estimates.
            let dashboardStats = null;
            try {
                const statsPayload = await civant.functions.invoke('getDashboardStats', {});
                dashboardStats = statsPayload?.stats || null;
            } catch (error) {
                console.warn('getDashboardStats unavailable, falling back to client estimates:', error);
                setLoadError('Live dashboard aggregates are temporarily unavailable; showing sample-based estimates.');
            }
            
            const now = new Date();
            const last24h = subDays(now, 1);
            const next7days = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);

            const fallbackNew24h = allTenders.filter(t =>
                getTenderFirstSeen(t) && isAfter(new Date(getTenderFirstSeen(t)), last24h)
            ).length;
            const fallbackDeadlines7d = allTenders.filter(t => {
                if (!t.deadline_date) return false;
                const deadline = new Date(t.deadline_date);
                return deadline >= now && deadline <= next7days;
            }).length;
            const fallbackOpenTendersNow = allTenders.filter(t => {
                if (!t.deadline_date) return false;
                const deadline = new Date(t.deadline_date);
                return !Number.isNaN(deadline.getTime()) && deadline >= startOfDay(now);
            }).length;

            const fallbackCompetitorMovement7d = allTenders.filter(t =>
                getTenderFirstSeen(t) && isAfter(new Date(getTenderFirstSeen(t)), subDays(now, 7))
            ).length;

            setStats({
                newTenders24h: Number(dashboardStats?.new_tenders_24h ?? fallbackNew24h ?? 0),
                deadlinesIn7Days: Number(dashboardStats?.deadlines_in_7_days ?? fallbackDeadlines7d ?? 0),
                alertsTriggered: Number(dashboardStats?.alerts_triggered_24h ?? 0),
                openTendersNow: Number(dashboardStats?.open_tenders_now ?? fallbackOpenTendersNow ?? 0),
                competitorMovement7d: Number(dashboardStats?.competitor_movement_7d ?? fallbackCompetitorMovement7d ?? 0)
            });
            
            // Latest tenders
            setLatestTenders(allTenders.slice(0, 8));

            // Highest-confidence signal source (existing forecast engine data).
            let predictions = [];
            try {
                const { data, error } = await supabase
                    .rpc('get_tenant_predictions', { p_tenant_id: activeTenantId })
                    .limit(250);
                if (error) throw error;
                predictions = Array.isArray(data) ? data : [];
            } catch (error) {
                console.warn('get_tenant_predictions unavailable for briefing:', error);
            }
            setPredictionRows(predictions);
            
            // Connector health - get latest run per source
            const runs = await civant.entities.ConnectorRuns.list('-started_at', 50);
            const latestBySource = {};
            runs.forEach(run => {
                const source = mapRunToSource(run);
                if (!source) return;
                if (!latestBySource[source]) {
                    latestBySource[source] = {
                        ...run,
                        source,
                        fetched_count: readRunFetchedCount(run),
                        run_started_at: run?.started_at || run?.created_at || null
                    };
                }
            });
            setConnectorHealth(Object.values(latestBySource));
            
        } catch (error) {
            console.error('Error loading dashboard:', error);
            setLoadError(error?.message || 'Failed to load dashboard data');
        } finally {
            setLoading(false);
        }
    };

    const briefing = useMemo(() => {
        const now = new Date();
        const horizon = addMonths(now, 6);

        const predictionTimeline = predictionRows
            .map((row) => ({
                row,
                confidence: getPredictionConfidence(row),
                predictedAt: parseDate(getPredictionDate(row))
            }));

        const rankedPredictions = predictionTimeline
            .filter((entry) => entry.confidence !== null)
            .sort((a, b) => Number(b.confidence) - Number(a.confidence));

        const topPrediction = rankedPredictions[0]?.row || null;
        const topPredictionConfidence = rankedPredictions[0]?.confidence ?? null;
        const topTender = latestTenders[0] || null;

        const predictionDate = topPrediction ? parseDate(getPredictionDate(topPrediction)) : null;
        const publicationDate = topTender ? parseDate(getTenderPublicationDate(topTender)) : null;
        const fromWindow = parseDate(topPrediction?.predicted_window_start);
        const toWindow = parseDate(topPrediction?.predicted_window_end);

        const timeframe = fromWindow && toWindow
            ? `${format(fromWindow, 'MMM yyyy')} - ${format(toWindow, 'MMM yyyy')}`
            : predictionDate
                ? `Window ${format(predictionDate, 'MMM yyyy')}`
                : publicationDate
                    ? `Published ${format(publicationDate, 'MMM d, yyyy')}`
                    : 'Window pending';

        const region = topPrediction?.region || topPrediction?.country || topTender?.country || 'Multi-region';
        const sector = topPrediction?.category
            || topPrediction?.cpv_cluster_label
            || topPrediction?.cpv_cluster_id
            || topTender?.sector
            || topTender?.cpv_cluster
            || 'General procurement';
        const buyer = topPrediction?.buyer_name
            || topPrediction?.buyer_display_name
            || topTender?.buyer_name
            || '';

        const title = topPrediction?.signal_title
            || topPrediction?.title
            || (buyer ? `${buyer} procurement cycle watch` : null)
            || topTender?.title
            || 'No high-confidence signal available';

        const upcomingRenewals = predictionTimeline.filter((entry) => {
            if (!entry.predictedAt) return false;
            return entry.predictedAt >= now && entry.predictedAt <= horizon;
        }).length;
        const highConfidenceSignals = rankedPredictions.filter((entry) => Number(entry.confidence) >= 75).length;

        return {
            title,
            timeframe,
            confidence: topPredictionConfidence ?? 0,
            region,
            sector,
            buyer,
            upcomingRenewals,
            highConfidenceSignals,
            competitorMovement7d: Number(stats?.competitorMovement7d ?? 0)
        };
    }, [latestTenders, predictionRows, stats]);
    
    const StatCard = ({ title, value, icon: Icon, color, subtext, to }) => (
        <Link
            to={to}
            className="group block rounded-2xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 focus-visible:ring-offset-2 focus-visible:ring-offset-background"
            aria-label={`Open ${title}`}
        >
            <Card className="h-full cursor-pointer transition-all duration-150 group-hover:border-primary/40 group-hover:bg-card/95">
                <CardContent className="p-6">
                    <div className="flex items-start justify-between">
                        <div>
                            <p className="text-sm font-medium text-muted-foreground">{title}</p>
                            <p className={`text-3xl font-bold mt-2 ${color}`}>{value}</p>
                            {subtext && <p className="text-xs text-muted-foreground mt-1">{subtext}</p>}
                        </div>
                        <div className="p-3 rounded-xl bg-primary/15 border border-primary/25 transition-transform duration-150 group-hover:translate-x-0.5 group-hover:-translate-y-0.5">
                            <Icon className={`h-5 w-5 ${color}`} />
                        </div>
                    </div>
                </CardContent>
            </Card>
        </Link>
    );
    
    const getSourceBadge = (source) => {
        const colors = {
            'BOAMP_FR': 'bg-secondary text-secondary-foreground border-border',
            'TED': 'bg-primary/20 text-primary border-primary/30',
            'ETENDERS_IE': 'bg-primary/15 text-card-foreground border-border'
        };
        return colors[source] || 'bg-secondary text-secondary-foreground border-border';
    };
    
    const getCountryFlag = (country) => {
        return country === 'FR' ? '🇫🇷' : country === 'IE' ? '🇮🇪' : country === 'ES' ? '🇪🇸' : '🌍';
    };
    
    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-indigo-600" />
            </div>
        );
    }

    if (!activeTenantId) {
        return (
            <Page className="space-y-8">
                <PageHero>
                    <PageTitle>Select a workspace to continue</PageTitle>
                    <PageDescription>
                        Choose a tenant from the workspace switcher to load scoped intelligence and forecasts.
                    </PageDescription>
                </PageHero>
            </Page>
        );
    }
    
    return (
        <Page className="space-y-8">
            <section className="bg-black/12">
                <div className="grid grid-cols-1 gap-10 px-12 py-14 md:py-16 lg:grid-cols-[minmax(0,1.95fr)_minmax(0,1fr)] lg:items-end">
                    <div className="space-y-5">
                        <p className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">Intelligence Briefing</p>
                        <h1 className="max-w-4xl text-3xl font-semibold tracking-tight text-card-foreground md:text-5xl">
                            {briefing.title}
                        </h1>
                        <p className="text-sm text-muted-foreground md:text-base">
                            {briefing.timeframe} · Confidence {briefing.confidence}% · {briefing.region} · {briefing.sector}
                        </p>
                        <div className="flex flex-wrap items-center gap-3 pt-1">
                            <Button asChild variant="primary">
                                <Link to={createPageUrl('Forecast')}>
                                    View Forecast
                                </Link>
                            </Button>
                            <Button asChild variant="ghost" className="text-muted-foreground hover:text-card-foreground">
                                <Link to={createPageUrl(briefing.buyer ? `Search?buyer=${encodeURIComponent(briefing.buyer)}` : 'Search')}>
                                    View Buyer
                                </Link>
                            </Button>
                        </div>
                    </div>

                    <div className="space-y-6">
                        <div>
                            <p className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground">Upcoming renewals (6 months)</p>
                            <p className="mt-1 text-3xl font-semibold tracking-tight text-card-foreground">
                                {briefing.upcomingRenewals.toLocaleString()}
                            </p>
                        </div>
                        <div>
                            <p className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground">High confidence signals</p>
                            <p className="mt-1 text-3xl font-semibold tracking-tight text-card-foreground">
                                {briefing.highConfidenceSignals.toLocaleString()}
                            </p>
                        </div>
                        <div>
                            <p className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground">Competitor movement (7 days)</p>
                            <p className="mt-1 text-3xl font-semibold tracking-tight text-card-foreground">
                                {briefing.competitorMovement7d.toLocaleString()}
                            </p>
                        </div>
                    </div>
                </div>
            </section>

            <PageBody>
                {loadError ? (
                    <div className="rounded-xl border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
                        {loadError}
                    </div>
                ) : null}
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                    <StatCard 
                        title="New Tenders (24h)" 
                        value={stats?.newTenders24h || 0}
                        icon={FileText}
                        color="text-primary"
                        subtext="Just published"
                        to={createPageUrl('Search?lastTendered=1')}
                    />
                    <StatCard 
                        title="Deadlines (7 days)" 
                        value={stats?.deadlinesIn7Days || 0}
                        icon={Clock}
                        color="text-card-foreground"
                        subtext="Closing soon"
                        to={createPageUrl('Search?deadlineWithin=7')}
                    />
                    <StatCard 
                        title="Alerts Triggered" 
                        value={stats?.alertsTriggered || 0}
                        icon={Bell}
                        color="text-primary"
                        subtext="Last 24 hours"
                        to={createPageUrl('Alerts?view=triggered&period=24h')}
                    />
                    <StatCard 
                        title="Open Tenders" 
                        value={stats?.openTendersNow || 0}
                        icon={TrendingUp}
                        color="text-card-foreground"
                        subtext="Currently open"
                        to={createPageUrl('Search')}
                    />
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Latest Tenders */}
                <div className="lg:col-span-2">
                    <Card>
                        <CardHeader className="pb-3">
                            <div className="flex items-center justify-between">
                                <CardTitle className="text-lg font-semibold">Latest Tenders</CardTitle>
                                <Link to={createPageUrl('Search')}>
                                    <Button variant="ghost" size="sm">
                                        View all <ArrowRight className="h-4 w-4 ml-1" />
                                    </Button>
                                </Link>
                            </div>
                        </CardHeader>
                        <CardContent className="p-0">
                            <div className="divide-y divide-border/80">
                                {latestTenders.length === 0 ? (
                                    <div className="p-8 text-center text-muted-foreground">
                                        <FileText className="h-8 w-8 mx-auto mb-2 text-muted-foreground" />
                                        <p>No tenders yet. Run a connector to fetch data.</p>
                                    </div>
                                ) : (
                                    latestTenders.map(tender => {
                                        const firstSeenAt = getTenderFirstSeen(tender);
                                        const isNew = firstSeenAt && isAfter(new Date(firstSeenAt), subDays(new Date(), 1));
                                        const isUpdated = tender.version_count > 1;
                                        
                                        return (
                                            <Link
                                                key={tender.id}
                                                to={createPageUrl(`TenderDetail?id=${tender.id}`)}
                                                className="block p-4 hover:bg-muted/40 transition-colors"
                                            >
                                                <div className="flex items-start gap-3">
                                                    <div className="text-lg">{getCountryFlag(tender.country)}</div>
                                                    <div className="flex-1 min-w-0">
                                                        <div className="flex items-center gap-2 flex-wrap">
                                                            <h3 className="font-medium text-card-foreground truncate">
                                                                {tender.title}
                                                            </h3>
                                                            {isNew && (
                                                                <Badge className="text-xs" variant="primary">
                                                                    New
                                                                </Badge>
                                                            )}
                                                            {isUpdated && (
                                                                <Badge className="text-xs" variant="secondary">
                                                                    Updated
                                                                </Badge>
                                                            )}
                                                        </div>
                                                        <div className="flex items-center gap-3 mt-1.5 text-sm text-muted-foreground">
                                                            <span className="flex items-center gap-1">
                                                                <Building2 className="h-3.5 w-3.5" />
                                                                {tender.buyer_name || 'Unknown buyer'}
                                                            </span>
                                                            {tender.deadline_date && (
                                                                <span className="flex items-center gap-1">
                                                                    <Calendar className="h-3.5 w-3.5" />
                                                                    {format(new Date(tender.deadline_date), 'MMM d, yyyy')}
                                                                </span>
                                                            )}
                                                        </div>
                                                    </div>
                                                    <Badge className={`${getSourceBadge(tender.source)} border text-xs`}>
                                                        {tender.source}
                                                    </Badge>
                                                </div>
                                            </Link>
                                        );
                                    })
                                )}
                            </div>
                        </CardContent>
                    </Card>
                </div>
                
                {/* Connector Health */}
                <div>
                    <Card>
                        <CardHeader className="pb-3">
                            <CardTitle className="text-lg font-semibold">Connector Health</CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-3">
                            {['BOAMP_FR', 'TED', 'ETENDERS_IE', 'PLACSP_ES'].map(source => {
                                const run = connectorHealth.find(r => r.source === source);
                                const status = String(run?.status || '').toLowerCase();
                                
                                return (
                                    <div 
                                        key={source}
                                        className="flex items-center justify-between p-3 rounded-xl border border-border bg-muted/30"
                                    >
                                        <div className="flex items-center gap-3">
                                            {status === 'success' ? (
                                                <CheckCircle2 className="h-5 w-5 text-primary" />
                                            ) : status === 'fail' || status === 'failed' || status === 'error' ? (
                                                <AlertCircle className="h-5 w-5 text-destructive" />
                                            ) : status === 'partial' ? (
                                                <AlertCircle className="h-5 w-5 text-card-foreground" />
                                            ) : (
                                                <div className="h-5 w-5 rounded-full bg-secondary" />
                                            )}
                                            <div>
                                                <p className="font-medium text-sm text-card-foreground">{source}</p>
                                                {run ? (
                                                    <p className="text-xs text-muted-foreground">
                                                        {run.run_started_at ? formatDistanceToNow(new Date(run.run_started_at), { addSuffix: true }) : 'No run timestamp'}
                                                    </p>
                                                ) : (
                                                    <p className="text-xs text-muted-foreground">Never run</p>
                                                )}
                                            </div>
                                        </div>
                                        {run && (
                                            <div className="text-right">
                                                <p className="text-sm font-medium text-card-foreground">{run.fetched_count || 0}</p>
                                                <p className="text-xs text-muted-foreground">fetched</p>
                                            </div>
                                        )}
                                    </div>
                                );
                            })}
                        </CardContent>
                    </Card>
                </div>
                </div>
            </PageBody>
        </Page>
    );
}
