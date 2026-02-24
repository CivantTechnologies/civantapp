import React, { useState, useEffect, useMemo } from 'react';
import { civant } from '@/api/civantClient';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useTenant } from '@/lib/tenant';
import { supabase } from '@/lib/supabaseClient';
import IntelligenceTrajectorySection from '@/components/home/IntelligenceTrajectorySection';
import HomePlatformFooter from '@/components/home/HomePlatformFooter';
import { 
    FileText, 
    Clock, 
    Bell, 
    TrendingUp,
    ArrowRight,
    Loader2,
} from 'lucide-react';
import { Page, PageHero, PageTitle, PageDescription, PageBody, Card, CardContent, CardHeader, CardTitle, Button } from '@/components/ui';
import { format, formatDistanceToNow, subDays, subMonths, isAfter, startOfDay, startOfMonth, addMonths } from 'date-fns';
import { isCompanyScopeFilterTemporarilyDisabled, setCompanyScopeFilterTemporarilyDisabled } from '@/lib/companyScopeSession';

const CLUSTER_ALIAS_MAP = {
    cluster_digital: 'cluster_it_software',
    digital: 'cluster_it_software',
    it: 'cluster_it_software',
    software: 'cluster_it_software',
    telecommunications: 'cluster_it_software',
    telecoms: 'cluster_it_software',
    cluster_professional_services: 'cluster_consulting',
    'professional services': 'cluster_consulting',
    consulting: 'cluster_consulting',
    construction: 'cluster_construction',
    cluster_facilities: 'cluster_facilities_maintenance',
    maintenance: 'cluster_facilities_maintenance',
    cluster_health: 'cluster_health_medical',
    healthcare: 'cluster_health_medical',
    medical: 'cluster_health_medical',
    cluster_education: 'cluster_education_training',
    education: 'cluster_education_training',
    transport: 'cluster_transport',
    food: 'cluster_food_catering',
    hospitality: 'cluster_food_catering',
    energy: 'cluster_energy_environment',
    environmental: 'cluster_energy_environment',
    cluster_environment: 'cluster_energy_environment',
    cluster_communications: 'cluster_communications_media',
    culture: 'cluster_communications_media',
    financial: 'cluster_financial_legal',
    legal: 'cluster_financial_legal',
    cluster_finance: 'cluster_financial_legal',
    cluster_legal: 'cluster_financial_legal',
    security: 'cluster_defence_security',
    cluster_defence: 'cluster_defence_security',
    cluster_research: 'cluster_research'
};

const BUYER_TYPE_PATTERNS = {
    education: /(university|college|school|universit[eé]|[eé]cole|education)/i,
    health: /(health|hospital|sant[eé]|h[oô]pital|hse|clinic)/i,
    local_authority: /(council|city|county|commune|ville|municipal|municipality|mairie)/i,
    central_government: /(ministry|minist[eè]re|minister|department|government|agency)/i,
    transport: /(transport|rail|railway|road|airport|port|infrastructure)/i,
    defence: /(defence|defense|military|police|gendarmerie|security)/i,
    utilities: /(water|electric|electricity|gas|telecom|utility|energy)/i
};

export default function Home() {
    const [stats, setStats] = useState(null);
    const [latestTenders, setLatestTenders] = useState([]);
    const [predictionRows, setPredictionRows] = useState([]);
    const [lastDataUpdateAt, setLastDataUpdateAt] = useState(null);
    const [trajectorySeries12m, setTrajectorySeries12m] = useState([]);
    const [trajectoryIndicators, setTrajectoryIndicators] = useState({
        missedRenewalCycles12m: 0,
        newBuyersDetected90d: 0,
        incumbentDominanceShift12m: 0
    });
    const [trajectoryRange, setTrajectoryRange] = useState('12m');
    const [loading, setLoading] = useState(true);
    const [profileScope, setProfileScope] = useState(null);
    const [loadError, setLoadError] = useState('');
    const { activeTenantId, isLoadingTenants } = useTenant();
    const [scopeTemporarilyDisabled, setScopeTemporarilyDisabled] = useState(() =>
        isCompanyScopeFilterTemporarilyDisabled(activeTenantId)
    );
    const clearScopeTemporarily = () => {
        setCompanyScopeFilterTemporarilyDisabled(activeTenantId, true);
        setScopeTemporarilyDisabled(true);
    };
    const restoreScopeFilter = () => {
        setCompanyScopeFilterTemporarilyDisabled(activeTenantId, false);
        setScopeTemporarilyDisabled(false);
    };
    
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
    const toArray = (value) => {
        if (!value) return [];
        if (Array.isArray(value)) {
            return value.map((item) => String(item || '').trim()).filter(Boolean);
        }
        if (typeof value === 'string') {
            return value
                .split(',')
                .map((item) => item.trim())
                .filter(Boolean);
        }
        return [];
    };
    const normalizeCluster = (value) => {
        const raw = String(value || '').trim();
        if (!raw) return '';
        const lower = raw.toLowerCase();
        return CLUSTER_ALIAS_MAP[lower] || lower;
    };
    const inferBuyerTypeMatch = (buyerName, selectedBuyerTypes) => {
        if (!selectedBuyerTypes.length) return true;
        const name = String(buyerName || '').trim();
        if (!name) return false;
        return selectedBuyerTypes.some((buyerType) => {
            if (buyerType === 'other') return true;
            const pattern = BUYER_TYPE_PATTERNS[buyerType];
            return pattern ? pattern.test(name) : false;
        });
    };
    const collectTrackedBuyerNames = (profile) => {
        const keys = [
            'tracked_buyers',
            'target_buyers',
            'priority_buyers',
            'watch_buyers',
            'key_buyers',
            'tracked_buyer_names'
        ];
        const values = keys.flatMap((key) => toArray(profile?.[key]));
        return Array.from(new Set(values.map((value) => value.toLowerCase())));
    };
    const getTenderClusterCandidates = (tender) => {
        const raw = [
            tender?.cpv_cluster,
            tender?.cpv_cluster_id,
            tender?.category,
            tender?.cpv_family,
            tender?.sector,
            tender?.cpv_cluster_label
        ];
        return raw
            .map((value) => normalizeCluster(value))
            .filter(Boolean);
    };
    const matchesScope = ({ country, clusters, buyerName }, scope) => {
        const normalizedCountry = String(country || '').trim().toUpperCase();
        if (scope.targetCountries.size && (!normalizedCountry || !scope.targetCountries.has(normalizedCountry))) {
            return false;
        }

        if (scope.targetClusters.size) {
            const hasClusterMatch = clusters.some((cluster) => scope.targetClusters.has(cluster));
            if (!hasClusterMatch) return false;
        }

        if (scope.trackedBuyerNames.length) {
            const normalizedBuyerName = String(buyerName || '').toLowerCase();
            if (!normalizedBuyerName) return false;
            return scope.trackedBuyerNames.some((tracked) => normalizedBuyerName.includes(tracked));
        }

        return inferBuyerTypeMatch(buyerName, scope.targetBuyerTypes);
    };
    const buildMonthlyTrajectorySeries = (totalRows, scopedRows) => {
        const now = new Date();
        const points = Array.from({ length: 12 }, (_, index) => {
            const monthDate = startOfMonth(subMonths(now, 11 - index));
            const monthKey = format(monthDate, 'yyyy-MM');
            return {
                monthKey,
                monthLabel: format(monthDate, 'MMM'),
                trackedScope: 0,
                totalMarket: 0
            };
        });
        const pointByKey = new Map(points.map((point) => [point.monthKey, point]));

        const applyRowsToSeries = (rows, key) => {
            rows.forEach((row) => {
                const publicationDate = parseDate(getTenderPublicationDate(row));
                if (!publicationDate) return;
                const monthKey = format(startOfMonth(publicationDate), 'yyyy-MM');
                const bucket = pointByKey.get(monthKey);
                if (!bucket) return;
                bucket[key] += 1;
            });
        };

        applyRowsToSeries(totalRows, 'totalMarket');
        applyRowsToSeries(scopedRows, 'trackedScope');

        return points.map(({ monthKey, ...rest }) => rest);
    };
    const computeTrajectoryIndicators = (scopedTenders, scopedPredictions) => {
        const now = new Date();
        const oneYearAgo = subMonths(now, 12);
        const sixMonthsAgo = subMonths(now, 6);
        const ninetyDaysAgo = subDays(now, 90);

        const missedRenewalCycles12m = scopedPredictions.filter((row) => {
            const predictedDate = parseDate(getPredictionDate(row));
            if (!predictedDate || predictedDate < oneYearAgo || predictedDate > now) return false;
            const urgency = String(row?.urgency || '').toLowerCase();
            const status = String(row?.status || row?.lifecycle_status || '').toLowerCase();
            const confidence = getPredictionConfidence(row) ?? 0;
            return urgency === 'overdue' || status === 'miss' || confidence >= 80;
        }).length;

        const recentBuyers = new Set();
        const baselineBuyers = new Set();
        scopedTenders.forEach((tender) => {
            const publicationDate = parseDate(getTenderPublicationDate(tender));
            const buyerName = String(tender?.buyer_name || '').trim().toLowerCase();
            if (!publicationDate || !buyerName) return;

            if (publicationDate >= ninetyDaysAgo) {
                recentBuyers.add(buyerName);
                return;
            }

            if (publicationDate >= oneYearAgo) {
                baselineBuyers.add(buyerName);
            }
        });
        const newBuyersDetected90d = Array.from(recentBuyers).filter((buyer) => !baselineBuyers.has(buyer)).length;

        const topBuyerShare = (startDate, endDate) => {
            const counts = new Map();
            let total = 0;

            scopedTenders.forEach((tender) => {
                const publicationDate = parseDate(getTenderPublicationDate(tender));
                if (!publicationDate || publicationDate < startDate || publicationDate >= endDate) return;
                const buyerName = String(tender?.buyer_name || '').trim().toLowerCase();
                if (!buyerName) return;
                counts.set(buyerName, (counts.get(buyerName) || 0) + 1);
                total += 1;
            });

            if (!total || counts.size === 0) return 0;
            const maxCount = Math.max(...counts.values());
            return (maxCount / total) * 100;
        };

        const previousTopShare = topBuyerShare(oneYearAgo, sixMonthsAgo);
        const currentTopShare = topBuyerShare(sixMonthsAgo, now);
        const incumbentDominanceShift12m = Number((currentTopShare - previousTopShare).toFixed(1));

        return {
            missedRenewalCycles12m,
            newBuyersDetected90d,
            incumbentDominanceShift12m
        };
    };

    const loadDashboardData = async () => {
        try {
            setLoadError('');
            const [allTenders, profileRows] = await Promise.all([
                civant.entities.TendersCurrent.list('-published_at', 1000),
                civant.entities.company_profiles.filter(
                    { tenant_id: activeTenantId },
                    '-updated_at',
                    1,
                    'target_cpv_clusters,target_countries,target_buyer_types,tracked_buyers,target_buyers,priority_buyers,watch_buyers,key_buyers,tracked_buyer_names'
                )
            ]);
            const profileScopeData = Array.isArray(profileRows) && profileRows.length > 0 ? profileRows[0] : null;
            setProfileScope(profileScopeData);
            const profileScope = profileScopeData;

            let predictions = [];
            try {
                const { data, error } = await supabase
                    .rpc('get_tenant_predictions', { p_tenant_id: activeTenantId })
                    .limit(500);
                if (error) throw error;
                predictions = Array.isArray(data) ? data : [];
            } catch (error) {
                console.warn('get_tenant_predictions unavailable for scoped stats:', error);
            }
            
            const now = new Date();
            const last24h = subDays(now, 1);
            const next7days = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
            const targetCountries = new Set(toArray(profileScope?.target_countries).map((country) => String(country).toUpperCase()));
            const targetClusters = new Set(toArray(profileScope?.target_cpv_clusters).map((cluster) => normalizeCluster(cluster)).filter(Boolean));
            const targetBuyerTypes = toArray(profileScope?.target_buyer_types).map((buyerType) => String(buyerType).toLowerCase());
            const trackedBuyerNames = collectTrackedBuyerNames(profileScope);
            const scope = {
                targetCountries,
                targetClusters,
                targetBuyerTypes,
                trackedBuyerNames
            };

            const scopedTenders = allTenders.filter((tender) => matchesScope({
                country: tender?.country,
                clusters: getTenderClusterCandidates(tender),
                buyerName: tender?.buyer_name
            }, scope));

            const scopedPredictions = predictions.filter((row) => {
                const clusters = [
                    row?.cpv_cluster_id,
                    row?.category,
                    row?.cpv_cluster_label,
                    row?.cpv_family
                ]
                    .map((cluster) => normalizeCluster(cluster))
                    .filter(Boolean);

                return matchesScope({
                    country: row?.region || row?.country,
                    clusters,
                    buyerName: row?.buyer_name || row?.buyer_display_name
                }, scope);
            });

            const newRelevantTenders24h = scopedTenders.filter((tender) =>
                getTenderFirstSeen(tender) && isAfter(new Date(getTenderFirstSeen(tender)), last24h)
            ).length;
            const expiringContracts7d = scopedTenders.filter((tender) => {
                if (!tender.deadline_date) return false;
                const deadline = new Date(tender.deadline_date);
                return !Number.isNaN(deadline.getTime()) && deadline >= now && deadline <= next7days;
            }).length;
            const openTrackedOpportunities = scopedTenders.filter((tender) => {
                if (!tender.deadline_date) return false;
                const deadline = new Date(tender.deadline_date);
                return !Number.isNaN(deadline.getTime()) && deadline >= startOfDay(now);
            }).length;
            const highConfidenceSignals = scopedPredictions.filter((row) => {
                const confidence = getPredictionConfidence(row);
                return confidence !== null && confidence >= 75;
            }).length;
            const scopedMovement7d = scopedTenders.filter((tender) =>
                getTenderFirstSeen(tender) && isAfter(new Date(getTenderFirstSeen(tender)), subDays(now, 7))
            ).length;

            setStats({
                newRelevantTenders24h: Number(newRelevantTenders24h ?? 0),
                expiringContracts7d: Number(expiringContracts7d ?? 0),
                highConfidenceSignals: Number(highConfidenceSignals ?? 0),
                openTrackedOpportunities: Number(openTrackedOpportunities ?? 0),
                competitorMovement7d: Number(scopedMovement7d ?? 0)
            });

            setTrajectorySeries12m(buildMonthlyTrajectorySeries(allTenders, scopedTenders));
            setTrajectoryIndicators(computeTrajectoryIndicators(scopedTenders, scopedPredictions));
            
            // Recent relevant activity is scoped and intentionally compact.
            setLatestTenders(scopedTenders.slice(0, 5));
            setPredictionRows(scopedPredictions);
            
            try {
                const [latestRun] = await civant.entities.ConnectorRuns.list('-started_at', 1);
                setLastDataUpdateAt(latestRun?.started_at || latestRun?.created_at || null);
            } catch (error) {
                console.warn('Unable to fetch latest connector run timestamp:', error);
                setLastDataUpdateAt(null);
            }
            
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
        const fromWindow = parseDate(topPrediction?.predicted_window_start);
        const signalDate = fromWindow || predictionDate;
        const opensInDays = signalDate
            ? Math.max(0, Math.ceil((signalDate.getTime() - now.getTime()) / 86400000))
            : null;
        const opensLabel = opensInDays === null ? 'Open date pending' : `Opens in ${opensInDays} days`;

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
            || 'Unknown buyer';
        const hasKnownBuyer = buyer !== 'Unknown buyer';

        const upcomingRenewals = predictionTimeline.filter((entry) => {
            if (!entry.predictedAt) return false;
            return entry.predictedAt >= now && entry.predictedAt <= horizon;
        }).length;
        const highConfidenceSignals = rankedPredictions.filter((entry) => Number(entry.confidence) >= 75).length;

        return {
            eventTitle: 'Renewal window approaching',
            buyer,
            hasKnownBuyer,
            opensLabel,
            confidence: topPredictionConfidence ?? 0,
            region,
            sector,
            upcomingRenewals,
            highConfidenceSignals,
            competitorMovement7d: Number(stats?.competitorMovement7d ?? 0)
        };
    }, [latestTenders, predictionRows, stats]);
    
    const StatCard = ({ title, value, icon: Icon, color, subtext = null, to }) => (
        <Link
            to={to}
            className="group block rounded-2xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 focus-visible:ring-offset-2 focus-visible:ring-offset-background"
            aria-label={`Open ${title}`}
        >
            <Card className="h-full cursor-pointer border border-white/[0.06] bg-white/[0.015] shadow-none transition-colors duration-150 group-hover:border-white/[0.12] group-hover:bg-white/[0.03]">
                <CardContent className="p-5">
                    <div className="flex items-start justify-between">
                        <div>
                            <p className="text-sm font-medium text-slate-300">{title}</p>
                            <p className={`mt-2 text-4xl font-semibold tracking-tight ${color}`}>{value}</p>
                            {subtext ? <p className="mt-1 text-xs text-muted-foreground/80">{subtext}</p> : null}
                        </div>
                        <div className="rounded-lg border border-primary/20 bg-primary/10 p-2.5 transition-transform duration-150 group-hover:-translate-y-px">
                            <Icon className={`h-4 w-4 ${color}`} />
                        </div>
                    </div>
                </CardContent>
            </Card>
        </Link>
    );
    
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
            <section className="bg-white/[0.03]">
                <div className="grid grid-cols-1 gap-8 px-12 py-14 md:py-16 lg:grid-cols-[minmax(0,1.55fr)_minmax(18rem,0.85fr)] lg:items-end">
                    <div className="max-w-[700px] space-y-4">
                        <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">Intelligence Briefing</p>
                        <h1 className="text-3xl font-semibold tracking-tight text-card-foreground md:text-4xl">
                            {briefing.eventTitle}
                        </h1>
                        <p className="text-xl font-medium text-card-foreground md:text-2xl">
                            {briefing.buyer}
                        </p>
                        <p className="text-sm text-muted-foreground">
                            {briefing.opensLabel} · Confidence {briefing.confidence}% · {briefing.region} · {briefing.sector}
                        </p>
                        <p className="text-xs text-muted-foreground/75">
                            Last data update: {lastDataUpdateAt ? formatDistanceToNow(new Date(lastDataUpdateAt), { addSuffix: true }) : 'Unknown'}
                        </p>
                        <div className="flex flex-wrap items-center gap-3 pt-2">
                            <Button asChild variant="primary">
                                <Link to={createPageUrl('Forecast')}>
                                    View Forecast
                                </Link>
                            </Button>
                            <Button asChild variant="ghost" className="text-slate-300 hover:bg-white/[0.05] hover:text-slate-100">
                                <Link to={createPageUrl(briefing.hasKnownBuyer ? `Search?buyer=${encodeURIComponent(briefing.buyer)}` : 'Search')}>
                                    View Buyer
                                </Link>
                            </Button>
                        </div>
                    </div>

                    <div className="space-y-4 lg:pl-1">
                        <div>
                            <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">Upcoming renewals (6 months)</p>
                            <p className="mt-1 text-4xl font-semibold tracking-tight text-card-foreground">
                                {briefing.upcomingRenewals.toLocaleString()}
                            </p>
                        </div>
                        <div>
                            <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">High confidence signals</p>
                            <p className="mt-1 text-4xl font-semibold tracking-tight text-card-foreground">
                                {briefing.highConfidenceSignals.toLocaleString()}
                            </p>
                        </div>
                        <div>
                            <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">Competitor movement (7 days)</p>
                            <p className="mt-1 text-4xl font-semibold tracking-tight text-card-foreground">
                                {briefing.competitorMovement7d.toLocaleString()}
                            </p>
                        </div>
                    </div>
                </div>
            </section>

            {profileScope && !scopeTemporarilyDisabled ? (
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-12 text-xs text-muted-foreground">
                    <span>Filtered by Company Scope</span>
                    <Link to={createPageUrl('Company?tab=personalization')} className="text-cyan-300 hover:underline">Edit scope</Link>
                    <button type="button" onClick={clearScopeTemporarily} className="text-cyan-300 hover:underline">Clear temporarily</button>
                </div>
            ) : profileScope && scopeTemporarilyDisabled ? (
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-12 text-xs text-muted-foreground">
                    <span>Company scope filter temporarily cleared for this session.</span>
                    <button type="button" onClick={restoreScopeFilter} className="text-cyan-300 hover:underline">Turn back on</button>
                </div>
            ) : !profileScope ? (
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-12 text-xs text-muted-foreground">
                    <span>No company scope configured.</span>
                    <Link to={createPageUrl('Company?tab=personalization')} className="text-cyan-300 hover:underline">Set up scope</Link>
                </div>
            ) : null}

            <PageBody>
                {loadError ? (
                    <div className="rounded-xl border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
                        {loadError}
                    </div>
                ) : null}
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                    <StatCard 
                        title="New Relevant Tenders (24h)" 
                        value={stats?.newRelevantTenders24h || 0}
                        icon={FileText}
                        color="text-primary"
                        to={createPageUrl('Search?lastTendered=1')}
                    />
                    <StatCard 
                        title="Expiring Contracts (7 days)" 
                        value={stats?.expiringContracts7d || 0}
                        icon={Clock}
                        color="text-card-foreground"
                        to={createPageUrl('Search?deadlineWithin=7')}
                    />
                    <StatCard 
                        title="High-Confidence Signals" 
                        value={stats?.highConfidenceSignals || 0}
                        icon={Bell}
                        color="text-primary"
                        to={createPageUrl('Forecast')}
                    />
                    <StatCard 
                        title="Open Opportunities (Tracked Scope)" 
                        value={stats?.openTrackedOpportunities || 0}
                        icon={TrendingUp}
                        color="text-card-foreground"
                        to={createPageUrl('Search')}
                    />
                </div>

                <IntelligenceTrajectorySection
                    series12m={trajectorySeries12m}
                    range={trajectoryRange}
                    onRangeChange={setTrajectoryRange}
                    indicators={trajectoryIndicators}
                />

                <Card className="border border-white/[0.06] bg-white/[0.015] shadow-none">
                    <CardHeader className="pb-3">
                        <div className="flex items-center justify-between">
                            <CardTitle className="text-lg font-semibold">Recent Relevant Activity</CardTitle>
                            <Link to={createPageUrl('Search')}>
                                <Button variant="ghost" size="sm">
                                    View all <ArrowRight className="ml-1 h-4 w-4" />
                                </Button>
                            </Link>
                        </div>
                    </CardHeader>
                    <CardContent className="p-0">
                        <div className="divide-y divide-white/[0.06]">
                            {latestTenders.length === 0 ? (
                                <div className="px-6 py-8 text-sm text-muted-foreground">
                                    No relevant activity yet. We will surface updates as tracked signals arrive.
                                </div>
                            ) : (
                                latestTenders.map((tender) => {
                                    const publishedAt = getTenderPublicationDate(tender);
                                    return (
                                        <Link
                                            key={tender.id}
                                            to={createPageUrl(`TenderDetail?id=${tender.id}`)}
                                            className="block px-6 py-4 transition-colors hover:bg-white/[0.02]"
                                        >
                                            <div className="flex items-start justify-between gap-4">
                                                <div className="min-w-0 flex-1 space-y-1">
                                                    <p className="truncate text-sm font-medium text-card-foreground">
                                                        {tender.title || 'Untitled notice'}
                                                    </p>
                                                    <p className="truncate text-xs text-muted-foreground">
                                                        {tender.buyer_name || 'Unknown buyer'}
                                                        {publishedAt ? ` · ${format(new Date(publishedAt), 'MMM d, yyyy')}` : ''}
                                                        {tender.source ? ` · ${String(tender.source).toUpperCase()}` : ''}
                                                    </p>
                                                </div>
                                                <span className="pt-0.5 text-xs text-muted-foreground">→</span>
                                            </div>
                                        </Link>
                                    );
                                })
                            )}
                        </div>
                    </CardContent>
                </Card>

                <HomePlatformFooter
                    version={import.meta.env.VITE_APP_VERSION || 'v0.9.3'}
                    lastDataRefresh={lastDataUpdateAt}
                    supportTo={createPageUrl('Company?section=support')}
                    legalTo={createPageUrl('Company?section=legal')}
                />
            </PageBody>
        </Page>
    );
}
