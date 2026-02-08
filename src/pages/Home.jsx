import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
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
import { Page, PageHeader, PageTitle, PageDescription, PageBody, Card, CardContent, CardHeader, CardTitle, Button, Badge } from '@/components/ui';
import { format, formatDistanceToNow, subDays, isAfter } from 'date-fns';

export default function Home() {
    const [stats, setStats] = useState(null);
    const [latestTenders, setLatestTenders] = useState([]);
    const [connectorHealth, setConnectorHealth] = useState([]);
    const [loading, setLoading] = useState(true);
    
    useEffect(() => {
        loadDashboardData();
    }, []);

    const getTenderPublicationDate = (tender) => tender.publication_date || tender.published_at || tender.first_seen_at || tender.updated_at;
    const getTenderFirstSeen = (tender) => tender.first_seen_at || tender.published_at || tender.publication_date || tender.updated_at;
    
    const loadDashboardData = async () => {
        try {
            // Load all tenders
            const allTenders = await civant.entities.TendersCurrent.list('-published_at', 1000);
            
            const now = new Date();
            const last24h = subDays(now, 1);
            const next7days = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
            
            // Calculate stats
            const newTenders24h = allTenders.filter(t => 
                getTenderFirstSeen(t) && isAfter(new Date(getTenderFirstSeen(t)), last24h)
            ).length;
            
            const deadlinesIn7Days = allTenders.filter(t => {
                if (!t.deadline_date) return false;
                const deadline = new Date(t.deadline_date);
                return deadline >= now && deadline <= next7days;
            }).length;
            
            // Load alert events from last 24h
            const alertEvents = await civant.entities.AlertEvents.filter({});
            const alertsTriggered = alertEvents.filter(e => 
                e.matched_at && isAfter(new Date(e.matched_at), last24h)
            ).length;
            
            setStats({
                newTenders24h,
                deadlinesIn7Days,
                alertsTriggered,
                totalTenders: allTenders.length
            });
            
            // Latest tenders
            const sortedTenders = [...allTenders].sort((a, b) => {
                const aTs = getTenderPublicationDate(a) ? new Date(getTenderPublicationDate(a)).getTime() : 0;
                const bTs = getTenderPublicationDate(b) ? new Date(getTenderPublicationDate(b)).getTime() : 0;
                return bTs - aTs;
            });
            setLatestTenders(sortedTenders.slice(0, 8));
            
            // Connector health - get latest run per source
            const runs = await civant.entities.ConnectorRuns.list('-started_at', 50);
            const latestBySource = {};
            runs.forEach(run => {
                if (!latestBySource[run.source]) {
                    latestBySource[run.source] = run;
                }
            });
            setConnectorHealth(Object.values(latestBySource));
            
        } catch (error) {
            console.error('Error loading dashboard:', error);
        } finally {
            setLoading(false);
        }
    };
    
    const StatCard = ({ title, value, icon: Icon, color, subtext }) => (
        <Card className="hover:border-primary/30 transition-colors">
            <CardContent className="p-6">
                <div className="flex items-start justify-between">
                    <div>
                        <p className="text-sm font-medium text-muted-foreground">{title}</p>
                        <p className={`text-3xl font-bold mt-2 ${color}`}>{value}</p>
                        {subtext && <p className="text-xs text-muted-foreground mt-1">{subtext}</p>}
                    </div>
                    <div className="p-3 rounded-xl bg-primary/15 border border-primary/25">
                        <Icon className={`h-5 w-5 ${color}`} />
                    </div>
                </div>
            </CardContent>
        </Card>
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
        return country === 'FR' ? '🇫🇷' : country === 'IE' ? '🇮🇪' : '🌍';
    };
    
    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-indigo-600" />
            </div>
        );
    }
    
    return (
        <Page className="space-y-8">
            <PageHeader className="gap-4">
                <span className="inline-flex w-fit rounded-full border border-primary/30 bg-primary/15 px-3 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-primary">
                    Civant Radar
                </span>
                <div className="space-y-3">
                    <PageTitle className="max-w-3xl text-3xl md:text-4xl">
                        Track public procurement shifts before your competitors do
                    </PageTitle>
                    <PageDescription className="max-w-2xl">
                        AI-assisted monitoring for Irish and French tender markets, with alerts and predictive signals from live procurement activity.
                    </PageDescription>
                </div>
                <div className="flex flex-wrap gap-2">
                    <Button asChild variant="primary">
                        <Link to={createPageUrl('Search')}>
                            Explore tenders
                            <ArrowRight className="h-4 w-4 ml-1" />
                        </Link>
                    </Button>
                    <Button asChild variant="secondary">
                        <Link to={createPageUrl('Predictions')}>
                            View predictions
                        </Link>
                    </Button>
                </div>
            </PageHeader>

            <PageBody>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                    <StatCard 
                        title="New Tenders (24h)" 
                        value={stats?.newTenders24h || 0}
                        icon={FileText}
                        color="text-primary"
                        subtext="Just published"
                    />
                    <StatCard 
                        title="Deadlines (7 days)" 
                        value={stats?.deadlinesIn7Days || 0}
                        icon={Clock}
                        color="text-card-foreground"
                        subtext="Closing soon"
                    />
                    <StatCard 
                        title="Alerts Triggered" 
                        value={stats?.alertsTriggered || 0}
                        icon={Bell}
                        color="text-primary"
                        subtext="Last 24 hours"
                    />
                    <StatCard 
                        title="Total Tenders" 
                        value={stats?.totalTenders || 0}
                        icon={TrendingUp}
                        color="text-card-foreground"
                        subtext="In database"
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
                            {['BOAMP_FR', 'TED', 'ETENDERS_IE'].map(source => {
                                const run = connectorHealth.find(r => r.source === source);
                                const status = run?.status;
                                
                                return (
                                    <div 
                                        key={source}
                                        className="flex items-center justify-between p-3 rounded-xl border border-border bg-muted/30"
                                    >
                                        <div className="flex items-center gap-3">
                                            {status === 'success' ? (
                                                <CheckCircle2 className="h-5 w-5 text-primary" />
                                            ) : status === 'fail' ? (
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
                                                        {formatDistanceToNow(new Date(run.started_at), { addSuffix: true })}
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
