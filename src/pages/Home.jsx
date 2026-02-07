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
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { format, formatDistanceToNow, subDays, isAfter } from 'date-fns';

export default function Home() {
    const [stats, setStats] = useState(null);
    const [latestTenders, setLatestTenders] = useState([]);
    const [connectorHealth, setConnectorHealth] = useState([]);
    const [loading, setLoading] = useState(true);
    
    useEffect(() => {
        loadDashboardData();
    }, []);
    
    const loadDashboardData = async () => {
        try {
            // Load all tenders
            const allTenders = await civant.entities.TendersCurrent.list('-first_seen_at', 1000);
            
            const now = new Date();
            const last24h = subDays(now, 1);
            const next7days = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
            
            // Calculate stats
            const newTenders24h = allTenders.filter(t => 
                t.first_seen_at && isAfter(new Date(t.first_seen_at), last24h)
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
            setLatestTenders(allTenders.slice(0, 8));
            
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
        <Card className="border-0 shadow-sm hover:shadow-md transition-shadow">
            <CardContent className="p-6">
                <div className="flex items-start justify-between">
                    <div>
                        <p className="text-sm font-medium text-slate-500">{title}</p>
                        <p className={`text-3xl font-bold mt-2 ${color}`}>{value}</p>
                        {subtext && <p className="text-xs text-slate-400 mt-1">{subtext}</p>}
                    </div>
                    <div className={`p-3 rounded-xl ${color.replace('text-', 'bg-').replace('-600', '-50')}`}>
                        <Icon className={`h-5 w-5 ${color}`} />
                    </div>
                </div>
            </CardContent>
        </Card>
    );
    
    const getSourceBadge = (source) => {
        const colors = {
            'BOAMP_FR': 'bg-blue-50 text-blue-700 border-blue-200',
            'TED': 'bg-purple-50 text-purple-700 border-purple-200',
            'ETENDERS_IE': 'bg-emerald-50 text-emerald-700 border-emerald-200'
        };
        return colors[source] || 'bg-slate-50 text-slate-700 border-slate-200';
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
        <div className="space-y-8">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-bold text-slate-900">Dashboard</h1>
                <p className="text-slate-500 mt-1">Predict future tenders using AI-powered analysis of historical contract awards</p>
            </div>
            
            {/* Stats Grid */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <StatCard 
                    title="New Tenders (24h)" 
                    value={stats?.newTenders24h || 0}
                    icon={FileText}
                    color="text-indigo-600"
                    subtext="Just published"
                />
                <StatCard 
                    title="Deadlines (7 days)" 
                    value={stats?.deadlinesIn7Days || 0}
                    icon={Clock}
                    color="text-amber-600"
                    subtext="Closing soon"
                />
                <StatCard 
                    title="Alerts Triggered" 
                    value={stats?.alertsTriggered || 0}
                    icon={Bell}
                    color="text-emerald-600"
                    subtext="Last 24 hours"
                />
                <StatCard 
                    title="Total Tenders" 
                    value={stats?.totalTenders || 0}
                    icon={TrendingUp}
                    color="text-slate-600"
                    subtext="In database"
                />
            </div>
            
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Latest Tenders */}
                <div className="lg:col-span-2">
                    <Card className="border-0 shadow-sm">
                        <CardHeader className="pb-3">
                            <div className="flex items-center justify-between">
                                <CardTitle className="text-lg font-semibold">Latest Tenders</CardTitle>
                                <Link to={createPageUrl('Search')}>
                                    <Button variant="ghost" size="sm" className="text-indigo-600 hover:text-indigo-700">
                                        View all <ArrowRight className="h-4 w-4 ml-1" />
                                    </Button>
                                </Link>
                            </div>
                        </CardHeader>
                        <CardContent className="p-0">
                            <div className="divide-y divide-slate-100">
                                {latestTenders.length === 0 ? (
                                    <div className="p-8 text-center text-slate-500">
                                        <FileText className="h-8 w-8 mx-auto mb-2 text-slate-300" />
                                        <p>No tenders yet. Run a connector to fetch data.</p>
                                    </div>
                                ) : (
                                    latestTenders.map(tender => {
                                        const isNew = tender.first_seen_at && 
                                            isAfter(new Date(tender.first_seen_at), subDays(new Date(), 1));
                                        const isUpdated = tender.version_count > 1;
                                        
                                        return (
                                            <Link
                                                key={tender.id}
                                                to={createPageUrl(`TenderDetail?id=${tender.id}`)}
                                                className="block p-4 hover:bg-slate-50 transition-colors"
                                            >
                                                <div className="flex items-start gap-3">
                                                    <div className="text-lg">{getCountryFlag(tender.country)}</div>
                                                    <div className="flex-1 min-w-0">
                                                        <div className="flex items-center gap-2 flex-wrap">
                                                            <h3 className="font-medium text-slate-900 truncate">
                                                                {tender.title}
                                                            </h3>
                                                            {isNew && (
                                                                <Badge className="bg-emerald-50 text-emerald-700 border-emerald-200 text-xs">
                                                                    New
                                                                </Badge>
                                                            )}
                                                            {isUpdated && (
                                                                <Badge className="bg-amber-50 text-amber-700 border-amber-200 text-xs">
                                                                    Updated
                                                                </Badge>
                                                            )}
                                                        </div>
                                                        <div className="flex items-center gap-3 mt-1.5 text-sm text-slate-500">
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
                    <Card className="border-0 shadow-sm">
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
                                        className="flex items-center justify-between p-3 rounded-xl bg-slate-50"
                                    >
                                        <div className="flex items-center gap-3">
                                            {status === 'success' ? (
                                                <CheckCircle2 className="h-5 w-5 text-emerald-500" />
                                            ) : status === 'fail' ? (
                                                <AlertCircle className="h-5 w-5 text-red-500" />
                                            ) : status === 'partial' ? (
                                                <AlertCircle className="h-5 w-5 text-amber-500" />
                                            ) : (
                                                <div className="h-5 w-5 rounded-full bg-slate-200" />
                                            )}
                                            <div>
                                                <p className="font-medium text-sm text-slate-900">{source}</p>
                                                {run ? (
                                                    <p className="text-xs text-slate-500">
                                                        {formatDistanceToNow(new Date(run.started_at), { addSuffix: true })}
                                                    </p>
                                                ) : (
                                                    <p className="text-xs text-slate-400">Never run</p>
                                                )}
                                            </div>
                                        </div>
                                        {run && (
                                            <div className="text-right">
                                                <p className="text-sm font-medium text-slate-900">{run.fetched_count || 0}</p>
                                                <p className="text-xs text-slate-500">fetched</p>
                                            </div>
                                        )}
                                    </div>
                                );
                            })}
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}