import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { 
    Play,
    CheckCircle2,
    AlertCircle,
    Clock,
    Loader2,
    Power,
    Calendar,
    Activity
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Switch } from '@/components/ui/switch';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { format, formatDistanceToNow } from 'date-fns';

export default function Connectors() {
    const [user, setUser] = useState(null);
    const [configs, setConfigs] = useState([]);
    const [runs, setRuns] = useState([]);
    const [loading, setLoading] = useState(true);
    const [runningConnector, setRunningConnector] = useState(null);
    
    const connectorDetails = {
        'BOAMP_FR': { 
            name: 'BOAMP France', 
            country: 'FR', 
            description: 'French government procurement portal',
            icon: '🇫🇷'
        },
        'TED_FR': { 
            name: 'TED France', 
            country: 'FR', 
            description: 'EU Tenders Electronic Daily (France)',
            icon: '🇪🇺'
        },
        'TED_IE': { 
            name: 'TED Ireland', 
            country: 'IE', 
            description: 'EU Tenders Electronic Daily (Ireland)',
            icon: '🇪🇺'
        },
        'ETENDERS_IE': { 
            name: 'eTenders Ireland', 
            country: 'IE', 
            description: 'Irish government eTenders portal',
            icon: '🇮🇪'
        },
        'ETENDERS_IE_INCREMENTAL': { 
            name: 'eTenders Ireland (Incremental)', 
            country: 'IE', 
            description: 'Incremental eTenders sync (new/updated CfTs only)',
            icon: '🇮🇪'
        }
    };
    
    useEffect(() => {
        checkAccess();
    }, []);
    
    const checkAccess = async () => {
        try {
            const userData = await civant.auth.me();
            setUser(userData);
            
            if (userData?.role === 'admin') {
                await loadData();
            }
        } catch (error) {
            console.error('Error checking access:', error);
        } finally {
            setLoading(false);
        }
    };
    
    const loadData = async () => {
        try {
            const [configsData, runsData] = await Promise.all([
                civant.entities.ConnectorConfig.list(),
                civant.entities.ConnectorRuns.list('-started_at', 100)
            ]);
            
            // Initialize configs for connectors that don't exist
            const existingIds = configsData.map(c => c.connector_id);
            const allConnectorIds = Object.keys(connectorDetails);
            
            for (const connectorId of allConnectorIds) {
                if (!existingIds.includes(connectorId)) {
                    await civant.entities.ConnectorConfig.create({
                        connector_id: connectorId,
                        enabled: true,
                        fetch_interval: 'daily',
                        total_runs: 0,
                        successful_runs: 0
                    });
                }
            }
            
            const updatedConfigs = await civant.entities.ConnectorConfig.list();
            setConfigs(updatedConfigs);
            setRuns(runsData);
            
        } catch (error) {
            console.error('Error loading data:', error);
        }
    };
    
    const toggleConnector = async (config) => {
        try {
            await civant.entities.ConnectorConfig.update(config.id, {
                enabled: !config.enabled
            });
            await loadData();
        } catch (error) {
            console.error('Error toggling connector:', error);
        }
    };
    
    const updateInterval = async (config, interval) => {
        try {
            await civant.entities.ConnectorConfig.update(config.id, {
                fetch_interval: interval
            });
            await loadData();
        } catch (error) {
            console.error('Error updating interval:', error);
        }
    };
    
    const runConnector = async (connectorId) => {
        setRunningConnector(connectorId);
        
        try {
            let response;
            const params = { days_since: 1825, limit: 100 };
            
            if (connectorId === 'BOAMP_FR') {
                response = await civant.functions.invoke('fetchBoampFr', params);
            } else if (connectorId === 'TED_FR') {
                response = await civant.functions.invoke('fetchTed', { ...params, country: 'FR' });
            } else if (connectorId === 'TED_IE') {
                response = await civant.functions.invoke('fetchTed', { ...params, country: 'IE' });
            } else if (connectorId === 'ETENDERS_IE') {
                response = await civant.functions.invoke('fetchIreland', params);
            } else if (connectorId === 'ETENDERS_IE_INCREMENTAL') {
                response = await civant.functions.invoke('fetchEtendersIeIncremental', params);
            }
            
            const config = configs.find(c => c.connector_id === connectorId);
            if (config) {
                const updateData = {
                    total_runs: (config.total_runs || 0) + 1,
                    last_error: null,
                    last_error_at: null
                };
                
                if (response?.data?.success) {
                    updateData.last_successful_run = new Date().toISOString();
                    updateData.successful_runs = (config.successful_runs || 0) + 1;
                } else {
                    updateData.last_error = response?.data?.error || 'Unknown error';
                    updateData.last_error_at = new Date().toISOString();
                }
                
                await civant.entities.ConnectorConfig.update(config.id, updateData);
            }
            
            await loadData();
            
        } catch (error) {
            const config = configs.find(c => c.connector_id === connectorId);
            if (config) {
                await civant.entities.ConnectorConfig.update(config.id, {
                    last_error: error.message,
                    last_error_at: new Date().toISOString(),
                    total_runs: (config.total_runs || 0) + 1
                });
            }
            await loadData();
        } finally {
            setRunningConnector(null);
        }
    };
    
    const getRecentRuns = (connectorId) => {
        const source = connectorId.replace('_FR', '').replace('_IE', '');
        return runs.filter(r => r.source === source).slice(0, 3);
    };
    
    const getSuccessRate = (config) => {
        if (!config.total_runs || config.total_runs === 0) return 0;
        return Math.round((config.successful_runs / config.total_runs) * 100);
    };
    
    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-indigo-600" />
            </div>
        );
    }
    
    if (user?.role !== 'admin') {
        return (
            <div className="text-center py-12">
                <AlertCircle className="h-12 w-12 mx-auto text-red-400 mb-4" />
                <h2 className="text-xl font-semibold text-slate-900">Access Denied</h2>
                <p className="text-slate-500 mt-2">This page is only accessible to administrators.</p>
            </div>
        );
    }
    
    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-slate-900">Data Connectors</h1>
                    <p className="text-slate-500 mt-1">Manage procurement data sources and sync schedules</p>
                </div>
                <Badge className="bg-indigo-50 text-indigo-700 border-indigo-200">
                    {configs.filter(c => c.enabled).length} / {configs.length} Active
                </Badge>
            </div>
            
            {/* Connector Cards */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {configs.map(config => {
                    const details = connectorDetails[config.connector_id];
                    const recentRuns = getRecentRuns(config.connector_id);
                    const successRate = getSuccessRate(config);
                    const isRunning = runningConnector === config.connector_id;
                    
                    return (
                        <Card key={config.id} className={`border-0 shadow-sm ${!config.enabled && 'opacity-60'}`}>
                            <CardHeader className="pb-3">
                                <div className="flex items-start justify-between">
                                    <div className="flex items-center gap-3">
                                        <div className={`text-3xl ${!config.enabled && 'grayscale'}`}>
                                            {details?.icon}
                                        </div>
                                        <div>
                                            <CardTitle className="text-lg font-semibold">
                                                {details?.name}
                                            </CardTitle>
                                            <p className="text-sm text-slate-500 mt-0.5">
                                                {details?.description}
                                            </p>
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        <Switch
                                            checked={config.enabled}
                                            onCheckedChange={() => toggleConnector(config)}
                                        />
                                        {config.enabled ? (
                                            <CheckCircle2 className="h-5 w-5 text-emerald-600" />
                                        ) : (
                                            <Power className="h-5 w-5 text-slate-400" />
                                        )}
                                    </div>
                                </div>
                            </CardHeader>
                            
                            <CardContent className="space-y-4">
                                {/* Stats Row */}
                                <div className="grid grid-cols-3 gap-4 p-3 bg-slate-50 rounded-lg">
                                    <div>
                                        <p className="text-xs text-slate-500">Success Rate</p>
                                        <p className={`text-lg font-bold ${
                                            successRate >= 80 ? 'text-emerald-600' :
                                            successRate >= 50 ? 'text-amber-600' : 'text-red-600'
                                        }`}>
                                            {successRate}%
                                        </p>
                                    </div>
                                    <div>
                                        <p className="text-xs text-slate-500">Total Runs</p>
                                        <p className="text-lg font-bold text-slate-900">
                                            {config.total_runs || 0}
                                        </p>
                                    </div>
                                    <div>
                                        <p className="text-xs text-slate-500">Successful</p>
                                        <p className="text-lg font-bold text-slate-900">
                                            {config.successful_runs || 0}
                                        </p>
                                    </div>
                                </div>
                                
                                {/* Last Run Status */}
                                {config.last_successful_run && (
                                    <div className="flex items-center gap-2 text-sm">
                                        <Clock className="h-4 w-4 text-slate-400" />
                                        <span className="text-slate-600">Last successful run:</span>
                                        <span className="font-medium text-slate-900">
                                            {formatDistanceToNow(new Date(config.last_successful_run), { addSuffix: true })}
                                        </span>
                                    </div>
                                )}
                                
                                {/* Error Display */}
                                {config.last_error && (
                                    <div className="p-3 bg-red-50 border border-red-100 rounded-lg">
                                        <div className="flex items-start gap-2">
                                            <AlertCircle className="h-4 w-4 text-red-600 mt-0.5" />
                                            <div className="flex-1 min-w-0">
                                                <p className="text-xs font-medium text-red-700">Last Error</p>
                                                <p className="text-xs text-red-600 mt-0.5 line-clamp-2">
                                                    {config.last_error}
                                                </p>
                                                {config.last_error_at && (
                                                    <p className="text-xs text-red-500 mt-1">
                                                        {format(new Date(config.last_error_at), 'MMM d, HH:mm')}
                                                    </p>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                )}
                                
                                {/* Recent Runs */}
                                {recentRuns.length > 0 && (
                                    <div className="space-y-2">
                                        <p className="text-xs font-medium text-slate-700 flex items-center gap-1">
                                            <Activity className="h-3 w-3" />
                                            Recent Runs
                                        </p>
                                        <div className="space-y-1.5">
                                            {recentRuns.map(run => (
                                                <div key={run.id} className="flex items-center justify-between text-xs p-2 bg-slate-50 rounded">
                                                    <div className="flex items-center gap-2">
                                                        <Badge className={
                                                            run.status === 'success' ? 'bg-emerald-50 text-emerald-700' :
                                                            run.status === 'partial' ? 'bg-amber-50 text-amber-700' :
                                                            'bg-red-50 text-red-700'
                                                        }>
                                                            {run.status}
                                                        </Badge>
                                                        <span className="text-slate-600">
                                                            {run.inserted_count || 0} inserted, {run.updated_count || 0} updated
                                                        </span>
                                                    </div>
                                                    <span className="text-slate-500">
                                                        {format(new Date(run.started_at), 'MMM d, HH:mm')}
                                                    </span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                                
                                {/* Configuration */}
                                <div className="pt-3 border-t border-slate-100 space-y-3">
                                    <div>
                                        <Label className="text-xs text-slate-600 mb-1.5 block flex items-center gap-1">
                                            <Calendar className="h-3 w-3" />
                                            Fetch Interval
                                        </Label>
                                        <Select 
                                            value={config.fetch_interval} 
                                            onValueChange={(value) => updateInterval(config, value)}
                                            disabled={!config.enabled}
                                        >
                                            <SelectTrigger className="bg-white">
                                                <SelectValue />
                                            </SelectTrigger>
                                            <SelectContent>
                                                <SelectItem value="hourly">Every Hour</SelectItem>
                                                <SelectItem value="daily">Daily</SelectItem>
                                                <SelectItem value="weekly">Weekly</SelectItem>
                                                <SelectItem value="manual">Manual Only</SelectItem>
                                            </SelectContent>
                                        </Select>
                                    </div>
                                    
                                    <Button 
                                        className="w-full"
                                        onClick={() => runConnector(config.connector_id)}
                                        disabled={!config.enabled || isRunning}
                                    >
                                        {isRunning ? (
                                            <>
                                                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                                Running...
                                            </>
                                        ) : (
                                            <>
                                                <Play className="h-4 w-4 mr-2" />
                                                Run Now (5 years)
                                            </>
                                        )}
                                    </Button>
                                </div>
                            </CardContent>
                        </Card>
                    );
                })}
            </div>
        </div>
    );
}