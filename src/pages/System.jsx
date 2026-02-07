import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { 
    Play, 
    RefreshCw,
    CheckCircle2,
    AlertCircle,
    Loader2,
    TestTube,
    FileText,
    Zap,
    Upload
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { format, formatDistanceToNow } from 'date-fns';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function System() {
    const [user, setUser] = useState(null);
    const [runs, setRuns] = useState([]);
    const [stats, setStats] = useState({});
    const [loading, setLoading] = useState(true);
    const [runningConnector, setRunningConnector] = useState(null);
    const [testResults, setTestResults] = useState({});
    const [uploading, setUploading] = useState(false);
    const [uploadResult, setUploadResult] = useState(null);
    
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
            // Load connector runs
            const runsData = await civant.entities.ConnectorRuns.list('-started_at', 100);
            setRuns(runsData);
            
            // Calculate stats by source
            const tenders = await civant.entities.TendersCurrent.list('-created_date', 1000);
            const statsBySource = {};
            tenders.forEach(t => {
                if (!statsBySource[t.source]) {
                    statsBySource[t.source] = { count: 0, missingBuyer: 0, missingDeadline: 0 };
                }
                statsBySource[t.source].count++;
                if (!t.buyer_name) statsBySource[t.source].missingBuyer++;
                if (!t.deadline_date) statsBySource[t.source].missingDeadline++;
            });
            setStats(statsBySource);
            
        } catch (error) {
            console.error('Error loading data:', error);
        }
    };
    
    const runConnector = async (source, daysSince = 1825) => {
        setRunningConnector(source);
        setTestResults(prev => ({ ...prev, [source]: { running: true } }));
        
        try {
            let response;
            
            if (source === 'BOAMP_FR') {
                response = await civant.functions.invoke('fetchBoampFr', {
                    days_since: daysSince,
                    limit: 100
                });
            } else if (source === 'TED_IE' || source === 'TED_FR' || source === 'TED') {
                response = await civant.functions.invoke('fetchTed', {
                    country: source === 'TED_IE' || source === 'TED' ? 'IE' : 'FR',
                    days_since: daysSince,
                    limit: 100
                });
            } else if (source === 'ETENDERS_IE') {
                response = await civant.functions.invoke('fetchIreland', {
                    days_since: daysSince,
                    limit: 100
                });
            }
            
            setTestResults(prev => ({ 
                ...prev, 
                [source]: { 
                    success: true, 
                    data: response?.data,
                    running: false 
                } 
            }));
            
            await loadData();
            
        } catch (error) {
            setTestResults(prev => ({ 
                ...prev, 
                [source]: { 
                    success: false, 
                    error: error.message || 'Unknown error',
                    running: false 
                } 
            }));
        } finally {
            setRunningConnector(null);
        }
    };
    
    const handleFileUpload = async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;
        
        setUploading(true);
        setUploadResult(null);
        
        try {
            // Upload file
            const { file_url } = await civant.integrations.Core.UploadFile({ file });
            
            // Import data
            const response = await civant.functions.invoke('importCsvData', {
                file_url: file_url,
                data_type: 'tenders'
            });
            
            setUploadResult(response.data);
            await loadData();
        } catch (error) {
            setUploadResult({
                success: false,
                error: error.message
            });
        } finally {
            setUploading(false);
            e.target.value = '';
        }
    };
    
    const getStatusBadge = (status) => {
        const styles = {
            success: 'bg-emerald-50 text-emerald-700 border-emerald-200',
            partial: 'bg-amber-50 text-amber-700 border-amber-200',
            fail: 'bg-red-50 text-red-700 border-red-200'
        };
        return styles[status] || 'bg-slate-50 text-slate-700 border-slate-200';
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
    
    // Get latest run per source
    const latestRuns = {};
    runs.forEach(run => {
        if (!latestRuns[run.source]) {
            latestRuns[run.source] = run;
        }
    });
    
    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-slate-900">System Admin</h1>
                    <p className="text-slate-500 mt-1">Monitor connectors and data quality</p>
                </div>
                <Badge className="bg-red-50 text-red-700 border-red-200">Admin Only</Badge>
            </div>
            
            <Tabs defaultValue="connectors" className="space-y-6">
                <TabsList>
                    <TabsTrigger value="connectors">Connectors</TabsTrigger>
                    <TabsTrigger value="import">Import Data</TabsTrigger>
                    <TabsTrigger value="testlab">Test Lab</TabsTrigger>
                    <TabsTrigger value="logs">Run Logs</TabsTrigger>
                </TabsList>
                
                {/* Connectors Tab */}
                <TabsContent value="connectors" className="space-y-6">
                    {/* Run All Button */}
                    <div className="flex justify-end">
                        <Button 
                            onClick={async () => {
                                setRunningConnector('ALL');
                                try {
                                    const response = await civant.functions.invoke('runAllConnectors', {});
                                    setTestResults(prev => ({ ...prev, ALL: { success: true, data: response.data } }));
                                    await loadData();
                                } catch (error) {
                                    setTestResults(prev => ({ ...prev, ALL: { success: false, error: error.message } }));
                                } finally {
                                    setRunningConnector(null);
                                }
                            }}
                            disabled={runningConnector === 'ALL'}
                            className="bg-indigo-600 hover:bg-indigo-700"
                        >
                            {runningConnector === 'ALL' ? (
                                <>
                                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                    Running All...
                                </>
                            ) : (
                                <>
                                    <Zap className="h-4 w-4 mr-2" />
                                    Run All Connectors
                                </>
                            )}
                        </Button>
                    </div>
                    
                    {/* Connector Status Cards */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        {['BOAMP_FR', 'TED', 'ETENDERS_IE'].map(source => {
                            const run = latestRuns[source];
                            const stat = stats[source] || { count: 0, missingBuyer: 0, missingDeadline: 0 };
                            
                            return (
                                <Card key={source} className="border-0 shadow-sm">
                                    <CardHeader className="pb-2">
                                        <div className="flex items-center justify-between">
                                            <CardTitle className="text-base font-semibold">{source}</CardTitle>
                                            {run ? (
                                                <Badge className={`${getStatusBadge(run.status)} border`}>
                                                    {run.status}
                                                </Badge>
                                            ) : (
                                                <Badge variant="outline">Never run</Badge>
                                            )}
                                        </div>
                                    </CardHeader>
                                    <CardContent className="space-y-3">
                                        <div className="grid grid-cols-2 gap-3 text-sm">
                                            <div>
                                                <p className="text-slate-500">Total Records</p>
                                                <p className="font-semibold text-slate-900">{stat.count}</p>
                                            </div>
                                            <div>
                                                <p className="text-slate-500">Last Run</p>
                                                <p className="font-semibold text-slate-900">
                                                    {run?.started_at 
                                                        ? formatDistanceToNow(new Date(run.started_at), { addSuffix: true })
                                                        : '-'
                                                    }
                                                </p>
                                            </div>
                                            {run && (
                                                <>
                                                    <div>
                                                        <p className="text-slate-500">Fetched</p>
                                                        <p className="font-semibold">{run.fetched_count || 0}</p>
                                                    </div>
                                                    <div>
                                                        <p className="text-slate-500">Inserted</p>
                                                        <p className="font-semibold text-emerald-600">{run.inserted_count || 0}</p>
                                                    </div>
                                                </>
                                            )}
                                        </div>
                                        
                                        {/* Data Quality */}
                                        <div className="pt-2 border-t border-slate-100">
                                            <p className="text-xs text-slate-500 mb-1">Data Quality</p>
                                            <div className="flex gap-2 text-xs">
                                                <span className={stat.missingBuyer > 0 ? 'text-amber-600' : 'text-emerald-600'}>
                                                    {stat.count > 0 ? Math.round((1 - stat.missingBuyer / stat.count) * 100) : 100}% buyer
                                                </span>
                                                <span className={stat.missingDeadline > 0 ? 'text-amber-600' : 'text-emerald-600'}>
                                                    {stat.count > 0 ? Math.round((1 - stat.missingDeadline / stat.count) * 100) : 100}% deadline
                                                </span>
                                            </div>
                                        </div>
                                        
                                        <Button 
                                            className="w-full"
                                            onClick={() => runConnector(source)}
                                            disabled={runningConnector === source}
                                        >
                                            {runningConnector === source ? (
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
                                    </CardContent>
                                </Card>
                            );
                        })}
                    </div>
                </TabsContent>
                
                {/* Import Data Tab */}
                <TabsContent value="import" className="space-y-6">
                    <Card className="border-0 shadow-sm">
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Upload className="h-5 w-5" />
                                Import Tender Data from CSV
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-6">
                            <div>
                                <p className="text-slate-500 mb-4">
                                    Upload CSV files with tender data. The system will automatically parse and import records into the database.
                                </p>
                                <div className="bg-slate-50 p-4 rounded-lg mb-4">
                                    <p className="text-sm font-medium text-slate-700 mb-2">Expected CSV columns:</p>
                                    <div className="text-xs text-slate-600 space-y-1">
                                        <p>• tender_uid, source, source_notice_id</p>
                                        <p>• country (IE/FR), notice_type (tender/award)</p>
                                        <p>• title, buyer_name, cpv_codes</p>
                                        <p>• publication_date, deadline_date</p>
                                        <p>• estimated_value, currency, url</p>
                                    </div>
                                </div>
                                
                                <div className="flex items-center gap-4">
                                    <label className="cursor-pointer">
                                        <input 
                                            type="file" 
                                            accept=".csv,.xlsx,.xls"
                                            onChange={handleFileUpload}
                                            disabled={uploading}
                                            className="hidden"
                                        />
                                        <div className={`px-6 py-3 rounded-lg font-medium transition-colors ${
                                            uploading 
                                                ? 'bg-slate-300 text-slate-500 cursor-not-allowed'
                                                : 'bg-indigo-600 text-white hover:bg-indigo-700'
                                        }`}>
                                            {uploading ? (
                                                <span className="flex items-center gap-2">
                                                    <Loader2 className="h-4 w-4 animate-spin" />
                                                    Importing...
                                                </span>
                                            ) : (
                                                <span className="flex items-center gap-2">
                                                    <Upload className="h-4 w-4" />
                                                    Select CSV File
                                                </span>
                                            )}
                                        </div>
                                    </label>
                                </div>
                            </div>
                            
                            {/* Upload Results */}
                            {uploadResult && (
                                <div className={`p-4 rounded-lg ${
                                    uploadResult.success 
                                        ? 'bg-emerald-50 border border-emerald-200'
                                        : 'bg-red-50 border border-red-200'
                                }`}>
                                    {uploadResult.success ? (
                                        <div className="space-y-2">
                                            <div className="flex items-center gap-2 text-emerald-700 font-semibold mb-3">
                                                <CheckCircle2 className="h-5 w-5" />
                                                Import Successful
                                            </div>
                                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                                                <div>
                                                    <p className="text-slate-500">Total Records</p>
                                                    <p className="text-xl font-bold text-slate-900">{uploadResult.total_records}</p>
                                                </div>
                                                <div>
                                                    <p className="text-slate-500">Inserted</p>
                                                    <p className="text-xl font-bold text-emerald-600">{uploadResult.inserted}</p>
                                                </div>
                                                <div>
                                                    <p className="text-slate-500">Updated</p>
                                                    <p className="text-xl font-bold text-amber-600">{uploadResult.updated}</p>
                                                </div>
                                                <div>
                                                    <p className="text-slate-500">Errors</p>
                                                    <p className="text-xl font-bold text-red-600">{uploadResult.errors}</p>
                                                </div>
                                            </div>
                                            {uploadResult.error_details && uploadResult.error_details.length > 0 && (
                                                <div className="mt-3 pt-3 border-t border-emerald-200">
                                                    <p className="text-xs font-medium text-slate-700 mb-2">Error Details (first 5):</p>
                                                    <ul className="text-xs text-slate-600 space-y-1">
                                                        {uploadResult.error_details.map((err, idx) => (
                                                            <li key={idx}>• {err.record}: {err.error}</li>
                                                        ))}
                                                    </ul>
                                                </div>
                                            )}
                                        </div>
                                    ) : (
                                        <div className="flex items-center gap-2 text-red-700">
                                            <AlertCircle className="h-5 w-5" />
                                            <div>
                                                <p className="font-semibold">Import Failed</p>
                                                <p className="text-sm">{uploadResult.error}</p>
                                            </div>
                                        </div>
                                    )}
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </TabsContent>
                
                {/* Test Lab Tab */}
                <TabsContent value="testlab" className="space-y-6">
                    <Card className="border-0 shadow-sm">
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <TestTube className="h-5 w-5" />
                                Connector Test Lab
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <p className="text-slate-500 mb-6">
                                Test each connector with a 5-year fetch window. Results show fetched, inserted, and versioned counts.
                            </p>
                            
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                {[
                                    { key: 'BOAMP_FR', label: 'Test BOAMP FR (5 years)' },
                                    { key: 'TED_IE', label: 'Test TED IE (5 years)' },
                                    { key: 'TED_FR', label: 'Test TED FR (5 years)' },
                                    { key: 'ETENDERS_IE', label: 'Test Ireland Source (5 years)' }
                                ].map(({ key, label }) => (
                                    <div key={key} className="p-4 border border-slate-200 rounded-xl">
                                        <div className="flex items-center justify-between mb-3">
                                            <span className="font-medium">{label}</span>
                                            <Button 
                                                size="sm"
                                                onClick={() => runConnector(key)}
                                                disabled={testResults[key]?.running}
                                            >
                                                {testResults[key]?.running ? (
                                                    <Loader2 className="h-4 w-4 animate-spin" />
                                                ) : (
                                                    <Play className="h-4 w-4" />
                                                )}
                                            </Button>
                                        </div>
                                        
                                        {testResults[key] && !testResults[key].running && (
                                            <div className={`p-3 rounded-lg text-sm ${
                                                testResults[key].success 
                                                    ? 'bg-emerald-50 text-emerald-700'
                                                    : 'bg-red-50 text-red-700'
                                            }`}>
                                                {testResults[key].success ? (
                                                    <div className="space-y-1">
                                                        <p>✓ Fetched: {testResults[key].data?.fetched_count || 0}</p>
                                                        <p>✓ Inserted: {testResults[key].data?.inserted_count || 0}</p>
                                                        <p>✓ Updated: {testResults[key].data?.updated_count || 0}</p>
                                                        <p>✓ Versioned: {testResults[key].data?.versioned_count || 0}</p>
                                                    </div>
                                                ) : (
                                                    <p>✗ Error: {testResults[key].error}</p>
                                                )}
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </CardContent>
                    </Card>
                    
                    {/* Sample Records */}
                    <Card className="border-0 shadow-sm">
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <FileText className="h-5 w-5" />
                                Latest 10 Ingested Records
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <SampleRecordsTable />
                        </CardContent>
                    </Card>
                </TabsContent>
                
                {/* Logs Tab */}
                <TabsContent value="logs" className="space-y-6">
                    <Card className="border-0 shadow-sm">
                        <CardHeader>
                            <div className="flex items-center justify-between">
                                <CardTitle className="text-lg font-semibold">Connector Run Logs</CardTitle>
                                <Button variant="outline" size="sm" onClick={loadData}>
                                    <RefreshCw className="h-4 w-4 mr-2" />
                                    Refresh
                                </Button>
                            </div>
                        </CardHeader>
                        <CardContent className="p-0">
                            <div className="overflow-x-auto">
                                <Table>
                                    <TableHeader>
                                        <TableRow className="bg-slate-50">
                                            <TableHead>Source</TableHead>
                                            <TableHead>Started</TableHead>
                                            <TableHead>Status</TableHead>
                                            <TableHead>Fetched</TableHead>
                                            <TableHead>Inserted</TableHead>
                                            <TableHead>Updated</TableHead>
                                            <TableHead>Versioned</TableHead>
                                            <TableHead>Errors</TableHead>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {runs.length === 0 ? (
                                            <TableRow>
                                                <TableCell colSpan={8} className="text-center py-8 text-slate-500">
                                                    No connector runs yet
                                                </TableCell>
                                            </TableRow>
                                        ) : (
                                            runs.slice(0, 50).map(run => (
                                                <TableRow key={run.id}>
                                                    <TableCell className="font-medium">{run.source}</TableCell>
                                                    <TableCell>
                                                        {run.started_at && format(new Date(run.started_at), 'MMM d, HH:mm')}
                                                    </TableCell>
                                                    <TableCell>
                                                        <Badge className={`${getStatusBadge(run.status)} border`}>
                                                            {run.status}
                                                        </Badge>
                                                    </TableCell>
                                                    <TableCell>{run.fetched_count || 0}</TableCell>
                                                    <TableCell className="text-emerald-600">{run.inserted_count || 0}</TableCell>
                                                    <TableCell>{run.updated_count || 0}</TableCell>
                                                    <TableCell className="text-amber-600">{run.versioned_count || 0}</TableCell>
                                                    <TableCell className="max-w-xs truncate text-red-600">
                                                        {run.error_summary || '-'}
                                                    </TableCell>
                                                </TableRow>
                                            ))
                                        )}
                                    </TableBody>
                                </Table>
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>
            </Tabs>
        </div>
    );
}

// Sample Records Component
function SampleRecordsTable() {
    const [records, setRecords] = useState([]);
    const [loading, setLoading] = useState(true);
    
    useEffect(() => {
        loadRecords();
    }, []);
    
    const loadRecords = async () => {
        try {
            const data = await civant.entities.TendersCurrent.list('-first_seen_at', 10);
            setRecords(data);
        } catch (error) {
            console.error('Error loading records:', error);
        } finally {
            setLoading(false);
        }
    };
    
    if (loading) {
        return <div className="py-8 text-center"><Loader2 className="h-6 w-6 animate-spin mx-auto" /></div>;
    }
    
    return (
        <div className="overflow-x-auto">
            <Table>
                <TableHeader>
                    <TableRow className="bg-slate-50">
                        <TableHead>Title</TableHead>
                        <TableHead>Buyer</TableHead>
                        <TableHead>Source</TableHead>
                        <TableHead>Country</TableHead>
                        <TableHead>First Seen</TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {records.length === 0 ? (
                        <TableRow>
                            <TableCell colSpan={5} className="text-center py-8 text-slate-500">
                                No records ingested yet
                            </TableCell>
                        </TableRow>
                    ) : (
                        records.map(record => (
                            <TableRow key={record.id}>
                                <TableCell className="max-w-xs truncate">{record.title}</TableCell>
                                <TableCell className="max-w-xs truncate">{record.buyer_name || '-'}</TableCell>
                                <TableCell>{record.source}</TableCell>
                                <TableCell>{record.country}</TableCell>
                                <TableCell>
                                    {record.first_seen_at && format(new Date(record.first_seen_at), 'MMM d, HH:mm')}
                                </TableCell>
                            </TableRow>
                        ))
                    )}
                </TableBody>
            </Table>
        </div>
    );
}