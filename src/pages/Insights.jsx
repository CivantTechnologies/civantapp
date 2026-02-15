import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { useTenant } from '@/lib/tenant';
import { 
    Building2,
    Clock,
    TrendingUp,
    Download,
    Loader2,
    Calendar,
    AlertCircle,
    HardDrive,
    Sparkles,
    Target
} from 'lucide-react';
import {
    Page,
    PageHeader,
    PageTitle,
    PageDescription,
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    Button,
    Badge
} from '@/components/ui';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { format, differenceInDays, startOfMonth, subMonths } from 'date-fns';

const NOTICE_TYPE_META = [
    { key: 'tender', label: 'Tender', colorClass: 'bg-primary' },
    { key: 'award', label: 'Award', colorClass: 'bg-emerald-400' },
    { key: 'corrigendum', label: 'Corrigendum', colorClass: 'bg-amber-400' },
    { key: 'cancellation', label: 'Cancellation', colorClass: 'bg-rose-400' },
    { key: 'other', label: 'Other', colorClass: 'bg-slate-400' }
];

function normalizeNoticeType(rawType) {
    const value = String(rawType || '').toLowerCase().trim();
    if (!value) return 'other';
    if (value.includes('corrig') || value.includes('rectif') || value.includes('amend') || value.includes('correct')) return 'corrigendum';
    if (value.includes('award') || value.includes('adjudic')) return 'award';
    if (value.includes('cancel')) return 'cancellation';
    if (value.includes('tender') || value.includes('notice') || value.includes('contract') || value.includes('competition')) return 'tender';
    return 'other';
}

export default function Insights() {
    const [tenders, setTenders] = useState([]);
    const [versions, setVersions] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedCountry, setSelectedCountry] = useState('all');
    const [exportingToDrive, setExportingToDrive] = useState(false);
    const [selectedBuyer, setSelectedBuyer] = useState(null);
    const [aiAnalysis, setAiAnalysis] = useState(null);
    const [analyzingBuyer, setAnalyzingBuyer] = useState(false);
    const { activeTenantId, isLoadingTenants } = useTenant();
    
    useEffect(() => {
        if (isLoadingTenants) return;
        if (!activeTenantId) return;
        setLoading(true);
        void loadData();
    }, [activeTenantId, isLoadingTenants]);
    
    const loadData = async () => {
        try {
            const [tendersData, versionsData] = await Promise.all([
                civant.entities.TendersCurrent.list('-publication_date', 1000),
                civant.entities.TenderVersions.list('-change_date', 500)
            ]);
            
            setTenders(tendersData);
            setVersions(versionsData);
        } catch (error) {
            console.error('Error loading data:', error);
        } finally {
            setLoading(false);
        }
    };
    
    // Filter by country
    const filteredTenders = selectedCountry === 'all' 
        ? tenders 
        : tenders.filter(t => t.country === selectedCountry);
    
    // Buyer analytics
    const buyerStats = {};
    filteredTenders.forEach(tender => {
        const buyer = tender.buyer_name || 'Unknown';
        if (!buyerStats[buyer]) {
            buyerStats[buyer] = {
                name: buyer,
                tenderCount: 0,
                totalValue: 0,
                avgWindow: [],
                deadlineChanges: 0
            };
        }
        buyerStats[buyer].tenderCount++;
        
        if (tender.estimated_value) {
            buyerStats[buyer].totalValue += tender.estimated_value;
        }
        
        // Calculate tender window (deadline - publication)
        if (tender.publication_date && tender.deadline_date) {
            const window = differenceInDays(
                new Date(tender.deadline_date),
                new Date(tender.publication_date)
            );
            if (window > 0 && window < 365) {
                buyerStats[buyer].avgWindow.push(window);
            }
        }
    });
    
    // Count deadline changes per buyer
    versions.filter(v => v.change_type === 'deadline_changed').forEach(version => {
        const tender = tenders.find(t => t.tender_uid === version.tender_uid);
        if (tender) {
            const buyer = tender.buyer_name || 'Unknown';
            if (buyerStats[buyer]) {
                buyerStats[buyer].deadlineChanges++;
            }
        }
    });
    
    // Calculate averages and sort by tender count
    const topBuyers = Object.values(buyerStats)
        .map(b => ({
            ...b,
            avgWindow: b.avgWindow.length > 0 
                ? Math.round(b.avgWindow.reduce((a, c) => a + c, 0) / b.avgWindow.length)
                : null
        }))
        .sort((a, b) => b.tenderCount - a.tenderCount)
        .slice(0, 20);
    
    // Monthly trends
    const monthlyData = {};
    const last6Months = Array.from({ length: 6 }, (_, i) => {
        const date = subMonths(new Date(), 5 - i);
        return format(startOfMonth(date), 'yyyy-MM');
    });
    
    last6Months.forEach(month => {
        monthlyData[month] = { month, count: 0, value: 0 };
    });
    
    filteredTenders.forEach(tender => {
        if (tender.publication_date) {
            const month = tender.publication_date.substring(0, 7);
            if (monthlyData[month]) {
                monthlyData[month].count++;
                if (tender.estimated_value) {
                    monthlyData[month].value += tender.estimated_value;
                }
            }
        }
    });
    
    const monthlyChartData = Object.values(monthlyData);
    
    // Notice type breakdown (normalized so Corrigendum is always visible)
    const noticeTypeCounts = NOTICE_TYPE_META.reduce((acc, item) => {
        acc[item.key] = 0;
        return acc;
    }, {});
    filteredTenders.forEach((tender) => {
        const typeKey = normalizeNoticeType(tender.notice_type);
        noticeTypeCounts[typeKey] = (noticeTypeCounts[typeKey] || 0) + 1;
    });
    const noticeTypeStats = NOTICE_TYPE_META.map((item) => ({
        ...item,
        count: noticeTypeCounts[item.key] || 0,
        percentage: filteredTenders.length > 0 ? Math.round(((noticeTypeCounts[item.key] || 0) / filteredTenders.length) * 100) : 0
    }));
    
    // Export to Google Drive
    const handleExportToDrive = async () => {
        setExportingToDrive(true);
        try {
            const filters = selectedCountry !== 'all' ? { country: selectedCountry } : null;
            const response = await civant.functions.invoke('exportToDrive', {
                report_type: 'insights',
                filters
            });
            
            if (response.data.success) {
                const openDrive = confirm(
                    `Successfully exported ${response.data.record_count} tenders to Google Drive!\n\n` +
                    `File: ${response.data.file_name}\n\n` +
                    `Click OK to open the file in Google Drive.`
                );
                if (openDrive) {
                    window.open(response.data.file_link, '_blank');
                }
            } else {
                alert('Failed to export to Drive: ' + response.data.error);
            }
        } catch (error) {
            alert('Error: ' + error.message + '\n\nPlease ensure Google Drive integration is authorized by your admin.');
        } finally {
            setExportingToDrive(false);
        }
    };
    
    const analyzeSpecificBuyer = async (buyerName, country) => {
        setAnalyzingBuyer(true);
        setSelectedBuyer(buyerName);
        
        try {
            const response = await civant.functions.invoke('predictTenders', {
                buyer_name: buyerName,
                country: country
            });
            
            if (response.data.success) {
                setAiAnalysis(response.data);
            }
        } catch (error) {
            console.error('Analysis failed:', error);
            setAiAnalysis(null);
        } finally {
            setAnalyzingBuyer(false);
        }
    };
    
    // Export to CSV
    const exportCSV = () => {
        const headers = ['Buyer', 'Tender Count', 'Total Value (EUR)', 'Avg Window (days)', 'Deadline Changes'];
        const rows = topBuyers.map(b => [
            b.name,
            b.tenderCount,
            b.totalValue.toFixed(2),
            b.avgWindow || '-',
            b.deadlineChanges
        ]);
        
        const csv = [headers.join(','), ...rows.map(r => r.map(c => `"${c}"`).join(','))].join('\n');
        
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `buyer-analytics-${format(new Date(), 'yyyy-MM-dd')}.csv`;
        a.click();
        window.URL.revokeObjectURL(url);
    };
    
    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-civant-teal" />
            </div>
        );
    }
    
    return (
        <Page className="space-y-8">
            {/* Header */}
            <PageHeader className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-4">
                <div className="space-y-3">
                    <span className="inline-flex w-fit rounded-full border border-primary/30 bg-primary/15 px-3 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-primary">
                        Civant Intelligence
                    </span>
                    <div className="space-y-2">
                        <PageTitle>Insights</PageTitle>
                        <PageDescription>Buyer analytics and predictability metrics.</PageDescription>
                    </div>
                </div>
                <div className="flex flex-wrap items-center gap-3">
                    <Select value={selectedCountry} onValueChange={setSelectedCountry}>
                        <SelectTrigger className="w-full sm:w-44 bg-card/70 border-border text-card-foreground">
                            <SelectValue placeholder="All countries" />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value="all">All Countries</SelectItem>
                            <SelectItem value="FR">🇫🇷 France</SelectItem>
                            <SelectItem value="IE">🇮🇪 Ireland</SelectItem>
                            <SelectItem value="ES">🇪🇸 Spain</SelectItem>
                        </SelectContent>
                    </Select>
                    <Button variant="outline" className="border-border/80 bg-card/50 hover:bg-card/75" onClick={exportCSV}>
                        <Download className="h-4 w-4 mr-2" />
                        Export CSV
                    </Button>
                    <Button 
                        variant="outline"
                        className="border-border/80 bg-card/50 hover:bg-card/75"
                        onClick={handleExportToDrive}
                        disabled={exportingToDrive}
                    >
                        {exportingToDrive ? (
                            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                        ) : (
                            <HardDrive className="h-4 w-4 mr-2" />
                        )}
                        Export to Drive
                    </Button>
                </div>
            </PageHeader>
            
            {/* Summary Stats */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-primary/15 border border-primary/25">
                                <Building2 className="h-5 w-5 text-primary" />
                            </div>
                            <div>
                                <p className="text-sm text-muted-foreground">Unique Buyers</p>
                                <p className="text-2xl font-bold text-card-foreground">{Object.keys(buyerStats).length}</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-emerald-500/15 border border-emerald-400/30">
                                <TrendingUp className="h-5 w-5 text-emerald-300" />
                            </div>
                            <div>
                                <p className="text-sm text-muted-foreground">Total Tenders</p>
                                <p className="text-2xl font-bold text-card-foreground">{filteredTenders.length}</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-amber-500/15 border border-amber-400/30">
                                <Clock className="h-5 w-5 text-amber-300" />
                            </div>
                            <div>
                                <p className="text-sm text-muted-foreground">Avg Tender Window</p>
                                <p className="text-2xl font-bold text-card-foreground">
                                    {(() => {
                                        const allWindows = topBuyers.flatMap(b => b.avgWindow ? [b.avgWindow] : []);
                                        return allWindows.length > 0 
                                            ? Math.round(allWindows.reduce((a, c) => a + c, 0) / allWindows.length) + ' days'
                                            : '-';
                                    })()}
                                </p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-rose-500/15 border border-rose-400/30">
                                <AlertCircle className="h-5 w-5 text-rose-300" />
                            </div>
                            <div>
                                <p className="text-sm text-muted-foreground">Deadline Changes</p>
                                <p className="text-2xl font-bold text-card-foreground">
                                    {versions.filter(v => v.change_type === 'deadline_changed').length}
                                </p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </div>
            
            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Monthly Trend */}
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-lg font-semibold">Monthly Tender Volume</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-64">
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={monthlyChartData}>
                                    <CartesianGrid strokeDasharray="4 4" stroke="hsl(var(--border))" />
                                    <XAxis 
                                        dataKey="month" 
                                        tick={{ fontSize: 12, fill: '#94a3b8' }}
                                        axisLine={{ stroke: 'hsl(var(--border))' }}
                                        tickLine={{ stroke: 'hsl(var(--border))' }}
                                        tickFormatter={(v) => format(new Date(v + '-01'), 'MMM')}
                                    />
                                    <YAxis
                                        tick={{ fontSize: 12, fill: '#94a3b8' }}
                                        axisLine={{ stroke: 'hsl(var(--border))' }}
                                        tickLine={{ stroke: 'hsl(var(--border))' }}
                                    />
                                    <Tooltip 
                                        contentStyle={{ 
                                            background: 'hsl(var(--card))', 
                                            border: '1px solid hsl(var(--border))',
                                            borderRadius: '10px'
                                        }}
                                        labelStyle={{ color: 'hsl(var(--card-foreground))' }}
                                        formatter={(value) => [value, 'Tenders']}
                                        labelFormatter={(v) => format(new Date(v + '-01'), 'MMMM yyyy')}
                                    />
                                    <Bar dataKey="count" fill="hsl(var(--primary))" radius={[6, 6, 0, 0]} />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </CardContent>
                </Card>
                
                {/* Notice Type Breakdown */}
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-lg font-semibold">Notice Types</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-4">
                            {noticeTypeStats.map((item) => {
                                return (
                                    <div key={item.key}>
                                        <div className="flex items-center justify-between mb-1">
                                            <span className="text-sm font-medium text-slate-300">{item.label}</span>
                                            <span className="text-sm text-slate-400">{item.count} ({item.percentage}%)</span>
                                        </div>
                                        <div className="h-2 bg-muted/50 rounded-full overflow-hidden">
                                            <div 
                                                className={`h-full ${item.colorClass} rounded-full transition-all`}
                                                style={{ width: `${item.percentage}%` }}
                                            />
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </CardContent>
                </Card>
            </div>
            
            {/* AI Analysis Panel */}
            {aiAnalysis && (
                <Card className="border border-civant-border bg-civant-navy/60 shadow-none">
                    <CardHeader>
                        <CardTitle className="text-lg font-semibold flex items-center gap-2">
                            <Sparkles className="h-5 w-5 text-primary" />
                            AI Forecast Analysis - {selectedBuyer}
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        {/* Analysis Summary */}
                        {aiAnalysis.analysis && (
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                                <div className="bg-slate-900/70 p-4 rounded-lg">
                                    <p className="text-xs text-slate-400">Avg Interval</p>
                                    <p className="text-xl font-bold text-slate-100">{Math.round(aiAnalysis.analysis.avg_interval_days)} days</p>
                                </div>
                                <div className="bg-slate-900/70 p-4 rounded-lg">
                                    <p className="text-xs text-slate-400">Seasonality</p>
                                    <p className="text-xl font-bold text-slate-100">
                                        {aiAnalysis.analysis.seasonality_detected ? 'Yes' : 'No'}
                                    </p>
                                </div>
                                <div className="bg-slate-900/70 p-4 rounded-lg">
                                    <p className="text-xs text-slate-400">Trend</p>
                                    <p className="text-xl font-bold text-slate-100 capitalize">{aiAnalysis.analysis.trend}</p>
                                </div>
                                <div className="bg-slate-900/70 p-4 rounded-lg">
                                    <p className="text-xs text-slate-400">Data Quality</p>
                                    <p className="text-xl font-bold text-slate-100 capitalize">{aiAnalysis.analysis.data_quality}</p>
                                </div>
                            </div>
                        )}

                        {/* Predictions with Confidence Intervals */}
                        {aiAnalysis.predictions && aiAnalysis.predictions.length > 0 && (
                            <div>
                                <h4 className="text-sm font-semibold text-slate-100 mb-3 flex items-center gap-2">
                                    <Target className="h-4 w-4" />
                                    Predicted Tenders
                                </h4>
                                <div className="grid gap-3">
                                    {aiAnalysis.predictions.map((pred, idx) => (
                                        <div key={idx} className="bg-slate-900/70 p-4 rounded-lg">
                                            <div className="flex items-start justify-between mb-3">
                                                <div>
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <Calendar className="h-4 w-4 text-slate-400" />
                                                        <span className="font-semibold text-slate-100">
                                                            {format(new Date(pred.predicted_date), 'MMMM d, yyyy')}
                                                        </span>
                                                    </div>
                                                    {pred.estimated_value_range && (
                                                        <p className="text-sm text-slate-300">
                                                            Expected Value: {new Intl.NumberFormat('en', { style: 'currency', currency: 'EUR', notation: 'compact' }).format(pred.estimated_value_range.min)} - {new Intl.NumberFormat('en', { style: 'currency', currency: 'EUR', notation: 'compact' }).format(pred.estimated_value_range.max)}
                                                        </p>
                                                    )}
                                                </div>
                                                <div className="text-right">
                                                    <Badge className={
                                                        pred.confidence_level === 'high' ? 'bg-emerald-500/15 text-emerald-200 border border-emerald-400/40' :
                                                        pred.confidence_level === 'medium' ? 'bg-amber-500/15 text-amber-200 border border-amber-400/40' :
                                                        'bg-secondary text-secondary-foreground border border-border/70'
                                                    }>
                                                        {pred.confidence_score ? `${Math.round(pred.confidence_score * 100)}%` : pred.confidence_level}
                                                    </Badge>
                                                    <p className="text-xs text-slate-400 mt-1">Confidence</p>
                                                </div>
                                            </div>

                                            {/* Confidence Interval Visualization */}
                                            <div className="mb-3">
                                                <div className="h-2 bg-muted/50 rounded-full overflow-hidden">
                                                    <div 
                                                        className={`h-full rounded-full ${
                                                            pred.confidence_level === 'high' ? 'bg-emerald-400' :
                                                            pred.confidence_level === 'medium' ? 'bg-amber-400' : 'bg-slate-400'
                                                        }`}
                                                        style={{ width: `${(pred.confidence_score || 0.5) * 100}%` }}
                                                    />
                                                </div>
                                            </div>

                                            {pred.seasonality_factor && (
                                                <p className="text-sm text-slate-300 mb-2">
                                                    <span className="font-medium">Seasonality:</span> {pred.seasonality_factor}
                                                </p>
                                            )}

                                            {pred.expected_cpv_codes && pred.expected_cpv_codes.length > 0 && (
                                                <div className="mb-2">
                                                    <p className="text-xs text-slate-400 mb-1">Expected Categories:</p>
                                                    <div className="flex flex-wrap gap-1">
                                                        {pred.expected_cpv_codes.map((cpv, i) => (
                                                            <Badge key={i} variant="outline" className="text-xs">
                                                                {cpv}
                                                            </Badge>
                                                        ))}
                                                    </div>
                                                </div>
                                            )}

                                            {pred.key_indicators && pred.key_indicators.length > 0 && (
                                                <div>
                                                    <p className="text-xs font-medium text-slate-300 mb-1">Key Indicators:</p>
                                                    <ul className="text-xs text-slate-300 space-y-1">
                                                        {pred.key_indicators.map((indicator, i) => (
                                                            <li key={i}>• {indicator}</li>
                                                        ))}
                                                    </ul>
                                                </div>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        <Button 
                            variant="outline" 
                            size="sm"
                            onClick={() => {
                                setAiAnalysis(null);
                                setSelectedBuyer(null);
                            }}
                        >
                            Close Analysis
                        </Button>
                    </CardContent>
                </Card>
            )}

            {/* Top Buyers Table */}
            <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                <CardHeader>
                    <CardTitle className="text-lg font-semibold flex items-center gap-2">
                        <Building2 className="h-5 w-5" />
                        Buyer Analytics
                    </CardTitle>
                </CardHeader>
                <CardContent className="p-0">
                    <div className="overflow-x-auto">
                        <Table>
                            <TableHeader>
                                <TableRow className="bg-slate-900/60">
                                    <TableHead>Buyer</TableHead>
                                    <TableHead className="text-right">Tender Count</TableHead>
                                    <TableHead className="text-right">Total Value</TableHead>
                                    <TableHead className="text-right">Avg Window</TableHead>
                                    <TableHead className="text-right">Deadline Changes</TableHead>
                                    <TableHead className="text-right">Predictability</TableHead>
                                    <TableHead className="text-right">Actions</TableHead>
                                    </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                    {topBuyers.length === 0 ? (
                                    <TableRow>
                                        <TableCell colSpan={7} className="text-center py-8 text-slate-400">
                                            No data available
                                        </TableCell>
                                    </TableRow>
                                    ) : (
                                    topBuyers.map((buyer, index) => {
                                        // Calculate predictability score
                                        const changeRate = buyer.tenderCount > 0 
                                            ? (buyer.deadlineChanges / buyer.tenderCount) 
                                            : 0;
                                        const predictability = changeRate < 0.1 ? 'High' 
                                            : changeRate < 0.3 ? 'Medium' 
                                            : 'Low';
                                        const predColor = predictability === 'High' 
                                            ? 'bg-emerald-500/15 text-emerald-200 border border-emerald-400/40'
                                            : predictability === 'Medium'
                                            ? 'bg-amber-500/15 text-amber-200 border border-amber-400/40'
                                            : 'bg-rose-500/15 text-rose-200 border border-rose-400/40';
                                        
                                        return (
                                            <TableRow key={index}>
                                                <TableCell className="font-medium max-w-xs truncate">
                                                    {buyer.name}
                                                </TableCell>
                                                <TableCell className="text-right">{buyer.tenderCount}</TableCell>
                                                <TableCell className="text-right">
                                                    {buyer.totalValue > 0 
                                                        ? new Intl.NumberFormat('en', { 
                                                            style: 'currency', 
                                                            currency: 'EUR',
                                                            maximumFractionDigits: 0,
                                                            notation: 'compact'
                                                        }).format(buyer.totalValue)
                                                        : '-'
                                                    }
                                                </TableCell>
                                                <TableCell className="text-right">
                                                    {buyer.avgWindow ? `${buyer.avgWindow} days` : '-'}
                                                </TableCell>
                                                <TableCell className="text-right">
                                                    {buyer.deadlineChanges > 0 ? (
                                                        <span className="text-amber-300">{buyer.deadlineChanges}</span>
                                                    ) : (
                                                        <span className="text-slate-400">0</span>
                                                    )}
                                                </TableCell>
                                                <TableCell className="text-right">
                                                    <Badge className={predColor}>
                                                        {predictability}
                                                    </Badge>
                                                </TableCell>
                                                <TableCell className="text-right">
                                                    <Button
                                                        variant="ghost"
                                                        size="sm"
                                                        onClick={() => {
                                                            const tender = filteredTenders.find(t => t.buyer_name === buyer.name);
                                                            analyzeSpecificBuyer(buyer.name, tender?.country);
                                                        }}
                                                        disabled={analyzingBuyer && selectedBuyer === buyer.name}
                                                    >
                                                        {analyzingBuyer && selectedBuyer === buyer.name ? (
                                                            <Loader2 className="h-4 w-4 animate-spin" />
                                                        ) : (
                                                            <>
                                                                <Sparkles className="h-4 w-4 mr-1" />
                                                                AI Forecast
                                                            </>
                                                        )}
                                                    </Button>
                                                </TableCell>
                                                </TableRow>
                                                );
                                                })
                                                )}
                                                </TableBody>
                        </Table>
                    </div>
                </CardContent>
            </Card>
        </Page>
    );
}
