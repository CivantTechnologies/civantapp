import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
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
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
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

export default function Insights() {
    const [tenders, setTenders] = useState([]);
    const [versions, setVersions] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedCountry, setSelectedCountry] = useState('all');
    const [exportingToDrive, setExportingToDrive] = useState(false);
    const [selectedBuyer, setSelectedBuyer] = useState(null);
    const [aiAnalysis, setAiAnalysis] = useState(null);
    const [analyzingBuyer, setAnalyzingBuyer] = useState(false);
    
    useEffect(() => {
        loadData();
    }, []);
    
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
    
    // Notice type breakdown
    const noticeTypes = {};
    filteredTenders.forEach(tender => {
        const type = tender.notice_type || 'unknown';
        noticeTypes[type] = (noticeTypes[type] || 0) + 1;
    });
    
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
        <div className="space-y-6">
            {/* Header */}
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
                <div>
                    <h1 className="text-2xl font-bold text-slate-100">Insights</h1>
                    <p className="text-slate-400 mt-1">Buyer analytics and predictability metrics</p>
                </div>
                <div className="flex items-center gap-3">
                    <Select value={selectedCountry} onValueChange={setSelectedCountry}>
                        <SelectTrigger className="w-40">
                            <SelectValue placeholder="All countries" />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value="all">All Countries</SelectItem>
                            <SelectItem value="FR">🇫🇷 France</SelectItem>
                            <SelectItem value="IE">🇮🇪 Ireland</SelectItem>
                        </SelectContent>
                    </Select>
                    <Button variant="outline" onClick={exportCSV}>
                        <Download className="h-4 w-4 mr-2" />
                        Export CSV
                    </Button>
                    <Button 
                        variant="outline"
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
            </div>
            
            {/* Summary Stats */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-indigo-50">
                                <Building2 className="h-5 w-5 text-civant-teal" />
                            </div>
                            <div>
                                <p className="text-sm text-slate-400">Unique Buyers</p>
                                <p className="text-2xl font-bold text-slate-100">{Object.keys(buyerStats).length}</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-emerald-50">
                                <TrendingUp className="h-5 w-5 text-emerald-600" />
                            </div>
                            <div>
                                <p className="text-sm text-slate-400">Total Tenders</p>
                                <p className="text-2xl font-bold text-slate-100">{filteredTenders.length}</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-amber-50">
                                <Clock className="h-5 w-5 text-amber-600" />
                            </div>
                            <div>
                                <p className="text-sm text-slate-400">Avg Tender Window</p>
                                <p className="text-2xl font-bold text-slate-100">
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
                            <div className="p-3 rounded-xl bg-red-50">
                                <AlertCircle className="h-5 w-5 text-red-600" />
                            </div>
                            <div>
                                <p className="text-sm text-slate-400">Deadline Changes</p>
                                <p className="text-2xl font-bold text-slate-100">
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
                                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                    <XAxis 
                                        dataKey="month" 
                                        tick={{ fontSize: 12 }}
                                        tickFormatter={(v) => format(new Date(v + '-01'), 'MMM')}
                                    />
                                    <YAxis tick={{ fontSize: 12 }} />
                                    <Tooltip 
                                        contentStyle={{ 
                                            background: 'white', 
                                            border: '1px solid #e2e8f0',
                                            borderRadius: '8px'
                                        }}
                                        formatter={(value) => [value, 'Tenders']}
                                        labelFormatter={(v) => format(new Date(v + '-01'), 'MMMM yyyy')}
                                    />
                                    <Bar dataKey="count" fill="#6366f1" radius={[4, 4, 0, 0]} />
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
                            {Object.entries(noticeTypes).map(([type, count]) => {
                                const percentage = Math.round((count / filteredTenders.length) * 100);
                                const colors = {
                                    tender: 'bg-indigo-500',
                                    award: 'bg-emerald-500',
                                    corrigendum: 'bg-amber-500',
                                    unknown: 'bg-slate-400'
                                };
                                
                                return (
                                    <div key={type}>
                                        <div className="flex items-center justify-between mb-1">
                                            <span className="text-sm font-medium text-slate-300 capitalize">{type}</span>
                                            <span className="text-sm text-slate-400">{count} ({percentage}%)</span>
                                        </div>
                                        <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
                                            <div 
                                                className={`h-full ${colors[type] || colors.unknown} rounded-full transition-all`}
                                                style={{ width: `${percentage}%` }}
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
                <Card className="border-indigo-100 bg-gradient-to-br from-indigo-50 to-purple-50">
                    <CardHeader>
                        <CardTitle className="text-lg font-semibold flex items-center gap-2">
                            <Sparkles className="h-5 w-5 text-civant-teal" />
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
                                                        pred.confidence_level === 'high' ? 'bg-emerald-50 text-emerald-700' :
                                                        pred.confidence_level === 'medium' ? 'bg-amber-50 text-amber-700' :
                                                        'bg-slate-900/60 text-slate-300'
                                                    }>
                                                        {pred.confidence_score ? `${Math.round(pred.confidence_score * 100)}%` : pred.confidence_level}
                                                    </Badge>
                                                    <p className="text-xs text-slate-400 mt-1">Confidence</p>
                                                </div>
                                            </div>

                                            {/* Confidence Interval Visualization */}
                                            <div className="mb-3">
                                                <div className="h-2 bg-slate-200 rounded-full overflow-hidden">
                                                    <div 
                                                        className={`h-full rounded-full ${
                                                            pred.confidence_level === 'high' ? 'bg-emerald-500' :
                                                            pred.confidence_level === 'medium' ? 'bg-amber-500' : 'bg-slate-400'
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
                                            ? 'bg-emerald-50 text-emerald-700'
                                            : predictability === 'Medium'
                                            ? 'bg-amber-50 text-amber-700'
                                            : 'bg-red-50 text-red-700';
                                        
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
                                                        <span className="text-amber-600">{buyer.deadlineChanges}</span>
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
        </div>
    );
}
