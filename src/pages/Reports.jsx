import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { supabase } from '@/lib/supabaseClient';
import { useTenant } from '@/lib/tenant';
import {
  FileText,
  Download,
  Loader2,
  BarChart3,
  Users,
  TrendingUp,
  Target,
  ChevronDown,
  ChevronRight,
} from 'lucide-react';
import {
  Page,
  PageBody,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Button,
  Badge,
} from '@/components/ui';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
} from 'recharts';
import { format, parseISO } from 'date-fns';

const COUNTRY_OPTIONS = [
  { value: 'all', label: 'All Countries' },
  { value: 'IE', label: 'Ireland' },
  { value: 'FR', label: 'France' },
  { value: 'ES', label: 'Spain' },
];

const PERIOD_OPTIONS = [
  { value: '6', label: 'Last 6 Months' },
  { value: '12', label: 'Last 12 Months' },
  { value: '24', label: 'Last 24 Months' },
];

const URGENCY_OPTIONS = [
  { value: 'all', label: 'All Urgencies' },
  { value: 'upcoming', label: 'Upcoming (0-90 days)' },
  { value: 'horizon', label: 'Horizon (90-180 days)' },
  { value: 'distant', label: 'Distant (180+ days)' },
];

const CHART_COLORS = ['#10b981', '#06b6d4', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#6366f1', '#14b8a6'];

const FLAG = { IE: '\u{1F1EE}\u{1F1EA}', FR: '\u{1F1EB}\u{1F1F7}', ES: '\u{1F1EA}\u{1F1F8}' };

function formatValue(v) {
  if (!v || isNaN(v)) return '\u2014';
  const n = Number(v);
  if (n >= 1_000_000) return `\u20AC${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `\u20AC${(n / 1_000).toFixed(0)}K`;
  return `\u20AC${n.toFixed(0)}`;
}

function downloadCSV(filename, headers, rows) {
  const csv = [
    headers.join(','),
    ...rows.map((r) => r.map((c) => `"${String(c ?? '').replace(/"/g, '""')}"`).join(',')),
  ].join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${filename}-${format(new Date(), 'yyyy-MM-dd')}.csv`;
  a.click();
  window.URL.revokeObjectURL(url);
}

// ---- Market Overview Tab ----
function MarketOverview({ tenantId }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [country, setCountry] = useState('all');
  const [period, setPeriod] = useState('12');

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const { data: result, error } = await supabase.rpc('get_report_market_overview', {
        p_tenant_id: tenantId,
        p_country: country === 'all' ? null : country,
        p_months_back: parseInt(period),
      });
      if (!error && result) setData(result);
    } catch (e) {
      console.error('Market overview error:', e);
    } finally {
      setLoading(false);
    }
  }, [tenantId, country, period]);

  useEffect(() => { if (tenantId) load(); }, [load, tenantId]);

  const monthlyChartData = useMemo(() => {
    if (!data?.by_month) return [];
    const map = {};
    data.by_month.forEach((m) => {
      if (!map[m.month]) map[m.month] = { month: m.month };
      map[m.month][m.country] = m.tender_count;
    });
    return Object.values(map).sort((a, b) => a.month.localeCompare(b.month));
  }, [data]);

  const categoryChartData = useMemo(() => {
    if (!data?.by_category) return [];
    return data.by_category.slice(0, 10).map((c) => ({
      name: c.category.length > 25 ? c.category.slice(0, 22) + '...' : c.category,
      value: c.tender_count,
    }));
  }, [data]);

  const exportMarketCSV = () => {
    if (!data?.by_month) return;
    downloadCSV(
      'market-overview',
      ['Month', 'Country', 'Tender Count'],
      data.by_month.map((m) => [m.month, m.country, m.tender_count])
    );
  };

  const exportBuyersCSV = () => {
    if (!data?.top_buyers) return;
    downloadCSV(
      'top-buyers',
      ['Buyer', 'Country', 'Tender Count', 'Avg Value (EUR)', 'Last Tender'],
      data.top_buyers.map((b) => [b.buyer_name, b.country, b.tender_count, b.avg_value, b.last_tender])
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48">
        <Loader2 className="h-6 w-6 animate-spin text-emerald-400" />
        <span className="ml-2 text-sm text-muted-foreground">Loading market data...</span>
      </div>
    );
  }

  if (!data) return <p className="text-sm text-muted-foreground">No data available</p>;

  const s = data.summary;
  return (
    <div className="space-y-6">
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <Select value={country} onValueChange={setCountry}>
          <SelectTrigger className="h-8 w-[150px] border-white/[0.08] bg-white/[0.02] text-xs text-slate-300">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>{COUNTRY_OPTIONS.map((o) => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
        </Select>
        <Select value={period} onValueChange={setPeriod}>
          <SelectTrigger className="h-8 w-[160px] border-white/[0.08] bg-white/[0.02] text-xs text-slate-300">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>{PERIOD_OPTIONS.map((o) => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
        </Select>
        <Button variant="ghost" size="sm" className="text-xs text-emerald-400 hover:text-emerald-300 ml-auto" onClick={exportMarketCSV}>
          <Download className="h-3 w-3 mr-1" /> Export CSV
        </Button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Tenders Published</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{s.total_tenders?.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Unique Buyers</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{s.total_buyers?.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Countries</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{s.countries}</p>
          </CardContent>
        </Card>
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Avg Tender Value</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{formatValue(s.avg_estimated_value)}</p>
          </CardContent>
        </Card>
      </div>

      {/* Country Breakdown */}
      {data.by_country?.length > 1 ? (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-sm font-semibold">By Country</CardTitle></CardHeader>
          <CardContent>
            <div className="divide-y divide-white/[0.06]">
              {data.by_country.map((c) => (
                <div key={c.country} className="flex items-center justify-between py-2 text-sm">
                  <span className="text-slate-300">{FLAG[c.country] || ''} {c.country}</span>
                  <div className="flex items-center gap-6 text-xs text-muted-foreground">
                    <span>{c.tender_count?.toLocaleString()} tenders</span>
                    <span>{c.buyer_count?.toLocaleString()} buyers</span>
                    <span>{formatValue(c.avg_value)} avg</span>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      ) : null}

      {/* Monthly Volume Chart */}
      {monthlyChartData.length > 0 ? (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-sm font-semibold">Monthly Tender Volume</CardTitle></CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={monthlyChartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                <XAxis dataKey="month" tick={{ fontSize: 10, fill: '#94a3b8' }} tickFormatter={(v) => v.slice(2)} />
                <YAxis tick={{ fontSize: 10, fill: '#94a3b8' }} />
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid rgba(255,255,255,0.1)', borderRadius: 8, fontSize: 12 }} />
                <Bar dataKey="FR" stackId="a" fill="#10b981" radius={[0, 0, 0, 0]} />
                <Bar dataKey="ES" stackId="a" fill="#06b6d4" />
                <Bar dataKey="IE" stackId="a" fill="#f59e0b" radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      ) : null}

      {/* Top Categories */}
      {categoryChartData.length > 0 ? (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-sm font-semibold">Top Categories</CardTitle></CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={categoryChartData} layout="vertical" margin={{ left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                <XAxis type="number" tick={{ fontSize: 10, fill: '#94a3b8' }} />
                <YAxis type="category" dataKey="name" tick={{ fontSize: 10, fill: '#94a3b8' }} width={140} />
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid rgba(255,255,255,0.1)', borderRadius: 8, fontSize: 12 }} />
                <Bar dataKey="value" fill="#10b981" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      ) : null}

      {/* Top Buyers Table */}
      {data.top_buyers?.length > 0 ? (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <CardTitle className="text-sm font-semibold">Top Buyers</CardTitle>
            <Button variant="ghost" size="sm" className="text-xs text-emerald-400 hover:text-emerald-300" onClick={exportBuyersCSV}>
              <Download className="h-3 w-3 mr-1" /> CSV
            </Button>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow className="border-white/[0.06]">
                  <TableHead className="text-[10px] uppercase text-muted-foreground">Buyer</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground">Country</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Tenders</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Avg Value</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Last Active</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.top_buyers.slice(0, 20).map((b, i) => (
                  <TableRow key={i} className="border-white/[0.04]">
                    <TableCell className="text-xs text-slate-300 max-w-[250px] truncate">{b.buyer_name}</TableCell>
                    <TableCell className="text-xs text-muted-foreground">{FLAG[b.country] || ''} {b.country}</TableCell>
                    <TableCell className="text-xs text-slate-300 text-right tabular-nums">{b.tender_count}</TableCell>
                    <TableCell className="text-xs text-muted-foreground text-right tabular-nums">{formatValue(b.avg_value)}</TableCell>
                    <TableCell className="text-xs text-muted-foreground text-right">{b.last_tender}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}

// ---- Pipeline Report Tab ----
function PipelineReport({ tenantId }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [country, setCountry] = useState('all');
  const [urgency, setUrgency] = useState('all');

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const { data: result, error } = await supabase.rpc('get_report_pipeline', {
        p_tenant_id: tenantId,
        p_country: country === 'all' ? null : country,
        p_urgency: urgency === 'all' ? null : urgency,
        p_limit: 500,
      });
      if (!error && result) setData(result);
    } catch (e) {
      console.error('Pipeline report error:', e);
    } finally {
      setLoading(false);
    }
  }, [tenantId, country, urgency]);

  useEffect(() => { if (tenantId) load(); }, [load, tenantId]);

  const exportPipelineCSV = () => {
    if (!data?.predictions) return;
    downloadCSV(
      'pipeline-report',
      ['Buyer', 'Country', 'Category', 'Urgency', 'Window Start', 'Window End', 'Probability', 'Confidence', 'Confidence Band'],
      data.predictions.map((p) => [
        p.buyer_name, p.country, p.category, p.urgency,
        p.expected_window_start, p.expected_window_end,
        p.probability, p.confidence, p.confidence_band,
      ])
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48">
        <Loader2 className="h-6 w-6 animate-spin text-emerald-400" />
        <span className="ml-2 text-sm text-muted-foreground">Loading pipeline data...</span>
      </div>
    );
  }

  if (!data) return <p className="text-sm text-muted-foreground">No data available</p>;

  const s = data.summary;
  return (
    <div className="space-y-6">
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <Select value={country} onValueChange={setCountry}>
          <SelectTrigger className="h-8 w-[150px] border-white/[0.08] bg-white/[0.02] text-xs text-slate-300">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>{COUNTRY_OPTIONS.map((o) => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
        </Select>
        <Select value={urgency} onValueChange={setUrgency}>
          <SelectTrigger className="h-8 w-[180px] border-white/[0.08] bg-white/[0.02] text-xs text-slate-300">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>{URGENCY_OPTIONS.map((o) => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
        </Select>
        <Button variant="ghost" size="sm" className="text-xs text-emerald-400 hover:text-emerald-300 ml-auto" onClick={exportPipelineCSV}>
          <Download className="h-3 w-3 mr-1" /> Export CSV
        </Button>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Total Predictions</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{s.total_predictions?.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="border border-emerald-500/20 bg-emerald-500/[0.06] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-emerald-400/80">Upcoming</p>
            <p className="text-2xl font-semibold text-emerald-400 tabular-nums">{s.upcoming?.toLocaleString()}</p>
            <p className="text-[10px] text-muted-foreground">Next 90 days</p>
          </CardContent>
        </Card>
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Horizon</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{s.horizon?.toLocaleString()}</p>
            <p className="text-[10px] text-muted-foreground">90-180 days</p>
          </CardContent>
        </Card>
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">High Confidence</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{s.high_confidence?.toLocaleString()}</p>
            <p className="text-[10px] text-muted-foreground">Confidence 60%+</p>
          </CardContent>
        </Card>
      </div>

      {/* Predictions Table */}
      {data.predictions?.length > 0 ? (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-semibold">
              Predicted Opportunities ({data.predictions.length}{data.predictions.length >= 500 ? '+' : ''})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow className="border-white/[0.06]">
                    <TableHead className="text-[10px] uppercase text-muted-foreground">Buyer</TableHead>
                    <TableHead className="text-[10px] uppercase text-muted-foreground">Country</TableHead>
                    <TableHead className="text-[10px] uppercase text-muted-foreground">Category</TableHead>
                    <TableHead className="text-[10px] uppercase text-muted-foreground">Urgency</TableHead>
                    <TableHead className="text-[10px] uppercase text-muted-foreground">Window</TableHead>
                    <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Confidence</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.predictions.slice(0, 100).map((p, i) => (
                    <TableRow key={i} className="border-white/[0.04]">
                      <TableCell className="text-xs text-slate-300 max-w-[200px] truncate">{p.buyer_name}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{FLAG[p.country] || ''} {p.country}</TableCell>
                      <TableCell className="text-xs text-muted-foreground max-w-[120px] truncate">{p.category}</TableCell>
                      <TableCell>
                        <Badge variant="outline" className={
                          p.urgency === 'upcoming' ? 'border-emerald-500/40 text-emerald-400 text-[10px]' :
                          p.urgency === 'horizon' ? 'border-cyan-500/40 text-cyan-400 text-[10px]' :
                          'border-white/[0.1] text-muted-foreground text-[10px]'
                        }>
                          {p.urgency}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground tabular-nums">
                        {p.expected_window_start} to {p.expected_window_end}
                      </TableCell>
                      <TableCell className="text-xs text-slate-300 text-right tabular-nums">{p.confidence}%</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
            {data.predictions.length > 100 ? (
              <p className="text-[10px] text-muted-foreground mt-3 text-center">
                Showing 100 of {data.predictions.length} results. Export CSV for full dataset.
              </p>
            ) : null}
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}

// ---- Validation Report Tab ----
function ValidationReport({ tenantId }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!tenantId) return;
    (async () => {
      setLoading(true);
      try {
        const { data: result, error } = await supabase.rpc('get_prediction_validation_stats', { p_tenant_id: tenantId });
        if (!error && result) setData(result);
      } catch (e) {
        console.error('Validation stats error:', e);
      } finally {
        setLoading(false);
      }
    })();
  }, [tenantId]);

  const exportValidationCSV = () => {
    if (!data?.accuracy_by_country) return;
    downloadCSV(
      'validation-report',
      ['Country', 'Total Resolved', 'Confirmed', 'Expired', 'Accuracy (%)'],
      data.accuracy_by_country.map((c) => [c.country, c.total, c.confirmed, c.expired || 0, c.accuracy])
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48">
        <Loader2 className="h-6 w-6 animate-spin text-emerald-400" />
      </div>
    );
  }

  if (!data) return <p className="text-sm text-muted-foreground">No data available</p>;

  return (
    <div className="space-y-6">
      <div className="flex justify-end">
        <Button variant="ghost" size="sm" className="text-xs text-emerald-400 hover:text-emerald-300" onClick={exportValidationCSV}>
          <Download className="h-3 w-3 mr-1" /> Export CSV
        </Button>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Card className="border border-emerald-500/20 bg-emerald-500/[0.06] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-emerald-400/80">Overall Accuracy</p>
            <p className="text-2xl font-bold text-emerald-400 tabular-nums">{data.accuracy_resolved}%</p>
            <p className="text-[10px] text-muted-foreground">{data.confirmed?.toLocaleString()} confirmed</p>
          </CardContent>
        </Card>
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Confirmed Hits</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{data.confirmed?.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Expired / Missed</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">{data.expired?.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
          <CardContent className="p-4">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Median Timing</p>
            <p className="text-2xl font-semibold text-card-foreground tabular-nums">\u00B1{data.median_delta_days}d</p>
          </CardContent>
        </Card>
      </div>

      {/* By Country */}
      {data.accuracy_by_country?.length > 0 ? (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-sm font-semibold">Accuracy by Country</CardTitle></CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow className="border-white/[0.06]">
                  <TableHead className="text-[10px] uppercase text-muted-foreground">Country</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Total Resolved</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Confirmed</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Expired</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Accuracy</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.accuracy_by_country.map((c) => (
                  <TableRow key={c.country} className="border-white/[0.04]">
                    <TableCell className="text-xs text-slate-300">{FLAG[c.country] || ''} {c.country}</TableCell>
                    <TableCell className="text-xs text-muted-foreground text-right tabular-nums">{c.total?.toLocaleString()}</TableCell>
                    <TableCell className="text-xs text-emerald-400 text-right tabular-nums">{c.confirmed?.toLocaleString()}</TableCell>
                    <TableCell className="text-xs text-muted-foreground text-right tabular-nums">{(c.expired || 0).toLocaleString()}</TableCell>
                    <TableCell className="text-xs text-right tabular-nums font-medium text-emerald-400">{c.accuracy}%</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      ) : null}

      {/* By Urgency */}
      {data.accuracy_by_urgency?.length > 0 ? (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-sm font-semibold">Accuracy by Urgency Window</CardTitle></CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow className="border-white/[0.06]">
                  <TableHead className="text-[10px] uppercase text-muted-foreground">Window</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Total</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Confirmed</TableHead>
                  <TableHead className="text-[10px] uppercase text-muted-foreground text-right">Hit Rate</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.accuracy_by_urgency.map((u) => (
                  <TableRow key={u.urgency} className="border-white/[0.04]">
                    <TableCell className="text-xs text-slate-300 capitalize">{u.urgency}</TableCell>
                    <TableCell className="text-xs text-muted-foreground text-right tabular-nums">{u.total?.toLocaleString()}</TableCell>
                    <TableCell className="text-xs text-emerald-400 text-right tabular-nums">{u.confirmed?.toLocaleString()}</TableCell>
                    <TableCell className="text-xs text-right tabular-nums font-medium text-emerald-400">{u.hit_rate ?? '\u2014'}%</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      ) : null}

      <p className="text-[10px] text-muted-foreground text-center">
        {data.pending?.toLocaleString()} predictions currently monitoring, awaiting future validation
      </p>
    </div>
  );
}

// ---- Main Reports Page ----
export default function Reports() {
  const { activeTenantId, isLoadingTenants } = useTenant();

  if (isLoadingTenants) {
    return (
      <Page>
        <PageBody>
          <div className="flex items-center justify-center h-64">
            <Loader2 className="h-8 w-8 animate-spin text-emerald-400" />
          </div>
        </PageBody>
      </Page>
    );
  }

  return (
    <Page>
      <PageBody className="space-y-6">
        <div className="space-y-1">
          <h1 className="text-lg font-semibold text-card-foreground">Reports</h1>
          <p className="text-xs text-muted-foreground">Market intelligence, pipeline forecasts, and prediction accuracy</p>
        </div>

        <Tabs defaultValue="market" className="space-y-4">
          <TabsList className="bg-white/[0.03] border border-white/[0.06]">
            <TabsTrigger value="market" className="text-xs data-[state=active]:bg-white/[0.06]">
              <BarChart3 className="h-3.5 w-3.5 mr-1.5" /> Market Overview
            </TabsTrigger>
            <TabsTrigger value="pipeline" className="text-xs data-[state=active]:bg-white/[0.06]">
              <Target className="h-3.5 w-3.5 mr-1.5" /> Pipeline
            </TabsTrigger>
            <TabsTrigger value="validation" className="text-xs data-[state=active]:bg-white/[0.06]">
              <TrendingUp className="h-3.5 w-3.5 mr-1.5" /> Prediction Accuracy
            </TabsTrigger>
          </TabsList>

          <TabsContent value="market">
            <MarketOverview tenantId={activeTenantId} />
          </TabsContent>
          <TabsContent value="pipeline">
            <PipelineReport tenantId={activeTenantId} />
          </TabsContent>
          <TabsContent value="validation">
            <ValidationReport tenantId={activeTenantId} />
          </TabsContent>
        </Tabs>
      </PageBody>
    </Page>
  );
}
