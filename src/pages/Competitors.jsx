import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { civant } from '@/api/civantClient';
import { supabase } from '@/lib/supabaseClient';
import { useTenant } from '@/lib/tenant';
import { ArrowLeft, Edit2, Loader2, MapPin, Plus, Trash2, Users } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import SupplierAutocomplete from '@/components/SupplierAutocomplete';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Textarea } from '@/components/ui/textarea';
import { createPageUrl } from '@/utils';

const fmtEur = (v) => {
  if (!v) return '€0';
  if (v >= 1e9) return `€${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `€${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `€${(v / 1e3).toFixed(0)}K`;
  return `€${Number(v).toLocaleString()}`;
};

const fmtCluster = (c) => {
  if (!c) return 'Unknown';
  return c.replace('cluster_', '').split('_').map((w) => w[0].toUpperCase() + w.slice(1)).join(' ');
};

const countryLabel = (country) => {
  if (!country) return null;
  if (country === 'both') return 'IE & FR';
  if (country === 'FR') return 'France';
  if (country === 'ES') return 'Spain';
  if (country === 'IE') return 'Ireland';
  return country;
};

function buildCompetitorAnalysis(companyName, data) {
  const summary = data?.summary;
  const categories = data?.category_breakdown || [];
  const buyers = data?.buyer_relationships || [];
  const renewals = data?.renewal_opportunities || [];
  const trendData = data?.yearly_trend || [];

  let overallTrend = 'stable';
  if (trendData.length >= 3) {
    const recent = trendData.slice(-2);
    const earlier = trendData.slice(-4, -2);
    const recentAvg = recent.reduce((s, t) => s + t.awards, 0) / recent.length;
    const earlierAvg = earlier.length > 0 ? earlier.reduce((s, t) => s + t.awards, 0) / earlier.length : recentAvg;
    if (recentAvg > earlierAvg * 1.2) overallTrend = 'growing';
    else if (recentAvg < earlierAvg * 0.8) overallTrend = 'declining';
  }

  const strengths = [];
  if (summary?.total_awards > 20) strengths.push(`${summary.total_awards} contracts over ${summary.years_active} years`);
  else if (summary?.total_awards > 5) strengths.push(`${summary.total_awards} public contracts awarded`);
  if (summary?.has_frameworks > 0) strengths.push(`${summary.has_frameworks} framework agreements`);

  const strongBuyers = buyers.filter((b) => b.relationship_strength === 'strong');
  if (strongBuyers.length > 0) {
    strengths.push(`Strong buyer ties: ${strongBuyers.slice(0, 3).map((b) => b.buyer_name).join(', ')}`);
  }

  if (summary?.active_contracts > 3) strengths.push(`${summary.active_contracts} active contracts`);
  if (categories.length > 2) strengths.push(`Diversified across ${categories.length} categories`);

  return {
    success: true,
    company_name: companyName,
    found_tenders: summary?.total_awards || 0,
    summary,
    renewal_opportunities: renewals,
    buyer_relationships: buyers,
    category_breakdown: categories,
    yearly_trend: trendData,
    recent_contracts: data?.recent_contracts || [],
    trading_names: data?.trading_names || [],
    trend: overallTrend,
    analysis: {
      strengths
    }
  };
}

function MetricTile({ label, value, hint }) {
  return (
    <Card className="border border-white/[0.05] bg-white/[0.015] shadow-none">
      <CardContent className="px-4 py-4">
        <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">{label}</p>
        <p className="mt-1 text-2xl font-semibold tracking-tight text-card-foreground tabular-nums">{value}</p>
        {hint ? <p className="mt-1 text-xs text-muted-foreground">{hint}</p> : null}
      </CardContent>
    </Card>
  );
}

function CompetitorDossier({
  competitor,
  competitors,
  analysis,
  loading,
  onBack,
  onSwitch,
  onEdit,
  onDelete
}) {
  const summary = analysis?.summary;
  const strengths = analysis?.analysis?.strengths || [];
  const categories = [...(analysis?.category_breakdown || [])]
    .sort((a, b) => Number(b.award_count || 0) - Number(a.award_count || 0))
    .slice(0, 5);
  const buyers = analysis?.buyer_relationships || [];
  const contracts = analysis?.recent_contracts || [];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="space-y-1">
          <Button variant="ghost" size="sm" className="-ml-2 text-slate-300" onClick={onBack}>
            <ArrowLeft className="mr-2 h-4 w-4" />
            Competitors / {competitor.company_name}
          </Button>
          <h1 className="text-3xl font-semibold tracking-tight text-slate-100">{competitor.company_name}</h1>
        </div>

        <div className="flex items-center gap-2">
          {competitors.length > 1 ? (
            <Select value={String(competitor.id)} onValueChange={onSwitch}>
              <SelectTrigger className="h-9 w-[260px] border-white/[0.08] bg-white/[0.02] text-sm text-slate-300">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {competitors.map((item) => (
                  <SelectItem key={item.id} value={String(item.id)}>{item.company_name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          ) : null}
          <Button variant="ghost" size="icon" onClick={() => onEdit(competitor)}><Edit2 className="h-4 w-4 text-slate-400" /></Button>
          <Button variant="ghost" size="icon" onClick={() => onDelete(competitor.id)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
        </div>
      </div>

      <section className="rounded-2xl border border-white/[0.06] bg-white/[0.015] p-4 md:p-5">
        {loading ? (
          <div className="flex min-h-[220px] items-center justify-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Building competitor dossier...
          </div>
        ) : !summary ? (
          <div className="space-y-1 py-8 text-center text-sm text-muted-foreground">
            <p>No dossier data available for this competitor yet.</p>
            <p>Try another competitor or refresh after new awards are indexed.</p>
          </div>
        ) : (
          <Tabs defaultValue="overview" className="w-full">
            <TabsList className="bg-slate-900/60 border border-white/[0.06]">
              <TabsTrigger value="overview" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">Overview</TabsTrigger>
              <TabsTrigger value="accounts" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">Accounts</TabsTrigger>
              <TabsTrigger value="contracts" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">Contracts</TabsTrigger>
            </TabsList>

            <TabsContent value="overview" className="mt-4 space-y-4">
              <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                <MetricTile label="Contracts" value={summary.total_awards || 0} hint={`${summary.years_active || 0} years active`} />
                <MetricTile label="Total Value" value={fmtEur(summary.total_value_eur)} hint={`avg ${fmtEur(summary.avg_contract_value_eur)}`} />
                <MetricTile label="Public Bodies" value={summary.distinct_buyers || 0} hint={`${summary.active_contracts || 0} active`} />
                <MetricTile label="Frameworks" value={summary.has_frameworks || 0} hint={`largest ${fmtEur(summary.max_contract_value_eur)}`} />
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <Card className="border border-white/[0.05] bg-white/[0.01] shadow-none">
                  <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-slate-300">Strengths</CardTitle></CardHeader>
                  <CardContent>
                    {strengths.length > 0 ? (
                      <ul className="space-y-2 text-sm text-slate-300">
                        {strengths.slice(0, 5).map((item, index) => (
                          <li key={`${item}-${index}`} className="flex items-start gap-2"><span className="mt-1 text-civant-teal">•</span><span>{item}</span></li>
                        ))}
                      </ul>
                    ) : (
                      <p className="text-sm text-muted-foreground">No strengths extracted yet.</p>
                    )}
                  </CardContent>
                </Card>

                <Card className="border border-white/[0.05] bg-white/[0.01] shadow-none">
                  <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-slate-300">Preferred Categories / Where They Win</CardTitle></CardHeader>
                  <CardContent>
                    {categories.length > 0 ? (
                      <div className="space-y-2">
                        {categories.map((cat, index) => (
                          <div key={`${cat.cluster}-${index}`} className="flex items-center justify-between text-sm">
                            <div className="flex items-center gap-2 text-slate-300">
                              <span className="w-5 text-xs text-muted-foreground">{index + 1}</span>
                              <span className="font-medium text-slate-100">{fmtCluster(cat.cluster)}</span>
                            </div>
                            <div className="text-right text-xs text-muted-foreground">
                              <p>{cat.award_count || 0} awards</p>
                              <p>{cat.distinct_buyers || 0} buyers · {fmtEur(cat.total_value)}</p>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="text-sm text-muted-foreground">No category concentration available.</p>
                    )}
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="accounts" className="mt-4">
              <Card className="border border-white/[0.05] bg-white/[0.01] shadow-none">
                <CardContent className="pt-4">
                  {buyers.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-white/[0.05] text-xs uppercase tracking-[0.08em] text-slate-500">
                            <th className="py-3 pr-4 text-left">Buyer</th>
                            <th className="px-4 py-3 text-right">Awards</th>
                            <th className="px-4 py-3 text-right">Total Value</th>
                            <th className="py-3 pl-4 text-left">Relationship</th>
                          </tr>
                        </thead>
                        <tbody>
                          {buyers.map((buyer, index) => (
                            <tr key={`${buyer.buyer_name}-${index}`} className="border-b border-white/[0.04]">
                              <td className="py-3 pr-4 font-medium text-slate-100">{buyer.buyer_name}</td>
                              <td className="px-4 py-3 text-right text-slate-300">{buyer.award_count || 0}</td>
                              <td className="px-4 py-3 text-right text-slate-300">{fmtEur(buyer.total_value)}</td>
                              <td className="py-3 pl-4 text-slate-300">{buyer.relationship_strength || '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="py-8 text-center text-sm text-muted-foreground">No buyer account data available.</p>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="contracts" className="mt-4">
              <Card className="border border-white/[0.05] bg-white/[0.01] shadow-none">
                <CardHeader className="pb-3"><CardTitle className="text-sm font-medium text-slate-300">Recent Contracts</CardTitle></CardHeader>
                <CardContent>
                  {contracts.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-white/[0.05] text-xs uppercase tracking-[0.08em] text-slate-500">
                            <th className="py-3 pr-4 text-left">Buyer</th>
                            <th className="px-4 py-3 text-right">Value</th>
                            <th className="px-4 py-3 text-left">Category</th>
                            <th className="px-4 py-3 text-left">Awarded</th>
                            <th className="py-3 pl-4 text-left">Ends</th>
                          </tr>
                        </thead>
                        <tbody>
                          {contracts.map((contract, index) => (
                            <tr key={`${contract.buyer_name}-${index}`} className="border-b border-white/[0.04]">
                              <td className="py-3 pr-4 font-medium text-slate-100">{contract.buyer_name}</td>
                              <td className="px-4 py-3 text-right text-slate-300">{fmtEur(contract.value_eur)}</td>
                              <td className="px-4 py-3 text-slate-300">{fmtCluster(contract.cpv_cluster)}</td>
                              <td className="px-4 py-3 text-slate-300">{contract.award_date || '—'}</td>
                              <td className="py-3 pl-4 text-slate-300">{contract.end_date || '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="py-8 text-center text-sm text-muted-foreground">No recent contracts available.</p>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        )}
      </section>
    </div>
  );
}

export default function Competitors() {
  const location = useLocation();
  const navigate = useNavigate();
  const { competitorId } = useParams();
  const { activeTenantId } = useTenant();

  const [user, setUser] = useState(null);
  const [competitors, setCompetitors] = useState([]);
  const [loading, setLoading] = useState(true);

  const [search, setSearch] = useState('');
  const [showForm, setShowForm] = useState(false);
  const [editingCompetitor, setEditingCompetitor] = useState(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState('');
  const [analyzingId, setAnalyzingId] = useState(null);
  const [analysisByCompetitorId, setAnalysisByCompetitorId] = useState({});

  const [formData, setFormData] = useState({
    company_name: '',
    country: '',
    industry_sectors: '',
    notes: '',
    active: true
  });

  const resetForm = useCallback(() => {
    setFormData({ company_name: '', country: '', industry_sectors: '', notes: '', active: true });
  }, []);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const userData = await civant.auth.me();
      setUser(userData);
      const competitorsData = await civant.entities.Competitors.filter({ tracked_by_user: userData.email });
      setCompetitors(competitorsData || []);
    } catch (error) {
      console.error('Error loading competitors:', error);
      setCompetitors([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    if (params.get('quickAdd') === '1') {
      setEditingCompetitor(null);
      resetForm();
      setShowForm(true);
    }
  }, [location.search, resetForm]);

  const selectedCompetitor = useMemo(
    () => competitors.find((c) => String(c.id) === String(competitorId || '')) || null,
    [competitorId, competitors]
  );

  const analyzeCompetitor = useCallback(async (competitor) => {
    if (!competitor || !activeTenantId) return null;

    setAnalyzingId(competitor.id);
    try {
      const { data: response, error: rpcError } = await supabase.rpc('get_competitor_intelligence', {
        p_tenant_id: activeTenantId,
        p_search_term: competitor.company_name
      });
      if (rpcError) throw new Error(rpcError.message);

      const payload = response?.data || response;
      if (payload?.error) throw new Error(payload.error);
      if (!payload || !payload.success) {
        throw new Error(payload?.message || 'No awards found for this competitor');
      }

      const dossierData = buildCompetitorAnalysis(competitor.company_name, payload);
      setAnalysisByCompetitorId((prev) => ({ ...prev, [competitor.id]: dossierData }));
      return dossierData;
    } catch (error) {
      console.error('Competitor analysis failed:', error);
      setAnalysisByCompetitorId((prev) => ({
        ...prev,
        [competitor.id]: {
          success: false,
          company_name: competitor.company_name,
          message: error.message
        }
      }));
      return null;
    } finally {
      setAnalyzingId(null);
    }
  }, [activeTenantId]);

  useEffect(() => {
    if (!selectedCompetitor || loading) return;
    const existing = analysisByCompetitorId[selectedCompetitor.id];
    if (!existing && analyzingId !== selectedCompetitor.id) {
      void analyzeCompetitor(selectedCompetitor);
    }
  }, [analyzeCompetitor, analyzingId, analysisByCompetitorId, loading, selectedCompetitor]);

  const handleSubmit = async (event) => {
    event.preventDefault();
    setSaving(true);
    setSaveError('');
    try {
      const payload = { ...formData, tracked_by_user: user?.email, active: true };
      if (editingCompetitor) {
        await civant.entities.Competitors.update(editingCompetitor.id, payload);
      } else {
        await civant.entities.Competitors.create(payload);
      }
      setShowForm(false);
      setEditingCompetitor(null);
      resetForm();
      await loadData();
    } catch (error) {
      setSaveError(error.message || 'Failed to save competitor.');
    } finally {
      setSaving(false);
    }
  };

  const handleEdit = (competitor) => {
    setEditingCompetitor(competitor);
    setFormData({
      company_name: competitor.company_name || '',
      country: competitor.country || '',
      industry_sectors: competitor.industry_sectors || '',
      notes: competitor.notes || '',
      active: competitor.active !== false
    });
    setShowForm(true);
  };

  const handleDelete = async (id) => {
    if (!confirm('Remove this competitor?')) return;
    try {
      await civant.entities.Competitors.delete(id);
      setAnalysisByCompetitorId((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      await loadData();
      if (String(competitorId || '') === String(id)) {
        navigate(createPageUrl('competitors'));
      }
    } catch (error) {
      console.error('Delete failed:', error);
    }
  };

  const filteredCompetitors = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return competitors;
    return competitors.filter((competitor) => {
      const name = String(competitor.company_name || '').toLowerCase();
      const sector = String(competitor.industry_sectors || '').toLowerCase();
      const country = String(competitor.country || '').toLowerCase();
      return name.includes(needle) || sector.includes(needle) || country.includes(needle);
    });
  }, [competitors, search]);

  if (loading) {
    return (
      <div className="flex h-64 items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-civant-teal" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {!competitorId ? (
        <>
          <div className="space-y-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="space-y-1">
                <h1 className="text-4xl font-semibold tracking-tight text-slate-100">Competitors</h1>
                <p className="text-sm text-slate-400">Track rival positioning and open each dossier for focused planning.</p>
              </div>
              <Button
                className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90"
                onClick={() => {
                  setEditingCompetitor(null);
                  resetForm();
                  setShowForm(true);
                }}
              >
                <Plus className="mr-2 h-4 w-4" />
                Add Competitor
              </Button>
            </div>

            <div className="max-w-md">
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Search competitors"
                className="h-10 border-white/[0.08] bg-white/[0.02]"
              />
            </div>
          </div>

          <div className="grid gap-4">
            {filteredCompetitors.length === 0 ? (
              <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                <CardContent className="py-12 text-center">
                  <Users className="mx-auto mb-4 h-12 w-12 text-slate-300" />
                  <h3 className="mb-2 text-lg font-semibold text-slate-100">No competitors tracked yet</h3>
                  <p className="mb-4 text-slate-400">Start with one competitor and build account-level dossiers.</p>
                  <Button onClick={() => setShowForm(true)}><Plus className="mr-2 h-4 w-4" />Add Your First Competitor</Button>
                </CardContent>
              </Card>
            ) : (
              filteredCompetitors.map((competitor) => (
                <Card key={competitor.id} className="border border-white/[0.06] bg-white/[0.02] shadow-none transition-colors hover:bg-slate-900/60">
                  <CardContent className="p-5">
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div className="min-w-0 flex-1 space-y-2">
                        <h3 className="truncate text-lg font-semibold text-slate-100">{competitor.company_name}</h3>
                        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
                          {countryLabel(competitor.country) ? (
                            <span className="inline-flex items-center gap-1"><MapPin className="h-3.5 w-3.5" />{countryLabel(competitor.country)}</span>
                          ) : null}
                          {competitor.industry_sectors ? <span>{competitor.industry_sectors}</span> : null}
                        </div>
                        {competitor.notes ? <p className="text-sm text-slate-300">{competitor.notes}</p> : null}
                      </div>

                      <div className="flex items-center gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          className="border-white/[0.1]"
                          onClick={() => navigate(createPageUrl(`competitors/${competitor.id}`))}
                        >
                          Open Dossier
                        </Button>
                        <Button variant="ghost" size="icon" onClick={() => handleEdit(competitor)}><Edit2 className="h-4 w-4 text-slate-400" /></Button>
                        <Button variant="ghost" size="icon" onClick={() => handleDelete(competitor.id)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))
            )}
          </div>
        </>
      ) : selectedCompetitor ? (
        <CompetitorDossier
          competitor={selectedCompetitor}
          competitors={competitors}
          analysis={analysisByCompetitorId[selectedCompetitor.id]}
          loading={analyzingId === selectedCompetitor.id || !analysisByCompetitorId[selectedCompetitor.id]}
          onBack={() => navigate(createPageUrl('competitors'))}
          onSwitch={(value) => navigate(createPageUrl(`competitors/${value}`))}
          onEdit={handleEdit}
          onDelete={handleDelete}
        />
      ) : (
        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
          <CardContent className="space-y-3 py-10 text-center">
            <p className="text-sm text-muted-foreground">Competitor not found in your tracked list.</p>
            <Button variant="outline" onClick={() => navigate(createPageUrl('competitors'))}>Back to Competitors</Button>
          </CardContent>
        </Card>
      )}

      <Dialog open={showForm} onOpenChange={setShowForm}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingCompetitor ? 'Edit Competitor' : 'Add Competitor'}</DialogTitle>
            <DialogDescription>Track a competitor to build an account-level dossier.</DialogDescription>
          </DialogHeader>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <Label htmlFor="company_name">Company Name *</Label>
              <SupplierAutocomplete
                value={formData.company_name}
                onChange={(value) => setFormData({ ...formData, company_name: value })}
                placeholder="Start typing to search suppliers..."
              />
            </div>
            <div>
              <Label htmlFor="country">Primary Country</Label>
              <Select value={formData.country} onValueChange={(value) => setFormData({ ...formData, country: value })}>
                <SelectTrigger><SelectValue placeholder="Select country" /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="IE">Ireland</SelectItem>
                  <SelectItem value="FR">France</SelectItem>
                  <SelectItem value="ES">Spain</SelectItem>
                  <SelectItem value="both">Multiple</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label htmlFor="industry_sectors">Industry Sectors</Label>
              <Input
                id="industry_sectors"
                value={formData.industry_sectors}
                onChange={(event) => setFormData({ ...formData, industry_sectors: event.target.value })}
                placeholder="e.g. IT Services, Construction"
              />
            </div>
            <div>
              <Label htmlFor="notes">Notes</Label>
              <Textarea
                id="notes"
                value={formData.notes}
                onChange={(event) => setFormData({ ...formData, notes: event.target.value })}
                placeholder="Any notes about this competitor..."
                rows={3}
              />
            </div>
            {saveError ? <p className="text-sm text-red-400">{saveError}</p> : null}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => { setShowForm(false); setEditingCompetitor(null); resetForm(); }}>Cancel</Button>
              <Button type="submit" className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90" disabled={saving}>
                {saving ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Saving...</> : editingCompetitor ? 'Update' : 'Add Competitor'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
