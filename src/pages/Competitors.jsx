import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { Users, Plus, Edit2, Trash2, Loader2, Target, TrendingUp, TrendingDown, Minus, Sparkles, Trophy, AlertCircle, CheckCircle2, Building2, MapPin, DollarSign, ArrowRight, FileText, Lightbulb } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { supabase } from '@/lib/supabaseClient';

const fmtEur = (v) => { if (!v) return '€0'; if (v >= 1e9) return `€${(v/1e9).toFixed(1)}B`; if (v >= 1e6) return `€${(v/1e6).toFixed(1)}M`; if (v >= 1e3) return `€${(v/1e3).toFixed(0)}K`; return `€${v.toLocaleString()}`; };
const fmtCluster = (c) => c ? c.replace('cluster_','').split('_').map(w=>w[0].toUpperCase()+w.slice(1)).join(' ') : 'Unknown';

const IncumbentBadge = ({strength}) => {
    const m = { strong_incumbent: ['bg-emerald-500/15 text-emerald-300 border border-emerald-400/40','Strong incumbent'], moderate_incumbent: ['bg-amber-500/15 text-amber-300 border border-amber-400/40','Competitive renewal'], low_lock_in: ['bg-red-500/15 text-red-300 border border-red-400/40','Low incumbent lock-in'] };
    const [cls, label] = m[strength] || ['',''];
    return <Badge className={`text-xs ${cls}`}>{label}</Badge>;
};

const StrengthBadge = ({strength}) => {
    const m = { strong: 'bg-emerald-500/15 text-emerald-300 border border-emerald-400/40', moderate: 'bg-amber-500/15 text-amber-300 border border-amber-400/40', emerging: 'bg-slate-700/50 text-slate-300 border border-slate-500/40' };
    return <Badge className={`text-xs ${m[strength]||''}`}>{strength}</Badge>;
};

function CompetitorDashboard({ data, onClose }) {
    const { summary, renewal_opportunities=[], buyer_relationships=[], category_breakdown=[], yearly_trend=[], recent_contracts=[], analysis, trend } = data;
    if (!summary) return <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardHeader><div className="flex justify-between"><CardTitle>{data.company_name}</CardTitle><Button variant="outline" size="sm" onClick={onClose}>Close</Button></div></CardHeader><CardContent><p className="text-slate-400">{data.found_tenders} tenders found. {data.message||''}</p></CardContent></Card>;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h2 className="text-2xl font-bold text-slate-100 uppercase tracking-wide">{data.company_name}</h2>
                    <div className="flex items-center gap-4 mt-2 text-sm text-slate-400 flex-wrap">
                        <span><strong className="text-slate-100">{summary.total_awards}</strong> contracts awarded ({summary.years_active} years)</span>
                        <span><strong className="text-slate-100">{fmtEur(summary.total_value_eur)}</strong> Total value</span>
                        <span><strong className="text-slate-100">{summary.distinct_buyers}</strong> public bodies</span>
                        <span><strong className="text-slate-100">{summary.active_contracts}</strong> likely active</span>
                    </div>
                    {summary.expiring_12m > 0 && (
                        <div className="flex items-center gap-3 mt-2 flex-wrap">
                            <Badge className="bg-civant-teal/15 text-civant-teal border border-civant-teal/40 text-xs">Opportunity Landscape: {summary.expiring_12m} renewals (12 months)</Badge>
                            <Badge className="bg-slate-700/50 text-slate-300 border border-slate-500/40 text-xs">🇮🇪 {fmtEur(renewal_opportunities.reduce((s,r) => s+(r.value_eur||0),0))} estimated value</Badge>
                        </div>
                    )}
                </div>
                <Button variant="outline" size="sm" onClick={onClose}>Close</Button>
            </div>

            <Tabs defaultValue="landscape" className="w-full">
                <TabsList className="bg-slate-900/70 border border-civant-border">
                    <TabsTrigger value="contracts" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal"><FileText className="h-3.5 w-3.5 mr-1.5" />Contracts</TabsTrigger>
                    <TabsTrigger value="landscape" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal"><Target className="h-3.5 w-3.5 mr-1.5" />Opportunity Landscape</TabsTrigger>
                    <TabsTrigger value="buyers" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal"><Building2 className="h-3.5 w-3.5 mr-1.5" />Buyers</TabsTrigger>
                    <TabsTrigger value="insights" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal"><Lightbulb className="h-3.5 w-3.5 mr-1.5" />Insights</TabsTrigger>
                </TabsList>

                {/* ===== OPPORTUNITY LANDSCAPE ===== */}
                <TabsContent value="landscape" className="mt-4 space-y-4">
                    <div className="grid grid-cols-3 gap-4">
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardContent className="p-4"><div className="flex items-center gap-3"><div className="text-3xl font-bold text-slate-100">{renewal_opportunities.length}</div><div><p className="text-sm text-slate-400">renewal opportunities</p><p className="text-xs text-slate-500">within 12 months</p></div></div></CardContent></Card>
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardContent className="p-4"><div className="text-3xl font-bold text-civant-teal">{fmtEur(renewal_opportunities.reduce((s,r)=>s+(r.value_eur||0),0))}</div><p className="text-sm text-slate-400">opportunity horizon</p></CardContent></Card>
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardContent className="p-4"><div className="flex gap-4">
                            <div className="text-center"><div className="text-2xl font-bold text-red-400">{renewal_opportunities.filter(r=>r.window_class==='imminent').length}</div><div className="text-[10px] text-slate-500">IMMINENT</div></div>
                            <div className="text-center"><div className="text-2xl font-bold text-amber-400">{renewal_opportunities.filter(r=>r.window_class==='upcoming').length}</div><div className="text-[10px] text-slate-500">UPCOMING</div></div>
                            <div className="text-center"><div className="text-2xl font-bold text-emerald-400">{renewal_opportunities.filter(r=>r.window_class==='horizon').length}</div><div className="text-[10px] text-slate-500">HORIZON</div></div>
                        </div></CardContent></Card>
                    </div>
                    {renewal_opportunities.length > 0 ? (
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                            <CardHeader className="pb-3"><CardTitle className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Upcoming Renewal Opportunities</CardTitle></CardHeader>
                            <CardContent>
                                <table className="w-full text-sm">
                                    <thead><tr className="border-b border-civant-border text-xs text-slate-500 uppercase tracking-wider">
                                        <th className="text-left py-3 pr-4">Buyer</th><th className="text-right py-3 px-4">Value</th><th className="text-left py-3 px-4">Category</th><th className="text-left py-3 px-4">Expiry</th><th className="text-left py-3 px-4">Window</th><th className="text-left py-3 pl-4">Opportunity Strength</th>
                                    </tr></thead>
                                    <tbody>{renewal_opportunities.map((r,i) => (
                                        <tr key={i} className="border-b border-civant-border/50 hover:bg-slate-900/40 transition-colors">
                                            <td className="py-3 pr-4"><div className="font-medium text-slate-100">{r.buyer_name}</div>{r.framework_flag && <span className="text-[10px] text-slate-500">Framework Agreement</span>}</td>
                                            <td className="py-3 px-4 text-right font-medium text-slate-100">{fmtEur(r.value_eur)}</td>
                                            <td className="py-3 px-4 text-slate-300">{fmtCluster(r.cpv_cluster)}</td>
                                            <td className="py-3 px-4"><div className="text-slate-300">{r.end_date}</div><div className="text-[10px] text-slate-500">{r.days_until_expiry} days</div></td>
                                            <td className="py-3 px-4"><IncumbentBadge strength={r.incumbent_strength} /></td>
                                            <td className="py-3 pl-4 text-slate-400 text-xs">{r.incumbent_strength==='low_lock_in'?'Favorable switching conditions':r.incumbent_strength==='strong_incumbent'?'High switching barriers':'Moderate incumbent entrenchment'}</td>
                                        </tr>
                                    ))}</tbody>
                                </table>
                                <p className="text-[11px] text-slate-600 mt-4">Opportunity strength based on estimated contract expiry and incumbency dynamics. Renewal windows are estimated using contract duration and historical procurement patterns.</p>
                            </CardContent>
                        </Card>
                    ) : (
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardContent className="py-8 text-center text-slate-400">No contracts expiring in the next 12 months</CardContent></Card>
                    )}
                </TabsContent>

                {/* ===== CONTRACTS ===== */}
                <TabsContent value="contracts" className="mt-4 space-y-4">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                        {category_breakdown.map((cat,i) => (
                            <Card key={i} className="border border-civant-border bg-civant-navy/55 shadow-none"><CardContent className="p-4">
                                <p className="text-xs text-slate-500 uppercase tracking-wider">{fmtCluster(cat.cluster)}</p>
                                <p className="text-xl font-bold text-slate-100 mt-1">{cat.award_count} awards</p>
                                <p className="text-sm text-civant-teal">{fmtEur(cat.total_value)}</p>
                                <p className="text-xs text-slate-500 mt-1">{cat.distinct_buyers} buyers · {cat.active_contracts} active</p>
                            </CardContent></Card>
                        ))}
                    </div>
                    <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                        <CardHeader className="pb-3"><CardTitle className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Recent Contracts</CardTitle></CardHeader>
                        <CardContent>
                            <table className="w-full text-sm">
                                <thead><tr className="border-b border-civant-border text-xs text-slate-500 uppercase tracking-wider">
                                    <th className="text-left py-3 pr-4">Buyer</th><th className="text-right py-3 px-4">Value</th><th className="text-left py-3 px-4">Category</th><th className="text-left py-3 px-4">Award Date</th><th className="text-left py-3 px-4">End Date</th><th className="text-left py-3 pl-4">Type</th>
                                </tr></thead>
                                <tbody>{recent_contracts.map((c,i) => (
                                    <tr key={i} className="border-b border-civant-border/50 hover:bg-slate-900/40 transition-colors">
                                        <td className="py-3 pr-4 font-medium text-slate-100">{c.buyer_name}</td>
                                        <td className="py-3 px-4 text-right text-slate-100">{fmtEur(c.value_eur)}</td>
                                        <td className="py-3 px-4 text-slate-300">{fmtCluster(c.cpv_cluster)}</td>
                                        <td className="py-3 px-4 text-slate-300">{c.award_date}</td>
                                        <td className="py-3 px-4 text-slate-300">{c.end_date||'—'}</td>
                                        <td className="py-3 pl-4">{c.framework_flag && <Badge variant="outline" className="text-[10px]">Framework</Badge>}</td>
                                    </tr>
                                ))}</tbody>
                            </table>
                        </CardContent>
                    </Card>
                    {yearly_trend.length > 0 && (
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                            <CardHeader className="pb-3"><div className="flex items-center justify-between">
                                <CardTitle className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Award Trend</CardTitle>
                                <div className="flex items-center gap-1.5">{trend==='growing'?<TrendingUp className="h-4 w-4 text-emerald-500"/>:trend==='declining'?<TrendingDown className="h-4 w-4 text-red-500"/>:<Minus className="h-4 w-4 text-slate-400"/>}<span className="text-sm text-slate-300 capitalize">{trend}</span></div>
                            </div></CardHeader>
                            <CardContent>
                                <div className="flex items-end gap-1.5 h-32">
                                    {yearly_trend.map((y,i) => { const mx = Math.max(...yearly_trend.map(t=>t.awards)); const h = mx>0?(y.awards/mx)*100:0; return (
                                        <div key={i} className="flex-1 flex flex-col items-center gap-1">
                                            <span className="text-[10px] text-slate-500">{y.awards}</span>
                                            <div className="w-full bg-civant-teal/30 rounded-t hover:bg-civant-teal/50 transition-colors" style={{height:`${Math.max(h,4)}%`}} title={`${y.year}: ${y.awards} awards, ${fmtEur(y.total_value)}`} />
                                            <span className="text-[10px] text-slate-500">{y.year}</span>
                                        </div>
                                    ); })}
                                </div>
                            </CardContent>
                        </Card>
                    )}
                </TabsContent>

                {/* ===== BUYERS ===== */}
                <TabsContent value="buyers" className="mt-4">
                    <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                        <CardHeader className="pb-3"><CardTitle className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Key Buyer Relationships</CardTitle></CardHeader>
                        <CardContent><div className="space-y-2">{buyer_relationships.map((b,i) => (
                            <div key={i} className="flex items-center justify-between p-3 bg-slate-900/40 rounded-lg hover:bg-slate-900/60 transition-colors">
                                <div className="flex-1"><div className="font-medium text-slate-100">{b.buyer_name}</div><div className="text-xs text-slate-500 mt-0.5">{b.first_award} → {b.last_award}{b.active_contracts>0 && <span className="text-emerald-400 ml-2">· {b.active_contracts} active</span>}</div></div>
                                <div className="flex items-center gap-3"><div className="text-right"><div className="text-sm font-medium text-slate-100">{fmtEur(b.total_value)}</div><div className="text-xs text-slate-500">{b.award_count} awards</div></div><StrengthBadge strength={b.relationship_strength}/></div>
                            </div>
                        ))}</div></CardContent>
                    </Card>
                </TabsContent>

                {/* ===== INSIGHTS ===== */}
                <TabsContent value="insights" className="mt-4 space-y-4">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {analysis?.strengths?.length > 0 && (
                            <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardHeader className="pb-3"><CardTitle className="text-sm font-semibold flex items-center gap-2"><Trophy className="h-4 w-4 text-emerald-500"/>Strengths</CardTitle></CardHeader><CardContent><ul className="space-y-2 text-sm">{analysis.strengths.map((s,i) => <li key={i} className="flex items-start gap-2"><CheckCircle2 className="h-4 w-4 text-emerald-500 mt-0.5 flex-shrink-0"/><span className="text-slate-300">{s}</span></li>)}</ul></CardContent></Card>
                        )}
                        {analysis?.weaknesses?.length > 0 && (
                            <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardHeader className="pb-3"><CardTitle className="text-sm font-semibold flex items-center gap-2"><AlertCircle className="h-4 w-4 text-amber-500"/>Weaknesses</CardTitle></CardHeader><CardContent><ul className="space-y-2 text-sm">{analysis.weaknesses.map((w,i) => <li key={i} className="flex items-start gap-2"><span className="text-amber-500 flex-shrink-0">•</span><span className="text-slate-300">{w}</span></li>)}</ul></CardContent></Card>
                        )}
                    </div>
                    {analysis?.strategic_insights?.length > 0 && (
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardHeader className="pb-3"><CardTitle className="text-sm font-semibold flex items-center gap-2"><Sparkles className="h-4 w-4 text-civant-teal"/>Strategic Insights</CardTitle></CardHeader><CardContent><ul className="space-y-2 text-sm">{analysis.strategic_insights.map((s,i) => <li key={i} className="flex items-start gap-2"><ArrowRight className="h-4 w-4 text-civant-teal mt-0.5 flex-shrink-0"/><span className="text-slate-300">{s}</span></li>)}</ul></CardContent></Card>
                    )}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardHeader className="pb-3"><CardTitle className="text-sm font-semibold flex items-center gap-2"><DollarSign className="h-4 w-4 text-civant-teal"/>Value Analysis</CardTitle></CardHeader><CardContent className="space-y-3 text-sm">
                            <div className="flex justify-between"><span className="text-slate-400">Average Contract</span><span className="font-medium text-slate-100">{fmtEur(summary.avg_contract_value_eur)}</span></div>
                            <div className="flex justify-between"><span className="text-slate-400">Largest Win</span><span className="font-medium text-emerald-400">{fmtEur(summary.max_contract_value_eur)}</span></div>
                            <div className="flex justify-between"><span className="text-slate-400">Framework Agreements</span><span className="font-medium text-slate-100">{summary.has_frameworks}</span></div>
                            <div className="flex justify-between"><span className="text-slate-400">Active Contracts</span><span className="font-medium text-slate-100">{summary.active_contracts}</span></div>
                        </CardContent></Card>
                        <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardHeader className="pb-3"><CardTitle className="text-sm font-semibold flex items-center gap-2"><Target className="h-4 w-4 text-civant-teal"/>Preferred Categories</CardTitle></CardHeader><CardContent><div className="space-y-2">{category_breakdown.slice(0,5).map((c,i) => (
                            <div key={i} className="flex items-center justify-between text-sm"><span className="font-medium text-slate-100">{fmtCluster(c.cluster)}</span><div className="flex items-center gap-2"><Badge variant="outline" className="text-xs">{c.award_count}x</Badge><span className="text-xs text-civant-teal">{fmtEur(c.total_value)}</span></div></div>
                        ))}</div></CardContent></Card>
                    </div>
                </TabsContent>
            </Tabs>
        </div>
    );
}

// ============================================================
// Main Competitors Page
// ============================================================
export default function Competitors() {
    const [user, setUser] = useState(null);
    const [competitors, setCompetitors] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showForm, setShowForm] = useState(false);
    const [editingCompetitor, setEditingCompetitor] = useState(null);
    const [saving, setSaving] = useState(false);
    const [saveError, setSaveError] = useState('');
    const [analyzing, setAnalyzing] = useState(null);
    const [analysis, setAnalysis] = useState(null);
    const [formData, setFormData] = useState({ company_name: '', country: '', industry_sectors: '', notes: '', active: true });

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        try {
            const userData = await civant.auth.me();
            setUser(userData);
            const competitorsData = await civant.entities.Competitors.filter({ tracked_by_user: userData.email });
            setCompetitors(competitorsData);
        } catch (error) { console.error('Error loading data:', error); }
        finally { setLoading(false); }
    };

    const handleSubmit = async (e) => {
        e.preventDefault(); setSaving(true); setSaveError('');
        try {
            const payload = { ...formData, tracked_by_user: user?.email, active: true };
            if (editingCompetitor) await civant.entities.Competitors.update(editingCompetitor.id, payload);
            else await civant.entities.Competitors.create(payload);
            setShowForm(false); setEditingCompetitor(null); resetForm(); await loadData();
        } catch (error) { setSaveError(error.message); }
        finally { setSaving(false); }
    };

    const handleEdit = (c) => { setEditingCompetitor(c); setFormData({ company_name: c.company_name||'', country: c.country||'', industry_sectors: c.industry_sectors||'', notes: c.notes||'', active: c.active!==false }); setShowForm(true); };
    const handleDelete = async (id) => { if (!confirm('Remove this competitor?')) return; try { await civant.entities.Competitors.delete(id); await loadData(); } catch(e) { console.error('Delete failed:', e); } };
    const resetForm = () => setFormData({ company_name: '', country: '', industry_sectors: '', notes: '', active: true });

    const analyzeCompetitor = async (companyName) => {
        setAnalyzing(companyName); setAnalysis(null);
        try {
            const { data, error } = await supabase.rpc('get_competitor_intelligence', { p_tenant_id: 'civant_default', p_search_term: companyName });
            if (error) throw new Error(error.message);
            if (!data || !data.success) { alert(data?.message || 'No awards found'); return; }
            const sm = data.summary, cats = data.category_breakdown||[], brs = data.buyer_relationships||[];
            const rens = data.renewal_opportunities||[], trd = data.yearly_trend||[];
            let trend = 'stable';
            if (trd.length >= 3) { const r2 = trd.slice(-2), e2 = trd.slice(-4,-2); const ra = r2.reduce((s,t)=>s+t.awards,0)/r2.length; const ea = e2.length>0 ? e2.reduce((s,t)=>s+t.awards,0)/e2.length : ra; if (ra > ea*1.2) trend='growing'; else if (ra < ea*0.8) trend='declining'; }
            const strengths = [], weaknesses = [], insights = [];
            if (sm.total_awards > 20) strengths.push('Extensive track record: '+sm.total_awards+' contracts over '+sm.years_active+' years');
            else if (sm.total_awards > 5) strengths.push('Established presence: '+sm.total_awards+' public contracts');
            if (sm.has_frameworks > 0) strengths.push(sm.has_frameworks+' framework agreements (pre-qualified)');
            const stB = brs.filter(b=>b.relationship_strength==='strong');
            if (stB.length > 0) strengths.push('Strong relationships: '+stB.slice(0,3).map(b=>b.buyer_name).join(', '));
            if (sm.active_contracts > 3) strengths.push(sm.active_contracts+' active contracts');
            if (cats.length > 2) strengths.push('Diversified across '+cats.length+' categories');
            if (cats[0] && cats.length > 1) { const p = cats[0].award_count/sm.total_awards*100; if (p>70) weaknesses.push('Concentrated in '+fmtCluster(cats[0].cluster)+' ('+Math.round(p)+'%)'); }
            const imm = rens.filter(r=>r.window_class==='imminent').length;
            if (imm > 0) weaknesses.push(imm+' contract(s) expiring imminently');
            if (sm.distinct_buyers < 5 && sm.total_awards > 5) weaknesses.push('Narrow buyer base ('+sm.distinct_buyers+' buyers)');
            if (rens.length > 0) { const tv = rens.reduce((s,r)=>s+(r.value_eur||0),0); insights.push(rens.length+' contracts ('+fmtEur(tv)+') expiring in 12 months'); }
            const ll = rens.filter(r=>(r.repeat_wins||0)<=1);
            if (ll.length > 0) insights.push(ll.length+' with low incumbent lock-in');
            const ehv = brs.filter(b=>b.relationship_strength==='emerging'&&(b.total_value||0)>1e7);
            if (ehv.length > 0) insights.push('Vulnerability at '+ehv.slice(0,2).map(b=>b.buyer_name).join(', '));
            setAnalysis({ success:true, company_name:companyName, found_tenders:sm.total_awards, summary:sm, renewal_opportunities:rens.map(r=>({...r, incumbent_strength:(r.repeat_wins||0)>=3?'strong_incumbent':(r.repeat_wins||0)>=2?'moderate_incumbent':'low_lock_in'})), buyer_relationships:brs, category_breakdown:cats, yearly_trend:trd, recent_contracts:data.recent_contracts||[], trend, analysis:{strengths, weaknesses, strategic_insights:insights} });
        } catch (error) { console.error('Analysis failed:', error); alert('Analysis failed: ' + error.message); }
        finally { setAnalyzing(null); }
    };

    if (loading) return <div className="flex items-center justify-center h-64"><Loader2 className="h-8 w-8 animate-spin text-civant-teal" /></div>;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2"><Users className="h-6 w-6 text-civant-teal" />Competitor Intelligence</h1>
                    <p className="text-slate-400 mt-1">Track competitors and analyze their public procurement activity</p>
                </div>
                <Button className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90" onClick={() => { setEditingCompetitor(null); resetForm(); setShowForm(true); }}>
                    <Plus className="h-4 w-4 mr-2" />Add Competitor
                </Button>
            </div>

            {/* Analysis Dashboard */}
            {analysis && <CompetitorDashboard data={analysis} onClose={() => setAnalysis(null)} />}

            {/* Competitor Cards */}
            <div className="grid gap-4">
                {competitors.length === 0 ? (
                    <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                        <CardContent className="py-12 text-center">
                            <Users className="h-12 w-12 mx-auto text-slate-300 mb-4" />
                            <h3 className="text-lg font-semibold text-slate-100 mb-2">No competitors tracked yet</h3>
                            <p className="text-slate-400 mb-4">Start tracking competitors to analyze their bidding patterns</p>
                            <Button onClick={() => setShowForm(true)}><Plus className="h-4 w-4 mr-2" />Add Your First Competitor</Button>
                        </CardContent>
                    </Card>
                ) : competitors.map(competitor => (
                    <Card key={competitor.id} className="border border-civant-border bg-civant-navy/55 shadow-none hover:bg-slate-900/70 transition-colors">
                        <CardContent className="p-6">
                            <div className="flex items-start justify-between">
                                <div className="flex-1">
                                    <div className="flex items-center gap-3 mb-2">
                                        <h3 className="text-lg font-semibold text-slate-100">{competitor.company_name}</h3>
                                        {!competitor.active && <Badge variant="outline" className="text-slate-400">Inactive</Badge>}
                                    </div>
                                    <div className="flex flex-wrap gap-2 mb-3">
                                        {competitor.country && <Badge variant="outline" className="text-xs"><MapPin className="h-3 w-3 mr-1" />{competitor.country === 'both' ? 'IE & FR' : competitor.country === 'FR' ? 'France' : 'Ireland'}</Badge>}
                                        {competitor.industry_sectors && <Badge variant="outline" className="text-xs">{competitor.industry_sectors}</Badge>}
                                    </div>
                                    {competitor.notes && <p className="text-sm text-slate-300 mb-3">{competitor.notes}</p>}
                                    <Button variant="outline" size="sm" onClick={() => analyzeCompetitor(competitor.company_name)} disabled={analyzing === competitor.company_name}>
                                        {analyzing === competitor.company_name ? <><Loader2 className="h-4 w-4 mr-2 animate-spin" />Analyzing...</> : <><Sparkles className="h-4 w-4 mr-2" />Analyze Competitor</>}
                                    </Button>
                                </div>
                                <div className="flex items-center gap-2">
                                    <Button variant="ghost" size="icon" onClick={() => handleEdit(competitor)}><Edit2 className="h-4 w-4 text-slate-400" /></Button>
                                    <Button variant="ghost" size="icon" onClick={() => handleDelete(competitor.id)}><Trash2 className="h-4 w-4 text-red-500" /></Button>
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                ))}
            </div>

            {/* Add/Edit Dialog */}
            <Dialog open={showForm} onOpenChange={setShowForm}>
                <DialogContent className="sm:max-w-lg">
                    <DialogHeader>
                        <DialogTitle>{editingCompetitor ? 'Edit Competitor' : 'Add Competitor'}</DialogTitle>
                        <DialogDescription>Track a competitor to analyze their bidding patterns and performance</DialogDescription>
                    </DialogHeader>
                    <form onSubmit={handleSubmit} className="space-y-4">
                        <div><Label htmlFor="company_name">Company Name *</Label><Input id="company_name" value={formData.company_name} onChange={(e) => setFormData({...formData, company_name: e.target.value})} placeholder="e.g. BAM Contractors" required /></div>
                        <div><Label htmlFor="country">Primary Country</Label>
                            <Select value={formData.country} onValueChange={(v) => setFormData({...formData, country: v})}>
                                <SelectTrigger><SelectValue placeholder="Select country" /></SelectTrigger>
                                <SelectContent><SelectItem value="IE">Ireland</SelectItem><SelectItem value="FR">France</SelectItem><SelectItem value="ES">Spain</SelectItem><SelectItem value="both">Multiple</SelectItem></SelectContent>
                            </Select>
                        </div>
                        <div><Label htmlFor="industry_sectors">Industry Sectors</Label><Input id="industry_sectors" value={formData.industry_sectors} onChange={(e) => setFormData({...formData, industry_sectors: e.target.value})} placeholder="e.g. IT Services, Construction" /></div>
                        <div><Label htmlFor="notes">Notes</Label><Textarea id="notes" value={formData.notes} onChange={(e) => setFormData({...formData, notes: e.target.value})} placeholder="Any notes about this competitor..." rows={3} /></div>
                        {saveError && <p className="text-sm text-red-400">{saveError}</p>}
                        <DialogFooter>
                            <Button type="button" variant="outline" onClick={() => { setShowForm(false); setEditingCompetitor(null); resetForm(); }}>Cancel</Button>
                            <Button type="submit" className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90" disabled={saving}>
                                {saving ? <><Loader2 className="h-4 w-4 mr-2 animate-spin" />Saving...</> : editingCompetitor ? 'Update' : 'Add Competitor'}
                            </Button>
                        </DialogFooter>
                    </form>
                </DialogContent>
            </Dialog>
        </div>
    );
}
