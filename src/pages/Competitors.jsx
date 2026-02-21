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
    const { summary, renewal_opportunities=[], buyer_relationships=[], category_breakdown=[], yearly_trend=[], recent_contracts=[], trading_names=[], analysis, trend } = data;
    const [showAllNames, setShowAllNames] = React.useState(false);
    if (!summary) return <Card className="border border-civant-border bg-civant-navy/55 shadow-none"><CardHeader><div className="flex justify-between"><CardTitle>{data.company_name}</CardTitle><Button variant="outline" size="sm" onClick={onClose}>Close</Button></div></CardHeader><CardContent><p className="text-slate-400">{data.found_tenders} tenders found. {data.message||''}</p></CardContent></Card>;

    const realNames = trading_names.filter(tn => tn.award_count > 1 || trading_names.length <= 3);

    return (
        <div className="space-y-4">
            {/* Header */}
            <div className="flex items-start justify-between">
                <div className="space-y-2">
                    <div className="flex items-center gap-3">
                        <h2 className="text-2xl font-bold text-slate-100 uppercase tracking-wide">{data.company_name}</h2>
                        {summary.expiring_12m > 0 && (
                            <Badge className="bg-civant-teal/15 text-civant-teal border border-civant-teal/40 text-xs">{summary.expiring_12m} renewals · {fmtEur(renewal_opportunities.reduce((s,r) => s+(r.value_eur||0),0))}</Badge>
                        )}
                    </div>
                    <div className="flex items-center gap-3 text-sm text-slate-500">
                        <span>{summary.total_awards} contracts</span>
                        <span className="text-slate-600">·</span>
                        <span className="text-slate-300">{fmtEur(summary.total_value_eur)}</span>
                        <span className="text-slate-600">·</span>
                        <span>{summary.distinct_buyers} buyers</span>
                        <span className="text-slate-600">·</span>
                        <span>{summary.active_contracts} active</span>
                        <span className="text-slate-600">·</span>
                        <span>{summary.years_active}yr history</span>
                    </div>
                    {realNames.length > 1 && (
                        <div className="flex items-center gap-1.5 flex-wrap">
                            <span className="text-[11px] text-slate-600">Entities:</span>
                            {(showAllNames ? realNames : realNames.slice(0, 3)).map((tn, i) => (
                                <span key={i} className="text-[11px] text-slate-500">{tn.name} ({tn.award_count}){i < (showAllNames ? realNames.length : Math.min(realNames.length, 3)) - 1 ? ',' : ''}</span>
                            ))}
                            {realNames.length > 3 && !showAllNames && (
                                <button onClick={() => setShowAllNames(true)} className="text-[11px] text-civant-teal hover:underline">+{realNames.length - 3} more</button>
                            )}
                            {showAllNames && realNames.length > 3 && (
                                <button onClick={() => setShowAllNames(false)} className="text-[11px] text-civant-teal hover:underline">show less</button>
                            )}
                        </div>
                    )}
                </div>
                <Button variant="ghost" size="sm" onClick={onClose} className="text-slate-400 hover:text-slate-100">✕</Button>
            </div>

            <Tabs defaultValue="landscape" className="w-full">
                <TabsList className="bg-transparent border-b border-civant-border/50 rounded-none h-auto p-0 gap-6">
                    <TabsTrigger value="contracts" className="rounded-none border-b-2 border-transparent data-[state=active]:border-civant-teal data-[state=active]:bg-transparent bg-transparent data-[state=active]:text-civant-teal text-slate-500 pb-3 px-0"><FileText className="h-4 w-4 mr-2" />Contracts</TabsTrigger>
                    <TabsTrigger value="landscape" className="rounded-none border-b-2 border-transparent data-[state=active]:border-civant-teal data-[state=active]:bg-transparent bg-transparent data-[state=active]:text-civant-teal text-slate-500 pb-3 px-0"><Target className="h-4 w-4 mr-2" />Opportunities</TabsTrigger>
                    <TabsTrigger value="buyers" className="rounded-none border-b-2 border-transparent data-[state=active]:border-civant-teal data-[state=active]:bg-transparent bg-transparent data-[state=active]:text-civant-teal text-slate-500 pb-3 px-0"><Building2 className="h-4 w-4 mr-2" />Buyers</TabsTrigger>
                    <TabsTrigger value="insights" className="rounded-none border-b-2 border-transparent data-[state=active]:border-civant-teal data-[state=active]:bg-transparent bg-transparent data-[state=active]:text-civant-teal text-slate-500 pb-3 px-0"><Lightbulb className="h-4 w-4 mr-2" />Insights</TabsTrigger>
                </TabsList>

                {/* ===== OPPORTUNITY LANDSCAPE ===== */}
                <TabsContent value="landscape" className="mt-8 space-y-8">
                    <div className="grid grid-cols-3 gap-6">
                        <div>
                            <div className="text-4xl font-light text-slate-100">{renewal_opportunities.length}</div>
                            <p className="text-sm text-slate-500 mt-1">renewals within 12 months</p>
                        </div>
                        <div>
                            <div className="text-4xl font-light text-civant-teal">{fmtEur(renewal_opportunities.reduce((s,r)=>s+(r.value_eur||0),0))}</div>
                            <p className="text-sm text-slate-500 mt-1">opportunity horizon</p>
                        </div>
                        <div className="flex items-end gap-6">
                            <div><div className="text-2xl font-light text-red-400">{renewal_opportunities.filter(r=>r.window_class==='imminent').length}</div><p className="text-xs text-slate-500 mt-1">Imminent</p></div>
                            <div><div className="text-2xl font-light text-amber-400">{renewal_opportunities.filter(r=>r.window_class==='upcoming').length}</div><p className="text-xs text-slate-500 mt-1">Upcoming</p></div>
                            <div><div className="text-2xl font-light text-emerald-400">{renewal_opportunities.filter(r=>r.window_class==='horizon').length}</div><p className="text-xs text-slate-500 mt-1">Horizon</p></div>
                        </div>
                    </div>
                    {renewal_opportunities.length > 0 ? (
                        <div>
                            <table className="w-full text-sm">
                                <thead><tr className="border-b border-civant-border/40 text-xs text-slate-500 uppercase tracking-wider">
                                    <th className="text-left py-3 pr-4 font-medium">Buyer</th><th className="text-right py-3 px-4 font-medium">Value</th><th className="text-left py-3 px-4 font-medium">Category</th><th className="text-left py-3 px-4 font-medium">Expiry</th><th className="text-left py-3 pl-4 font-medium">Strength</th>
                                </tr></thead>
                                <tbody>{renewal_opportunities.map((r,i) => (
                                    <tr key={i} className="border-b border-civant-border/20 hover:bg-slate-800/20 transition-colors">
                                        <td className="py-4 pr-4"><div className="text-slate-200">{r.buyer_name}</div>{r.framework_flag && <span className="text-xs text-slate-600">Framework</span>}</td>
                                        <td className="py-4 px-4 text-right text-slate-200">{fmtEur(r.value_eur)}</td>
                                        <td className="py-4 px-4 text-slate-400">{fmtCluster(r.cpv_cluster)}</td>
                                        <td className="py-4 px-4"><span className="text-slate-400">{r.end_date}</span> <span className="text-slate-600 text-xs">({r.days_until_expiry}d)</span></td>
                                        <td className="py-4 pl-4"><IncumbentBadge strength={r.incumbent_strength} /></td>
                                    </tr>
                                ))}</tbody>
                            </table>
                            <p className="text-xs text-slate-600 mt-6">Based on contract expiry and incumbency dynamics. Windows estimated from duration and procurement patterns.</p>
                        </div>
                    ) : (
                        <p className="text-slate-500 py-8 text-center">No contracts expiring in the next 12 months</p>
                    )}
                </TabsContent>

                {/* ===== CONTRACTS ===== */}
                <TabsContent value="contracts" className="mt-8 space-y-8">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
                        {category_breakdown.map((cat,i) => (
                            <div key={i}>
                                <p className="text-xs text-slate-500 uppercase tracking-wider">{fmtCluster(cat.cluster)}</p>
                                <p className="text-2xl font-light text-slate-100 mt-2">{cat.award_count} <span className="text-sm text-slate-500">award{cat.award_count !== 1 ? 's' : ''}</span></p>
                                <p className="text-sm text-civant-teal mt-0.5">{fmtEur(cat.total_value)}</p>
                                <p className="text-xs text-slate-600 mt-1">{cat.distinct_buyers} buyer{cat.distinct_buyers !== 1 ? 's' : ''} · {cat.active_contracts} active</p>
                            </div>
                        ))}
                    </div>
                    <div>
                        <h3 className="text-sm text-slate-500 uppercase tracking-wider mb-4">Recent Contracts</h3>
                        <table className="w-full text-sm">
                            <thead><tr className="border-b border-civant-border/40 text-xs text-slate-500 uppercase tracking-wider">
                                <th className="text-left py-3 pr-4 font-medium">Buyer</th><th className="text-right py-3 px-4 font-medium">Value</th><th className="text-left py-3 px-4 font-medium">Category</th><th className="text-left py-3 px-4 font-medium">Awarded</th><th className="text-left py-3 px-4 font-medium">Ends</th><th className="text-left py-3 pl-4 font-medium">Type</th>
                            </tr></thead>
                            <tbody>{recent_contracts.map((c,i) => (
                                <tr key={i} className="border-b border-civant-border/20 hover:bg-slate-800/20 transition-colors">
                                    <td className="py-4 pr-4 text-slate-200">{c.buyer_name}</td>
                                    <td className="py-4 px-4 text-right text-slate-200">{fmtEur(c.value_eur)}</td>
                                    <td className="py-4 px-4 text-slate-400">{fmtCluster(c.cpv_cluster)}</td>
                                    <td className="py-4 px-4 text-slate-400">{c.award_date}</td>
                                    <td className="py-4 px-4 text-slate-400">{c.end_date||'—'}</td>
                                    <td className="py-4 pl-4">{c.framework_flag && <span className="text-xs text-slate-500">Framework</span>}</td>
                                </tr>
                            ))}</tbody>
                        </table>
                    </div>
                    {yearly_trend.length > 0 && (
                        <div>
                            <div className="flex items-center justify-between mb-4">
                                <h3 className="text-sm text-slate-500 uppercase tracking-wider">Award Trend</h3>
                                <div className="flex items-center gap-1.5">{trend==='growing'?<TrendingUp className="h-4 w-4 text-emerald-500"/>:trend==='declining'?<TrendingDown className="h-4 w-4 text-red-500"/>:<Minus className="h-4 w-4 text-slate-400"/>}<span className="text-sm text-slate-400 capitalize">{trend}</span></div>
                            </div>
                            <div className="flex items-end gap-2 h-28">
                                {yearly_trend.map((y,i) => { const mx = Math.max(...yearly_trend.map(t=>t.awards)); const h = mx>0?(y.awards/mx)*100:0; return (
                                    <div key={i} className="flex-1 flex flex-col items-center gap-1">
                                        <span className="text-xs text-slate-600">{y.awards}</span>
                                        <div className="w-full bg-civant-teal/20 rounded-sm hover:bg-civant-teal/40 transition-colors" style={{height:`${Math.max(h,4)}%`}} title={`${y.year}: ${y.awards} awards, ${fmtEur(y.total_value)}`} />
                                        <span className="text-xs text-slate-600">{y.year}</span>
                                    </div>
                                ); })}
                            </div>
                        </div>
                    )}
                </TabsContent>

                {/* ===== BUYERS ===== */}
                <TabsContent value="buyers" className="mt-8">
                    <div className="space-y-1">
                        {buyer_relationships.map((b,i) => (
                            <div key={i} className="flex items-center justify-between py-4 border-b border-civant-border/20 hover:bg-slate-800/10 transition-colors">
                                <div className="flex-1">
                                    <div className="text-slate-200">{b.buyer_name}</div>
                                    <div className="text-xs text-slate-600 mt-1">{b.first_award} → {b.last_award}{b.active_contracts>0 && <span className="text-emerald-500 ml-2">· {b.active_contracts} active</span>}</div>
                                </div>
                                <div className="flex items-center gap-4">
                                    <div className="text-right">
                                        <div className="text-sm text-slate-200">{fmtEur(b.total_value)}</div>
                                        <div className="text-xs text-slate-600">{b.award_count} award{b.award_count !== 1 ? 's' : ''}</div>
                                    </div>
                                    <StrengthBadge strength={b.relationship_strength}/>
                                </div>
                            </div>
                        ))}
                    </div>
                </TabsContent>

                {/* ===== INSIGHTS ===== */}
                <TabsContent value="insights" className="mt-8 space-y-8">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                        {analysis?.strengths?.length > 0 && (
                            <div>
                                <h3 className="text-sm text-slate-500 uppercase tracking-wider flex items-center gap-2 mb-4"><Trophy className="h-4 w-4 text-emerald-500"/>Strengths</h3>
                                <ul className="space-y-3">{analysis.strengths.map((s,i) => <li key={i} className="flex items-start gap-3 text-sm"><CheckCircle2 className="h-4 w-4 text-emerald-500/60 mt-0.5 flex-shrink-0"/><span className="text-slate-300">{s}</span></li>)}</ul>
                            </div>
                        )}
                        {analysis?.weaknesses?.length > 0 && (
                            <div>
                                <h3 className="text-sm text-slate-500 uppercase tracking-wider flex items-center gap-2 mb-4"><AlertCircle className="h-4 w-4 text-amber-500"/>Weaknesses</h3>
                                <ul className="space-y-3">{analysis.weaknesses.map((w,i) => <li key={i} className="flex items-start gap-3 text-sm"><span className="text-amber-500/60 mt-0.5 flex-shrink-0">–</span><span className="text-slate-400">{w}</span></li>)}</ul>
                            </div>
                        )}
                    </div>
                    {analysis?.strategic_insights?.length > 0 && (
                        <div>
                            <h3 className="text-sm text-slate-500 uppercase tracking-wider flex items-center gap-2 mb-4"><Sparkles className="h-4 w-4 text-civant-teal"/>Strategic Insights</h3>
                            <ul className="space-y-3">{analysis.strategic_insights.map((s,i) => <li key={i} className="flex items-start gap-3 text-sm"><ArrowRight className="h-4 w-4 text-civant-teal/60 mt-0.5 flex-shrink-0"/><span className="text-slate-300">{s}</span></li>)}</ul>
                        </div>
                    )}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-8 pt-4 border-t border-civant-border/20">
                        <div>
                            <h3 className="text-sm text-slate-500 uppercase tracking-wider mb-4">Value Analysis</h3>
                            <div className="space-y-4 text-sm">
                                <div className="flex justify-between"><span className="text-slate-500">Average Contract</span><span className="text-slate-200">{fmtEur(summary.avg_contract_value_eur)}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Largest Win</span><span className="text-emerald-400">{fmtEur(summary.max_contract_value_eur)}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Framework Agreements</span><span className="text-slate-200">{summary.has_frameworks}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Active Contracts</span><span className="text-slate-200">{summary.active_contracts}</span></div>
                            </div>
                        </div>
                        <div>
                            <h3 className="text-sm text-slate-500 uppercase tracking-wider mb-4">Categories</h3>
                            <div className="space-y-3">{category_breakdown.slice(0,5).map((c,i) => (
                                <div key={i} className="flex items-center justify-between text-sm"><span className="text-slate-300">{fmtCluster(c.cluster)}</span><span className="text-slate-500">{c.award_count}× · <span className="text-civant-teal">{fmtEur(c.total_value)}</span></span></div>
                            ))}</div>
                        </div>
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
            const { data, error } = await supabase.rpc('get_competitor_intelligence', {
                p_tenant_id: 'civant_default',
                p_search_term: companyName
            });
            if (error) throw new Error(error.message);
            if (!data || !data.success) {
                alert(data?.message || 'No awards found for this competitor');
                return;
            }
            // Generate deterministic insights from the data
            const summary = data.summary;
            const categories = data.category_breakdown || [];
            const buyers = data.buyer_relationships || [];
            const renewals = data.renewal_opportunities || [];
            const trend_data = data.yearly_trend || [];
            let overallTrend = 'stable';
            if (trend_data.length >= 3) {
                const recent = trend_data.slice(-2);
                const earlier = trend_data.slice(-4, -2);
                const recentAvg = recent.reduce((s,t) => s+t.awards, 0) / recent.length;
                const earlierAvg = earlier.length > 0 ? earlier.reduce((s,t) => s+t.awards, 0) / earlier.length : recentAvg;
                if (recentAvg > earlierAvg * 1.2) overallTrend = 'growing';
                else if (recentAvg < earlierAvg * 0.8) overallTrend = 'declining';
            }
            const strengths = [];
            if (summary.total_awards > 20) strengths.push(`Extensive track record with ${summary.total_awards} public contracts over ${summary.years_active} years`);
            else if (summary.total_awards > 5) strengths.push(`Established presence with ${summary.total_awards} public contracts`);
            if (summary.has_frameworks > 0) strengths.push(`Holds ${summary.has_frameworks} framework agreements, indicating pre-qualified status`);
            const strongBuyers = buyers.filter(b => b.relationship_strength === 'strong');
            if (strongBuyers.length > 0) strengths.push(`Strong repeat relationships with ${strongBuyers.slice(0,3).map(b=>b.buyer_name).join(', ')}`);
            if (summary.active_contracts > 3) strengths.push(`Currently active on ${summary.active_contracts} contracts`);
            if (categories.length > 2) strengths.push(`Diversified across ${categories.length} procurement categories`);
            if (summary.max_contract_value_eur > 50000000) strengths.push(`Proven capability on large-scale contracts (up to ${fmtEur(summary.max_contract_value_eur)})`);
            const weaknesses = [];
            if (categories[0] && categories.length > 1) { const pct = (categories[0].award_count / summary.total_awards * 100); if (pct > 70) weaknesses.push(`Heavy concentration in ${fmtCluster(categories[0].cluster)} (${Math.round(pct)}% of awards)`); }
            const imminentCount = renewals.filter(r => r.window_class === 'imminent').length;
            if (imminentCount > 0) weaknesses.push(`${imminentCount} contract(s) expiring imminently`);
            if (summary.distinct_buyers < 5 && summary.total_awards > 5) weaknesses.push(`Concentrated buyer base (${summary.distinct_buyers} buyers)`);
            const insights = [];
            if (renewals.length > 0) { const tv = renewals.reduce((s,r) => s+(r.value_eur||0), 0); insights.push(`${renewals.length} contracts worth ${fmtEur(tv)} expiring in next 12 months — competitive entry points`); }
            const lowLockIn = renewals.filter(r => (r.repeat_wins || 0) <= 1);
            if (lowLockIn.length > 0) insights.push(`${lowLockIn.length} expiring contract(s) have low incumbent lock-in — strongest displacement opportunities`);
            const emergingHV = buyers.filter(b => b.relationship_strength === 'emerging' && (b.total_value||0) > 10000000);
            if (emergingHV.length > 0) insights.push(`Watch for vulnerability at ${emergingHV.slice(0,2).map(b=>b.buyer_name).join(', ')} — high-value but shallow relationships`);

            setAnalysis({
                success: true,
                company_name: companyName,
                found_tenders: summary.total_awards,
                summary,
                renewal_opportunities: renewals.map(r => ({...r, incumbent_strength: (r.repeat_wins||0) >= 3 ? 'strong_incumbent' : (r.repeat_wins||0) >= 2 ? 'moderate_incumbent' : 'low_lock_in'})),
                buyer_relationships: buyers,
                category_breakdown: categories,
                yearly_trend: trend_data,
                recent_contracts: data.recent_contracts || [],
                trading_names: data.trading_names || [],
                trend: overallTrend,
                analysis: { strengths, weaknesses, strategic_insights: insights }
            });
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
