import React, { useMemo } from 'react';
import { Badge } from '@/components/ui/badge';
import {
    Sparkles,
    AlertTriangle,
    CheckCircle2,
    Target,
    Clock,
    Building2,
    Shield,
    Users,
    ExternalLink,
    TrendingUp,
    TrendingDown,
    Minus
} from 'lucide-react';

const riskColor = (level) => {
    const l = String(level || '').toLowerCase();
    if (l === 'low') return 'text-emerald-400';
    if (l === 'medium') return 'text-amber-400';
    if (l === 'high') return 'text-red-400';
    return 'text-slate-400';
};

const riskBg = (level) => {
    const l = String(level || '').toLowerCase();
    if (l === 'low') return 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20';
    if (l === 'medium') return 'bg-amber-500/10 text-amber-400 border-amber-500/20';
    if (l === 'high') return 'bg-red-500/10 text-red-400 border-red-500/20';
    return 'bg-slate-500/10 text-slate-400 border-slate-500/20';
};

const recBg = (rec) => {
    const r = String(rec || '').toLowerCase();
    if (r === 'bid') return 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30';
    if (r === 'monitor') return 'bg-amber-500/15 text-amber-300 border-amber-500/30';
    if (r === 'pass') return 'bg-red-500/15 text-red-300 border-red-500/30';
    return 'bg-slate-500/10 text-slate-400 border-slate-500/20';
};

const RecIcon = ({ rec }) => {
    const r = String(rec || '').toLowerCase();
    if (r === 'bid') return <TrendingUp className="h-4 w-4" />;
    if (r === 'pass') return <TrendingDown className="h-4 w-4" />;
    return <Minus className="h-4 w-4" />;
};

function Section({ icon: Icon, title, children }) {
    return (
        <div className="space-y-3">
            <h4 className="text-sm font-semibold text-slate-200 flex items-center gap-2">
                <Icon className="h-4 w-4 text-civant-teal" />
                {title}
            </h4>
            {children}
        </div>
    );
}

export default function EnrichmentPanel({ enrichment }) {
    if (!enrichment) return null;

    const {
        summary,
        bid_recommendation,
        bid_reasoning,
        opportunity_score,
        key_requirements,
        evaluation_criteria,
        risk_assessment,
        competitive_landscape,
        timing_analysis,
        buyer_context,
        sources,
        model_used,
        research_cost_usd,
    } = enrichment;

    const reqs = useMemo(() => {
        if (Array.isArray(key_requirements)) return key_requirements;
        try { return JSON.parse(key_requirements || '[]'); } catch { return []; }
    }, [key_requirements]);

    const criteria = useMemo(() => {
        if (Array.isArray(evaluation_criteria)) return evaluation_criteria;
        try { return JSON.parse(evaluation_criteria || '[]'); } catch { return []; }
    }, [evaluation_criteria]);

    const risk = useMemo(() => {
        if (typeof risk_assessment === 'object' && risk_assessment !== null) return risk_assessment;
        try { return JSON.parse(risk_assessment || '{}'); } catch { return {}; }
    }, [risk_assessment]);

    const competition = useMemo(() => {
        if (typeof competitive_landscape === 'object' && competitive_landscape !== null) return competitive_landscape;
        try { return JSON.parse(competitive_landscape || '{}'); } catch { return {}; }
    }, [competitive_landscape]);

    const timing = useMemo(() => {
        if (typeof timing_analysis === 'object' && timing_analysis !== null) return timing_analysis;
        try { return JSON.parse(timing_analysis || '{}'); } catch { return {}; }
    }, [timing_analysis]);

    const buyer = useMemo(() => {
        if (typeof buyer_context === 'object' && buyer_context !== null) return buyer_context;
        try { return JSON.parse(buyer_context || '{}'); } catch { return {}; }
    }, [buyer_context]);

    const srcList = useMemo(() => {
        if (Array.isArray(sources)) return sources;
        try { return JSON.parse(sources || '[]'); } catch { return []; }
    }, [sources]);

    const scoreColor = opportunity_score >= 60 ? 'text-emerald-400' : opportunity_score >= 40 ? 'text-amber-400' : 'text-red-400';
    const scoreBg = opportunity_score >= 60 ? 'bg-emerald-500/10' : opportunity_score >= 40 ? 'bg-amber-500/10' : 'bg-red-500/10';

    return (
        <div className="space-y-6">
            {/* Header with recommendation and score */}
            <div className="flex flex-col sm:flex-row sm:items-start gap-4">
                <div className="flex items-center gap-3">
                    <Sparkles className="h-5 w-5 text-civant-teal flex-shrink-0" />
                    <h3 className="font-semibold text-slate-100 text-base">Civant Agent Intelligence Brief</h3>
                </div>
                <div className="flex items-center gap-2 sm:ml-auto flex-wrap">
                    {bid_recommendation && (
                        <Badge className={`${recBg(bid_recommendation)} border px-3 py-1 text-xs font-semibold uppercase tracking-wide`}>
                            <RecIcon rec={bid_recommendation} />
                            <span className="ml-1.5">{bid_recommendation}</span>
                        </Badge>
                    )}
                    {typeof opportunity_score === 'number' && (
                        <div className={`${scoreBg} rounded-lg px-3 py-1.5 flex items-center gap-1.5`}>
                            <span className={`text-lg font-bold ${scoreColor}`}>{opportunity_score}</span>
                            <span className="text-xs text-slate-400">/100</span>
                        </div>
                    )}
                </div>
            </div>

            {/* Summary */}
            {summary && (
                <p className="text-sm text-slate-300 leading-relaxed">{summary}</p>
            )}

            {/* Bid reasoning */}
            {bid_reasoning && (
                <div className="bg-white/[0.03] border border-white/[0.06] rounded-lg p-4">
                    <p className="text-sm text-slate-300">
                        <span className="font-medium text-slate-200">Recommendation: </span>
                        {bid_reasoning}
                    </p>
                </div>
            )}

            {/* Key Requirements */}
            {reqs.length > 0 && (
                <Section icon={CheckCircle2} title="Key Requirements">
                    <div className="space-y-2">
                        {reqs.map((r, i) => (
                            <div key={i} className="flex items-start gap-3 bg-white/[0.02] rounded-lg p-3">
                                <CheckCircle2 className="h-4 w-4 text-slate-500 mt-0.5 flex-shrink-0" />
                                <div className="flex-1 min-w-0">
                                    <span className="text-sm text-slate-200">{r.requirement || r}</span>
                                    {r.notes && <p className="text-xs text-slate-400 mt-1">{r.notes}</p>}
                                </div>
                                {r.complexity && (
                                    <Badge variant="outline" className={`${riskBg(r.complexity)} text-xs flex-shrink-0`}>
                                        {r.complexity}
                                    </Badge>
                                )}
                            </div>
                        ))}
                    </div>
                </Section>
            )}

            {/* Evaluation Criteria */}
            {criteria.length > 0 && (
                <Section icon={Target} title="Evaluation Criteria">
                    <div className="space-y-2">
                        {criteria.map((c, i) => (
                            <div key={i} className="flex items-center justify-between bg-white/[0.02] rounded-lg p-3">
                                <div className="min-w-0">
                                    <span className="text-sm font-medium text-slate-200">{c.criterion}</span>
                                    {c.notes && <p className="text-xs text-slate-400 mt-0.5">{c.notes}</p>}
                                </div>
                                <div className="flex items-center gap-2 flex-shrink-0 ml-3">
                                    {c.weight && (
                                        <Badge variant="outline" className="text-xs border-slate-600 text-slate-300">
                                            {c.weight}
                                        </Badge>
                                    )}
                                    {c.importance && (
                                        <Badge className={`${riskBg(c.importance === 'high' ? 'high' : c.importance === 'medium' ? 'medium' : 'low')} text-xs`}>
                                            {c.importance}
                                        </Badge>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>
                </Section>
            )}

            {/* Risk Assessment */}
            {risk.overall_risk && (
                <Section icon={AlertTriangle} title="Risk Assessment">
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        {['overall_risk', 'timeline_risk', 'compliance_risk', 'competition_risk'].map(key => {
                            if (!risk[key]) return null;
                            const label = key.replace('_risk', '').replace('overall', 'overall');
                            return (
                                <div key={key} className="bg-white/[0.02] rounded-lg p-3 text-center">
                                    <p className="text-xs text-slate-400 capitalize mb-1">{label}</p>
                                    <p className={`text-sm font-semibold capitalize ${riskColor(risk[key])}`}>{risk[key]}</p>
                                </div>
                            );
                        })}
                    </div>
                    {risk.key_concerns?.length > 0 && (
                        <div className="mt-3">
                            <p className="text-xs font-medium text-slate-400 mb-2">Key Concerns</p>
                            <ul className="space-y-1.5">
                                {risk.key_concerns.map((c, i) => (
                                    <li key={i} className="flex items-start gap-2 text-sm text-slate-300">
                                        <span className="text-amber-500 flex-shrink-0 mt-1">&#x2022;</span>
                                        {c}
                                    </li>
                                ))}
                            </ul>
                        </div>
                    )}
                    {risk.mitigations?.length > 0 && (
                        <div className="mt-3">
                            <p className="text-xs font-medium text-slate-400 mb-2">Mitigations</p>
                            <ul className="space-y-1.5">
                                {risk.mitigations.map((m, i) => (
                                    <li key={i} className="flex items-start gap-2 text-sm text-slate-300">
                                        <Shield className="h-3.5 w-3.5 text-emerald-500 flex-shrink-0 mt-0.5" />
                                        {m}
                                    </li>
                                ))}
                            </ul>
                        </div>
                    )}
                </Section>
            )}

            {/* Competitive Landscape */}
            {(competition.incumbent || competition.likely_competitors?.length > 0 || competition.market_notes) && (
                <Section icon={Users} title="Competitive Landscape">
                    <div className="space-y-3">
                        {competition.incumbent && (
                            <div className="bg-white/[0.02] rounded-lg p-3">
                                <p className="text-xs text-slate-400 mb-1">Incumbent</p>
                                <p className="text-sm font-medium text-slate-200">{competition.incumbent}</p>
                                {competition.incumbent_advantage && (
                                    <p className="text-xs text-slate-400 mt-1">{competition.incumbent_advantage}</p>
                                )}
                            </div>
                        )}
                        {competition.likely_competitors?.length > 0 && (
                            <div>
                                <p className="text-xs text-slate-400 mb-2">Likely Competitors</p>
                                <div className="flex flex-wrap gap-2">
                                    {competition.likely_competitors.map((c, i) => (
                                        <Badge key={i} variant="outline" className="border-slate-600 text-slate-300 text-xs">{c}</Badge>
                                    ))}
                                </div>
                            </div>
                        )}
                        {competition.market_notes && (
                            <p className="text-sm text-slate-300">{competition.market_notes}</p>
                        )}
                    </div>
                </Section>
            )}

            {/* Timing Analysis */}
            {(timing.recommendation || timing.key_dates) && (
                <Section icon={Clock} title="Timing Analysis">
                    <div className="bg-white/[0.02] rounded-lg p-3 space-y-2">
                        {timing.deadline_pressure && (
                            <div className="flex items-center gap-2">
                                <span className="text-xs text-slate-400">Deadline pressure:</span>
                                <span className={`text-sm font-medium capitalize ${riskColor(timing.deadline_pressure)}`}>
                                    {timing.deadline_pressure}
                                </span>
                            </div>
                        )}
                        {timing.key_dates && (
                            <p className="text-sm text-slate-300">{timing.key_dates}</p>
                        )}
                        {timing.recommendation && (
                            <p className="text-sm text-slate-300">
                                <span className="font-medium text-slate-200">Plan: </span>
                                {timing.recommendation}
                            </p>
                        )}
                    </div>
                </Section>
            )}

            {/* Buyer Context */}
            {(buyer.organization_type || buyer.recent_activity) && (
                <Section icon={Building2} title="Buyer Context">
                    <div className="bg-white/[0.02] rounded-lg p-3 space-y-2">
                        <div className="flex flex-wrap gap-2">
                            {buyer.organization_type && (
                                <Badge variant="outline" className="border-slate-600 text-slate-300 text-xs capitalize">{buyer.organization_type.replace(/_/g, ' ')}</Badge>
                            )}
                            {buyer.size_indicator && (
                                <Badge variant="outline" className="border-slate-600 text-slate-300 text-xs capitalize">{buyer.size_indicator}</Badge>
                            )}
                            {buyer.procurement_maturity && (
                                <Badge variant="outline" className="border-slate-600 text-slate-300 text-xs capitalize">{buyer.procurement_maturity}</Badge>
                            )}
                        </div>
                        {buyer.recent_activity && (
                            <p className="text-sm text-slate-300">{buyer.recent_activity}</p>
                        )}
                        {buyer.relationship_notes && (
                            <p className="text-sm text-slate-400 italic">{buyer.relationship_notes}</p>
                        )}
                    </div>
                </Section>
            )}

            {/* Sources */}
            {srcList.length > 0 && (
                <div className="pt-3 border-t border-white/[0.06]">
                    <p className="text-xs font-medium text-slate-400 mb-2">Sources</p>
                    <div className="flex flex-wrap gap-x-4 gap-y-1">
                        {srcList.map((s, i) => (
                            s.url ? (
                                <a
                                    key={i}
                                    href={s.url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="inline-flex items-center gap-1 text-xs text-civant-teal hover:underline"
                                >
                                    <ExternalLink className="h-3 w-3" />
                                    {s.title || s.url}
                                </a>
                            ) : (
                                <span key={i} className="text-xs text-slate-400">{s.title}</span>
                            )
                        ))}
                    </div>
                </div>
            )}

            {/* Meta */}
            <div className="flex items-center gap-3 pt-2 border-t border-white/[0.06]">
                <span className="text-[10px] text-slate-500">Powered by {model_used || 'Civant Agent'}</span>
                {research_cost_usd > 0 && (
                    <span className="text-[10px] text-slate-500">Cost: ${Number(research_cost_usd).toFixed(4)}</span>
                )}
            </div>
        </div>
    );
}
