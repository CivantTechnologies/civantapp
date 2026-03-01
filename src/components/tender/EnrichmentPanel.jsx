import React, { useMemo } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { 
    Sparkles, 
    AlertTriangle, 
    CheckCircle2, 
    Target,
    FileText,
    Building2
} from 'lucide-react';

export default function EnrichmentPanel({ enrichment }) {
    if (!enrichment) return null;

    const { keyClauses, riskAssessment, evaluationCriteria, technicalReqs, contractorDetails } = useMemo(() => ({
        keyClauses: JSON.parse(enrichment.key_clauses || '[]'),
        riskAssessment: JSON.parse(enrichment.risk_assessment || '{}'),
        evaluationCriteria: JSON.parse(enrichment.evaluation_criteria || '[]'),
        technicalReqs: JSON.parse(enrichment.technical_requirements || '[]'),
        contractorDetails: JSON.parse(enrichment.contractor_details || '{}'),
    }), [enrichment]);
    
    const complexityColors = {
        low: 'bg-emerald-50 text-emerald-700 border-emerald-200',
        medium: 'bg-amber-50 text-amber-700 border-amber-200',
        high: 'bg-red-50 text-red-700 border-red-200'
    };
    
    const riskColors = {
        low: 'text-emerald-600',
        medium: 'text-amber-600',
        high: 'text-red-600'
    };
    
    return (
        <div className="space-y-4">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <Sparkles className="h-5 w-5 text-indigo-600" />
                    <h3 className="font-semibold text-slate-900">AI-Enriched Analysis</h3>
                </div>
                <div className="flex items-center gap-2">
                    <Badge className={complexityColors[enrichment.estimated_complexity]}>
                        {enrichment.estimated_complexity} complexity
                    </Badge>
                    {enrichment.confidence_score && (
                        <Badge variant="outline">
                            {Math.round(enrichment.confidence_score * 100)}% confidence
                        </Badge>
                    )}
                </div>
            </div>
            
            {/* Key Clauses */}
            {keyClauses.length > 0 && (
                <Card className="border-slate-200">
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-semibold flex items-center gap-2">
                            <FileText className="h-4 w-4 text-indigo-600" />
                            Key Clauses & Requirements
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <ul className="space-y-2 text-sm">
                            {keyClauses.map((clause, idx) => (
                                <li key={idx} className="flex items-start gap-2">
                                    <CheckCircle2 className="h-4 w-4 text-slate-400 mt-0.5 flex-shrink-0" />
                                    <span className="text-slate-700">{clause}</span>
                                </li>
                            ))}
                        </ul>
                    </CardContent>
                </Card>
            )}
            
            {/* Risk Assessment */}
            {riskAssessment.overall_risk && (
                <Card className="border-slate-200">
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-semibold flex items-center gap-2">
                            <AlertTriangle className="h-4 w-4 text-amber-600" />
                            Risk Assessment
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-3">
                        <div className="grid grid-cols-2 gap-3 text-sm">
                            {riskAssessment.complexity_risk && (
                                <div>
                                    <span className="text-slate-500">Complexity:</span>
                                    <span className={`ml-2 font-medium ${riskColors[riskAssessment.complexity_risk.toLowerCase()] || 'text-slate-700'}`}>
                                        {riskAssessment.complexity_risk}
                                    </span>
                                </div>
                            )}
                            {riskAssessment.timeline_risk && (
                                <div>
                                    <span className="text-slate-500">Timeline:</span>
                                    <span className={`ml-2 font-medium ${riskColors[riskAssessment.timeline_risk.toLowerCase()] || 'text-slate-700'}`}>
                                        {riskAssessment.timeline_risk}
                                    </span>
                                </div>
                            )}
                            {riskAssessment.compliance_risk && (
                                <div>
                                    <span className="text-slate-500">Compliance:</span>
                                    <span className={`ml-2 font-medium ${riskColors[riskAssessment.compliance_risk.toLowerCase()] || 'text-slate-700'}`}>
                                        {riskAssessment.compliance_risk}
                                    </span>
                                </div>
                            )}
                            {riskAssessment.financial_risk && (
                                <div>
                                    <span className="text-slate-500">Financial:</span>
                                    <span className={`ml-2 font-medium ${riskColors[riskAssessment.financial_risk.toLowerCase()] || 'text-slate-700'}`}>
                                        {riskAssessment.financial_risk}
                                    </span>
                                </div>
                            )}
                        </div>
                        {riskAssessment.key_concerns && riskAssessment.key_concerns.length > 0 && (
                            <div>
                                <p className="text-xs font-medium text-slate-700 mb-2">Key Concerns:</p>
                                <ul className="space-y-1 text-sm">
                                    {riskAssessment.key_concerns.map((concern, idx) => (
                                        <li key={idx} className="flex items-start gap-2">
                                            <span className="text-amber-500 flex-shrink-0">•</span>
                                            <span className="text-slate-600">{concern}</span>
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        )}
                    </CardContent>
                </Card>
            )}
            
            {/* Evaluation Criteria */}
            {evaluationCriteria.length > 0 && (
                <Card className="border-slate-200">
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-semibold flex items-center gap-2">
                            <Target className="h-4 w-4 text-indigo-600" />
                            Evaluation Criteria
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-2">
                            {evaluationCriteria.map((criteria, idx) => (
                                <div key={idx} className="flex items-center justify-between p-2 bg-slate-50 rounded-lg text-sm">
                                    <span className="font-medium text-slate-900">{criteria.criterion}</span>
                                    <div className="flex items-center gap-2">
                                        {criteria.weight && (
                                            <Badge variant="outline" className="text-xs">
                                                {criteria.weight}
                                            </Badge>
                                        )}
                                        {criteria.importance && (
                                            <Badge className={
                                                criteria.importance.toLowerCase() === 'high' ? 'bg-red-50 text-red-700' :
                                                criteria.importance.toLowerCase() === 'medium' ? 'bg-amber-50 text-amber-700' :
                                                'bg-slate-50 text-slate-600'
                                            }>
                                                {criteria.importance}
                                            </Badge>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>
            )}
            
            {/* Technical Requirements */}
            {technicalReqs.length > 0 && (
                <Card className="border-slate-200">
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-semibold">Technical Requirements</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <ul className="space-y-2 text-sm">
                            {technicalReqs.map((req, idx) => (
                                <li key={idx} className="flex items-start gap-2">
                                    <span className="text-indigo-500 flex-shrink-0">•</span>
                                    <span className="text-slate-700">{req}</span>
                                </li>
                            ))}
                        </ul>
                    </CardContent>
                </Card>
            )}
            
            {/* Contractor Details (for awards) */}
            {contractorDetails.contractor_name && (
                <Card className="border-slate-200">
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-semibold flex items-center gap-2">
                            <Building2 className="h-4 w-4 text-indigo-600" />
                            Contractor Information
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-3">
                        <div className="grid grid-cols-2 gap-3 text-sm">
                            <div>
                                <span className="text-slate-500">Contractor:</span>
                                <p className="font-medium text-slate-900">{contractorDetails.contractor_name}</p>
                            </div>
                            {contractorDetails.contract_value && (
                                <div>
                                    <span className="text-slate-500">Contract Value:</span>
                                    <p className="font-medium text-slate-900">{contractorDetails.contract_value}</p>
                                </div>
                            )}
                            {contractorDetails.contract_duration && (
                                <div>
                                    <span className="text-slate-500">Duration:</span>
                                    <p className="font-medium text-slate-900">{contractorDetails.contract_duration}</p>
                                </div>
                            )}
                        </div>
                        {contractorDetails.key_deliverables && contractorDetails.key_deliverables.length > 0 && (
                            <div>
                                <p className="text-xs font-medium text-slate-700 mb-2">Key Deliverables:</p>
                                <ul className="space-y-1 text-sm">
                                    {contractorDetails.key_deliverables.map((deliverable, idx) => (
                                        <li key={idx} className="flex items-start gap-2">
                                            <CheckCircle2 className="h-4 w-4 text-emerald-500 mt-0.5 flex-shrink-0" />
                                            <span className="text-slate-700">{deliverable}</span>
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        )}
                    </CardContent>
                </Card>
            )}
        </div>
    );
}