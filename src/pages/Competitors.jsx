import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { 
    Users, 
    Plus, 
    Edit2,
    Trash2,
    Loader2,
    Target,
    TrendingUp,
    TrendingDown,
    Minus,
    Sparkles,
    Trophy,
    AlertCircle,
    CheckCircle2,
    Building2,
    MapPin,
    DollarSign
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
} from "@/components/ui/dialog";

export default function Competitors() {
    const [user, setUser] = useState(null);
    const [competitors, setCompetitors] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showForm, setShowForm] = useState(false);
    const [editingCompetitor, setEditingCompetitor] = useState(null);
    const [saving, setSaving] = useState(false);
    const [analyzing, setAnalyzing] = useState(null);
    const [analysis, setAnalysis] = useState(null);
    
    const [formData, setFormData] = useState({
        company_name: '',
        country: '',
        industry_sectors: '',
        notes: '',
        active: true
    });
    
    useEffect(() => {
        loadData();
    }, []);
    
    const loadData = async () => {
        try {
            const userData = await civant.auth.me();
            setUser(userData);
            
            const competitorsData = await civant.entities.Competitors.filter({
                tracked_by_user: userData.email
            });
            setCompetitors(competitorsData);
        } catch (error) {
            console.error('Error loading data:', error);
        } finally {
            setLoading(false);
        }
    };
    
    const handleSubmit = async (e) => {
        e.preventDefault();
        setSaving(true);
        
        try {
            const competitorData = {
                ...formData,
                tracked_by_user: user.email
            };
            
            if (editingCompetitor) {
                await civant.entities.Competitors.update(editingCompetitor.id, competitorData);
            } else {
                await civant.entities.Competitors.create(competitorData);
            }
            
            setShowForm(false);
            setEditingCompetitor(null);
            resetForm();
            await loadData();
        } catch (error) {
            console.error('Error saving competitor:', error);
        } finally {
            setSaving(false);
        }
    };
    
    const resetForm = () => {
        setFormData({
            company_name: '',
            country: '',
            industry_sectors: '',
            notes: '',
            active: true
        });
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
    
    const handleDelete = async (competitorId) => {
        if (!confirm('Are you sure you want to remove this competitor?')) return;
        
        try {
            await civant.entities.Competitors.delete(competitorId);
            await loadData();
        } catch (error) {
            console.error('Error deleting competitor:', error);
        }
    };
    
    const analyzeCompetitor = async (companyName) => {
        setAnalyzing(companyName);
        setAnalysis(null);
        
        try {
            const response = await civant.functions.invoke('analyzeCompetitor', {
                company_name: companyName
            });
            
            if (response.data.success) {
                setAnalysis(response.data);
            }
        } catch (error) {
            console.error('Analysis failed:', error);
            alert('Analysis failed: ' + error.message);
        } finally {
            setAnalyzing(null);
        }
    };
    
    const getTrendIcon = (trend) => {
        if (trend === 'growing') return <TrendingUp className="h-4 w-4 text-emerald-600" />;
        if (trend === 'declining') return <TrendingDown className="h-4 w-4 text-red-600" />;
        return <Minus className="h-4 w-4 text-slate-300" />;
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
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
                        <Users className="h-6 w-6 text-civant-teal" />
                        Competitor Intelligence
                    </h1>
                    <p className="text-slate-400 mt-1">Track competitors and analyze their bidding patterns</p>
                </div>
                <Button 
                    className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90"
                    onClick={() => {
                        setEditingCompetitor(null);
                        resetForm();
                        setShowForm(true);
                    }}
                >
                    <Plus className="h-4 w-4 mr-2" />
                    Add Competitor
                </Button>
            </div>
            
            {/* Analysis Panel */}
            {analysis && (
                <Card className="border border-civant-border bg-gradient-to-br from-civant-navy/70 to-slate-900/70">
                    <CardHeader>
                        <div className="flex items-center justify-between">
                            <CardTitle className="text-lg font-semibold flex items-center gap-2">
                                <Sparkles className="h-5 w-5 text-civant-teal" />
                                Competitive Analysis - {analysis.company_name}
                            </CardTitle>
                            <Button 
                                variant="outline" 
                                size="sm"
                                onClick={() => setAnalysis(null)}
                            >
                                Close
                            </Button>
                        </div>
                    </CardHeader>
                    <CardContent className="space-y-6">
                        {/* Quick Stats */}
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <div className="bg-slate-900/70 p-4 rounded-lg">
                                <p className="text-xs text-slate-400">Tenders Found</p>
                                <p className="text-2xl font-bold text-slate-100">{analysis.found_tenders}</p>
                            </div>
                            {analysis.analysis?.win_analysis && (
                                <>
                                    <div className="bg-slate-900/70 p-4 rounded-lg">
                                        <p className="text-xs text-slate-400">Estimated Wins</p>
                                        <p className="text-2xl font-bold text-emerald-600">
                                            {analysis.analysis.win_analysis.estimated_wins}
                                        </p>
                                    </div>
                                    <div className="bg-slate-900/70 p-4 rounded-lg">
                                        <p className="text-xs text-slate-400">Win Rate</p>
                                        <p className="text-2xl font-bold text-civant-teal">
                                            {Math.round(analysis.analysis.win_analysis.win_rate_percentage)}%
                                        </p>
                                    </div>
                                    <div className="bg-slate-900/70 p-4 rounded-lg">
                                        <p className="text-xs text-slate-400">Trend</p>
                                        <div className="flex items-center gap-2 mt-1">
                                            {getTrendIcon(analysis.analysis.trend)}
                                            <p className="text-lg font-bold text-slate-100 capitalize">
                                                {analysis.analysis.trend}
                                            </p>
                                        </div>
                                    </div>
                                </>
                            )}
                        </div>
                        
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            {/* Strengths */}
                            {analysis.analysis?.strengths && analysis.analysis.strengths.length > 0 && (
                                <Card className="border-0 bg-slate-900/70">
                                    <CardHeader className="pb-3">
                                        <CardTitle className="text-sm font-semibold flex items-center gap-2">
                                            <Trophy className="h-4 w-4 text-emerald-600" />
                                            Strengths
                                        </CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <ul className="space-y-2 text-sm">
                                            {analysis.analysis.strengths.map((strength, idx) => (
                                                <li key={idx} className="flex items-start gap-2">
                                                    <CheckCircle2 className="h-4 w-4 text-emerald-500 mt-0.5 flex-shrink-0" />
                                                    <span className="text-slate-300">{strength}</span>
                                                </li>
                                            ))}
                                        </ul>
                                    </CardContent>
                                </Card>
                            )}
                            
                            {/* Weaknesses */}
                            {analysis.analysis?.weaknesses && analysis.analysis.weaknesses.length > 0 && (
                                <Card className="border-0 bg-slate-900/70">
                                    <CardHeader className="pb-3">
                                        <CardTitle className="text-sm font-semibold flex items-center gap-2">
                                            <AlertCircle className="h-4 w-4 text-amber-600" />
                                            Weaknesses
                                        </CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <ul className="space-y-2 text-sm">
                                            {analysis.analysis.weaknesses.map((weakness, idx) => (
                                                <li key={idx} className="flex items-start gap-2">
                                                    <span className="text-amber-500 flex-shrink-0">•</span>
                                                    <span className="text-slate-300">{weakness}</span>
                                                </li>
                                            ))}
                                        </ul>
                                    </CardContent>
                                </Card>
                            )}
                        </div>
                        
                        {/* CPV Codes & Value Analysis */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            {/* Preferred CPV Codes */}
                            {analysis.analysis?.preferred_cpv_codes && analysis.analysis.preferred_cpv_codes.length > 0 && (
                                <Card className="border-0 bg-slate-900/70">
                                    <CardHeader className="pb-3">
                                        <CardTitle className="text-sm font-semibold flex items-center gap-2">
                                            <Target className="h-4 w-4 text-civant-teal" />
                                            Preferred Categories
                                        </CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <div className="space-y-2">
                                            {analysis.analysis.preferred_cpv_codes.slice(0, 5).map((cpv, idx) => (
                                                <div key={idx} className="flex items-center justify-between text-sm">
                                                    <span className="font-medium text-slate-100">{cpv.cpv_code}</span>
                                                    <div className="flex items-center gap-2">
                                                        <Badge variant="outline" className="text-xs">
                                                            {cpv.frequency}x
                                                        </Badge>
                                                        <Badge className="bg-emerald-500/15 text-emerald-200 border border-emerald-400/40 text-xs">
                                                            {cpv.success_rate}
                                                        </Badge>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    </CardContent>
                                </Card>
                            )}
                            
                            {/* Value Analysis */}
                            {analysis.analysis?.value_analysis && (
                                <Card className="border-0 bg-slate-900/70">
                                    <CardHeader className="pb-3">
                                        <CardTitle className="text-sm font-semibold flex items-center gap-2">
                                            <DollarSign className="h-4 w-4 text-civant-teal" />
                                            Value Analysis
                                        </CardTitle>
                                    </CardHeader>
                                    <CardContent className="space-y-3 text-sm">
                                        <div>
                                            <span className="text-slate-400">Typical Range:</span>
                                            <p className="font-medium text-slate-100">{analysis.analysis.value_analysis.typical_range}</p>
                                        </div>
                                        {analysis.analysis.value_analysis.average_contract_value > 0 && (
                                            <div>
                                                <span className="text-slate-400">Average Value:</span>
                                                <p className="font-medium text-slate-100">
                                                    {new Intl.NumberFormat('en', { style: 'currency', currency: 'EUR', notation: 'compact' }).format(analysis.analysis.value_analysis.average_contract_value)}
                                                </p>
                                            </div>
                                        )}
                                        {analysis.analysis.value_analysis.largest_win > 0 && (
                                            <div>
                                                <span className="text-slate-400">Largest Win:</span>
                                                <p className="font-medium text-emerald-600">
                                                    {new Intl.NumberFormat('en', { style: 'currency', currency: 'EUR', notation: 'compact' }).format(analysis.analysis.value_analysis.largest_win)}
                                                </p>
                                            </div>
                                        )}
                                    </CardContent>
                                </Card>
                            )}
                        </div>
                        
                        {/* Key Buyers */}
                        {analysis.analysis?.key_buyers && analysis.analysis.key_buyers.length > 0 && (
                            <Card className="border-0 bg-slate-900/70">
                                <CardHeader className="pb-3">
                                    <CardTitle className="text-sm font-semibold flex items-center gap-2">
                                        <Building2 className="h-4 w-4 text-civant-teal" />
                                        Key Buyer Relationships
                                    </CardTitle>
                                </CardHeader>
                                <CardContent>
                                    <div className="space-y-2">
                                        {analysis.analysis.key_buyers.slice(0, 5).map((buyer, idx) => (
                                            <div key={idx} className="flex items-center justify-between p-2 bg-slate-900/60 rounded-lg text-sm">
                                                <span className="font-medium text-slate-100">{buyer.buyer_name}</span>
                                                <div className="flex items-center gap-2">
                                                    <Badge variant="outline" className="text-xs">
                                                        {buyer.frequency}x
                                                    </Badge>
                                                    <Badge className={
                                                        buyer.relationship_strength === 'strong' ? 'bg-emerald-500/15 text-emerald-200 border border-emerald-400/40' :
                                                        buyer.relationship_strength === 'moderate' ? 'bg-amber-500/15 text-amber-200 border border-amber-400/40' :
                                                        'bg-slate-900/60 text-slate-300'
                                                    }>
                                                        {buyer.relationship_strength}
                                                    </Badge>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </CardContent>
                            </Card>
                        )}
                        
                        {/* Strategic Insights */}
                        {analysis.analysis?.strategic_insights && analysis.analysis.strategic_insights.length > 0 && (
                            <Card className="border-0 bg-slate-900/70">
                                <CardHeader className="pb-3">
                                    <CardTitle className="text-sm font-semibold flex items-center gap-2">
                                        <Sparkles className="h-4 w-4 text-civant-teal" />
                                        Strategic Insights
                                    </CardTitle>
                                </CardHeader>
                                <CardContent>
                                    <ul className="space-y-2 text-sm">
                                        {analysis.analysis.strategic_insights.map((insight, idx) => (
                                            <li key={idx} className="flex items-start gap-2">
                                                <span className="text-indigo-500 flex-shrink-0 font-bold">→</span>
                                                <span className="text-slate-300">{insight}</span>
                                            </li>
                                        ))}
                                    </ul>
                                </CardContent>
                            </Card>
                        )}
                    </CardContent>
                </Card>
            )}
            
            {/* Competitors List */}
            <div className="grid gap-4">
                {competitors.length === 0 ? (
                    <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                        <CardContent className="py-12 text-center">
                            <Users className="h-12 w-12 mx-auto text-slate-300 mb-4" />
                            <h3 className="text-lg font-semibold text-slate-100 mb-2">No competitors tracked yet</h3>
                            <p className="text-slate-400 mb-4">Start tracking competitors to analyze their bidding patterns</p>
                            <Button onClick={() => setShowForm(true)}>
                                <Plus className="h-4 w-4 mr-2" />
                                Add Your First Competitor
                            </Button>
                        </CardContent>
                    </Card>
                ) : (
                    competitors.map(competitor => (
                        <Card key={competitor.id} className="border border-civant-border bg-civant-navy/55 shadow-none hover:bg-slate-900/70 transition-colors">
                            <CardContent className="p-6">
                                <div className="flex items-start justify-between">
                                    <div className="flex-1">
                                        <div className="flex items-center gap-3 mb-2">
                                            <h3 className="text-lg font-semibold text-slate-100">
                                                {competitor.company_name}
                                            </h3>
                                            {!competitor.active && (
                                                <Badge variant="outline" className="text-slate-400">
                                                    Inactive
                                                </Badge>
                                            )}
                                        </div>
                                        
                                        <div className="flex flex-wrap gap-2 mb-3">
                                            {competitor.country && (
                                                <Badge variant="outline" className="text-xs">
                                                    <MapPin className="h-3 w-3 mr-1" />
                                                    {competitor.country === 'both' ? 'IE & FR' : competitor.country === 'FR' ? 'France' : 'Ireland'}
                                                </Badge>
                                            )}
                                            {competitor.industry_sectors && (
                                                <Badge variant="outline" className="text-xs">
                                                    {competitor.industry_sectors}
                                                </Badge>
                                            )}
                                        </div>
                                        
                                        {competitor.notes && (
                                            <p className="text-sm text-slate-300 mb-3">{competitor.notes}</p>
                                        )}
                                        
                                        <Button
                                            variant="outline"
                                            size="sm"
                                            onClick={() => analyzeCompetitor(competitor.company_name)}
                                            disabled={analyzing === competitor.company_name}
                                        >
                                            {analyzing === competitor.company_name ? (
                                                <>
                                                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                                    Analyzing...
                                                </>
                                            ) : (
                                                <>
                                                    <Sparkles className="h-4 w-4 mr-2" />
                                                    Analyze Competitor
                                                </>
                                            )}
                                        </Button>
                                    </div>
                                    
                                    <div className="flex items-center gap-2">
                                        <Button 
                                            variant="ghost" 
                                            size="icon"
                                            onClick={() => handleEdit(competitor)}
                                        >
                                            <Edit2 className="h-4 w-4 text-slate-400" />
                                        </Button>
                                        <Button 
                                            variant="ghost" 
                                            size="icon"
                                            onClick={() => handleDelete(competitor.id)}
                                        >
                                            <Trash2 className="h-4 w-4 text-red-500" />
                                        </Button>
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    ))
                )}
            </div>
            
            {/* Add/Edit Form Dialog */}
            <Dialog open={showForm} onOpenChange={setShowForm}>
                <DialogContent className="sm:max-w-lg">
                    <DialogHeader>
                        <DialogTitle>
                            {editingCompetitor ? 'Edit Competitor' : 'Add Competitor'}
                        </DialogTitle>
                        <DialogDescription>
                            Track a competitor to analyze their bidding patterns and performance
                        </DialogDescription>
                    </DialogHeader>
                    
                    <form onSubmit={handleSubmit} className="space-y-4">
                        <div>
                            <Label htmlFor="company_name">Company Name *</Label>
                            <Input
                                id="company_name"
                                value={formData.company_name}
                                onChange={(e) => setFormData({ ...formData, company_name: e.target.value })}
                                placeholder="e.g. Acme Corp"
                                required
                            />
                        </div>
                        
                        <div>
                            <Label htmlFor="country">Primary Country</Label>
                            <Select 
                                value={formData.country} 
                                onValueChange={(value) => setFormData({ ...formData, country: value })}
                            >
                                <SelectTrigger>
                                    <SelectValue placeholder="Select country" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="IE">Ireland</SelectItem>
                                    <SelectItem value="FR">France</SelectItem>
                                    <SelectItem value="both">Both</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                        
                        <div>
                            <Label htmlFor="industry_sectors">Industry Sectors</Label>
                            <Input
                                id="industry_sectors"
                                value={formData.industry_sectors}
                                onChange={(e) => setFormData({ ...formData, industry_sectors: e.target.value })}
                                placeholder="e.g. IT Services, Construction"
                            />
                        </div>
                        
                        <div>
                            <Label htmlFor="notes">Notes</Label>
                            <Textarea
                                id="notes"
                                value={formData.notes}
                                onChange={(e) => setFormData({ ...formData, notes: e.target.value })}
                                placeholder="Any notes about this competitor..."
                                rows={3}
                            />
                        </div>
                        
                        <DialogFooter>
                            <Button 
                                type="button" 
                                variant="outline" 
                                onClick={() => {
                                    setShowForm(false);
                                    setEditingCompetitor(null);
                                    resetForm();
                                }}
                            >
                                Cancel
                            </Button>
                            <Button 
                                type="submit" 
                                className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90"
                                disabled={saving}
                            >
                                {saving ? (
                                    <>
                                        <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                        Saving...
                                    </>
                                ) : (
                                    editingCompetitor ? 'Update' : 'Add Competitor'
                                )}
                            </Button>
                        </DialogFooter>
                    </form>
                </DialogContent>
            </Dialog>
        </div>
    );
}
