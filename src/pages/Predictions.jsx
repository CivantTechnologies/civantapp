import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { 
    TrendingUp, 
    Building2,
    Calendar,
    Target,
    Loader2,
    ArrowRight,
    Sparkles,
    Clock,
    DollarSign,
    MessageSquare
} from 'lucide-react';
import FeedbackDialog from '../components/predictions/FeedbackDialog';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { format, addMonths, differenceInMonths } from 'date-fns';

export default function Predictions() {
    const [tenders, setTenders] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedCountry, setSelectedCountry] = useState('all');
    const [predictions, setPredictions] = useState([]);
    const [aiPredictions, setAiPredictions] = useState({});
    const [loadingAI, setLoadingAI] = useState(false);
    const [feedbackDialog, setFeedbackDialog] = useState({ open: false, prediction: null, buyer: null });
    
    useEffect(() => {
        loadData();
    }, []);
    
    useEffect(() => {
        if (tenders.length > 0) {
            generatePredictions();
        }
    }, [tenders, selectedCountry]);
    
    const loadData = async () => {
        try {
            const tendersData = await civant.entities.TendersCurrent.list('-publication_date', 2000);
            setTenders(tendersData);
        } catch (error) {
            console.error('Error loading data:', error);
        } finally {
            setLoading(false);
        }
    };
    
    const generatePredictions = () => {
        // Filter by country
        const filteredTenders = selectedCountry === 'all' 
            ? tenders 
            : tenders.filter(t => t.country === selectedCountry);
        
        // Group by buyer
        const buyerPatterns = {};
        
        filteredTenders.forEach(tender => {
            const buyer = tender.buyer_name || 'Unknown';
            if (!buyerPatterns[buyer]) {
                buyerPatterns[buyer] = {
                    name: buyer,
                    country: tender.country,
                    tenders: [],
                    cpvCodes: {},
                    avgValue: [],
                    publicationMonths: {}
                };
            }
            
            buyerPatterns[buyer].tenders.push(tender);
            
            // Track CPV codes
            if (tender.cpv_codes) {
                const codes = tender.cpv_codes.split(',');
                codes.forEach(code => {
                    const mainCode = code.trim().substring(0, 8);
                    buyerPatterns[buyer].cpvCodes[mainCode] = 
                        (buyerPatterns[buyer].cpvCodes[mainCode] || 0) + 1;
                });
            }
            
            // Track values
            if (tender.estimated_value) {
                buyerPatterns[buyer].avgValue.push(tender.estimated_value);
            }
            
            // Track publication months
            if (tender.publication_date) {
                const month = new Date(tender.publication_date).getMonth();
                buyerPatterns[buyer].publicationMonths[month] = 
                    (buyerPatterns[buyer].publicationMonths[month] || 0) + 1;
            }
        });
        
        // Generate predictions for buyers with sufficient history
        const predictionsList = [];
        
        Object.values(buyerPatterns).forEach(pattern => {
            if (pattern.tenders.length < 2) return;
            
            // Calculate average tender frequency (months between tenders)
            const sortedDates = pattern.tenders
                .filter(t => t.publication_date)
                .map(t => new Date(t.publication_date))
                .sort((a, b) => a - b);
            
            if (sortedDates.length < 2) return;
            
            const intervals = [];
            for (let i = 1; i < sortedDates.length; i++) {
                const months = differenceInMonths(sortedDates[i], sortedDates[i-1]);
                if (months > 0) intervals.push(months);
            }
            
            if (intervals.length === 0) return;
            
            const avgInterval = intervals.reduce((a, b) => a + b, 0) / intervals.length;
            const lastTenderDate = sortedDates[sortedDates.length - 1];
            const monthsSinceLastTender = differenceInMonths(new Date(), lastTenderDate);
            
            // Predict if a tender is likely soon
            const likelihood = monthsSinceLastTender >= (avgInterval * 0.8) ? 'high' :
                             monthsSinceLastTender >= (avgInterval * 0.5) ? 'medium' : 'low';
            
            if (likelihood !== 'low') {
                // Most common CPV codes
                const topCpvs = Object.entries(pattern.cpvCodes)
                    .sort(([,a], [,b]) => b - a)
                    .slice(0, 3)
                    .map(([code]) => code);
                
                // Average value
                const avgValue = pattern.avgValue.length > 0
                    ? pattern.avgValue.reduce((a, b) => a + b, 0) / pattern.avgValue.length
                    : null;
                
                // Most common publication months
                const topMonths = Object.entries(pattern.publicationMonths)
                    .sort(([,a], [,b]) => b - a)
                    .slice(0, 2)
                    .map(([month]) => parseInt(month));
                
                // Predicted date (last date + avg interval)
                const predictedDate = addMonths(lastTenderDate, Math.round(avgInterval));
                
                predictionsList.push({
                    buyer: pattern.name,
                    country: pattern.country,
                    likelihood,
                    predictedDate,
                    historicalCount: pattern.tenders.length,
                    avgInterval: Math.round(avgInterval),
                    topCpvs,
                    avgValue,
                    topMonths,
                    lastTenderDate
                });
            }
        });
        
        // Sort by likelihood and date
        predictionsList.sort((a, b) => {
            const likelihoodScore = { high: 3, medium: 2, low: 1 };
            if (likelihoodScore[a.likelihood] !== likelihoodScore[b.likelihood]) {
                return likelihoodScore[b.likelihood] - likelihoodScore[a.likelihood];
            }
            return a.predictedDate - b.predictedDate;
        });
        
        setPredictions(predictionsList);
    };
    
    const fetchAIPrediction = async (buyer, country) => {
        if (aiPredictions[buyer]) return; // Already loaded
        
        setLoadingAI(true);
        try {
            const response = await civant.functions.invoke('predictTenders', {
                buyer_name: buyer,
                country: country
            });
            
            if (response.data.success) {
                setAiPredictions(prev => ({
                    ...prev,
                    [buyer]: response.data
                }));
            }
        } catch (error) {
            console.error('AI prediction failed:', error);
        } finally {
            setLoadingAI(false);
        }
    };
    
    const getLikelihoodBadge = (likelihood) => {
        const colors = {
            high: 'bg-emerald-50 text-emerald-700 border-emerald-200',
            medium: 'bg-amber-50 text-amber-700 border-amber-200',
            low: 'bg-slate-900/60 text-slate-300 border-slate-700'
        };
        return colors[likelihood];
    };
    
    const getCountryFlag = (country) => {
        return country === 'FR' ? '🇫🇷' : country === 'IE' ? '🇮🇪' : country === 'ES' ? '🇪🇸' : '🌍';
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
                    <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
                        <Sparkles className="h-6 w-6 text-civant-teal" />
                        Tender Predictions
                    </h1>
                    <p className="text-slate-400 mt-1">
                        AI-powered forecasts based on historical contract award patterns
                    </p>
                </div>
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
            </div>
            
            {/* Stats */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-emerald-50">
                                <Target className="h-5 w-5 text-emerald-600" />
                            </div>
                            <div>
                                <p className="text-sm text-slate-400">High Likelihood</p>
                                <p className="text-2xl font-bold text-slate-100">
                                    {predictions.filter(p => p.likelihood === 'high').length}
                                </p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-amber-50">
                                <TrendingUp className="h-5 w-5 text-amber-600" />
                            </div>
                            <div>
                                <p className="text-sm text-slate-400">Medium Likelihood</p>
                                <p className="text-2xl font-bold text-slate-100">
                                    {predictions.filter(p => p.likelihood === 'medium').length}
                                </p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardContent className="p-6">
                        <div className="flex items-center gap-3">
                            <div className="p-3 rounded-xl bg-indigo-50">
                                <Building2 className="h-5 w-5 text-civant-teal" />
                            </div>
                            <div>
                                <p className="text-sm text-slate-400">Total Predictions</p>
                                <p className="text-2xl font-bold text-slate-100">{predictions.length}</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </div>
            
            {/* Predictions List */}
            <div className="space-y-4">
                {predictions.length === 0 ? (
                    <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                        <CardContent className="py-12 text-center">
                            <Sparkles className="h-12 w-12 mx-auto text-slate-300 mb-4" />
                            <h3 className="text-lg font-semibold text-slate-100 mb-2">
                                No predictions available
                            </h3>
                            <p className="text-slate-400">
                                More historical data is needed to generate accurate predictions
                            </p>
                        </CardContent>
                    </Card>
                ) : (
                    predictions.map((prediction, idx) => {
                        const aiData = aiPredictions[prediction.buyer];
                        
                        return (
                            <Card key={idx} className="border border-civant-border bg-civant-navy/55 shadow-none hover:bg-slate-900/70 transition-colors">
                                <CardContent className="p-6">
                                    <div className="flex items-start gap-4">
                                        {/* Flag & Icon */}
                                        <div className="flex flex-col items-center gap-2">
                                            <span className="text-2xl">{getCountryFlag(prediction.country)}</span>
                                            <Badge className={`${getLikelihoodBadge(prediction.likelihood)} border text-xs`}>
                                                {prediction.likelihood}
                                            </Badge>
                                        </div>
                                        
                                        {/* Content */}
                                        <div className="flex-1">
                                            <div className="flex items-start justify-between mb-3">
                                                <div>
                                                    <h3 className="font-semibold text-slate-100 mb-1">
                                                        {prediction.buyer}
                                                    </h3>
                                                    <p className="text-sm text-slate-400">
                                                        Based on {prediction.historicalCount} historical tenders
                                                    </p>
                                                </div>
                                                <div className="flex gap-2">
                                                    <Button 
                                                        variant="outline" 
                                                        size="sm"
                                                        onClick={() => setFeedbackDialog({ 
                                                            open: true, 
                                                            prediction: { predicted_date: format(prediction.predictedDate, 'yyyy-MM-dd') }, 
                                                            buyer: prediction.buyer 
                                                        })}
                                                    >
                                                        <MessageSquare className="h-4 w-4 mr-1" />
                                                        Feedback
                                                    </Button>
                                                    {!aiData && (
                                                        <Button 
                                                            variant="outline" 
                                                            size="sm"
                                                            onClick={() => fetchAIPrediction(prediction.buyer, prediction.country)}
                                                            disabled={loadingAI}
                                                        >
                                                            {loadingAI ? (
                                                                <Loader2 className="h-4 w-4 animate-spin" />
                                                            ) : (
                                                                <>
                                                                    <Sparkles className="h-4 w-4 mr-1" />
                                                                    AI Analysis
                                                                </>
                                                            )}
                                                        </Button>
                                                    )}
                                                    <Link to={createPageUrl(`Search?buyer=${encodeURIComponent(prediction.buyer)}`)}>
                                                        <Button variant="ghost" size="sm">
                                                            View History
                                                            <ArrowRight className="h-4 w-4 ml-1" />
                                                        </Button>
                                                    </Link>
                                                </div>
                                            </div>
                                        
                                        {/* Prediction Details */}
                                        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-3">
                                            <div className="flex items-center gap-2">
                                                <Calendar className="h-4 w-4 text-slate-400" />
                                                <div>
                                                    <p className="text-xs text-slate-400">Predicted Date</p>
                                                    <p className="text-sm font-medium text-slate-100">
                                                        {format(prediction.predictedDate, 'MMM yyyy')}
                                                    </p>
                                                </div>
                                            </div>
                                            
                                            <div className="flex items-center gap-2">
                                                <Clock className="h-4 w-4 text-slate-400" />
                                                <div>
                                                    <p className="text-xs text-slate-400">Avg Interval</p>
                                                    <p className="text-sm font-medium text-slate-100">
                                                        {prediction.avgInterval} months
                                                    </p>
                                                </div>
                                            </div>
                                            
                                            {prediction.avgValue && (
                                                <div className="flex items-center gap-2">
                                                    <DollarSign className="h-4 w-4 text-slate-400" />
                                                    <div>
                                                        <p className="text-xs text-slate-400">Expected Value</p>
                                                        <p className="text-sm font-medium text-slate-100">
                                                            {new Intl.NumberFormat('en', {
                                                                style: 'currency',
                                                                currency: 'EUR',
                                                                notation: 'compact',
                                                                maximumFractionDigits: 1
                                                            }).format(prediction.avgValue)}
                                                        </p>
                                                    </div>
                                                </div>
                                            )}
                                            
                                            <div className="flex items-center gap-2">
                                                <Calendar className="h-4 w-4 text-slate-400" />
                                                <div>
                                                    <p className="text-xs text-slate-400">Last Tender</p>
                                                    <p className="text-sm font-medium text-slate-100">
                                                        {format(prediction.lastTenderDate, 'MMM yyyy')}
                                                    </p>
                                                </div>
                                            </div>
                                        </div>
                                        
                                        {/* CPV Codes */}
                                        {prediction.topCpvs.length > 0 && (
                                            <div>
                                                <p className="text-xs text-slate-400 mb-1">Likely Categories (CPV):</p>
                                                <div className="flex flex-wrap gap-1">
                                                    {prediction.topCpvs.map((cpv, i) => (
                                                        <Badge key={i} variant="outline" className="text-xs">
                                                            {cpv}
                                                        </Badge>
                                                    ))}
                                                </div>
                                            </div>
                                        )}
                                        
                                        {/* AI Predictions */}
                                        {aiData && aiData.predictions && (
                                            <div className="mt-4 pt-4 border-t border-slate-800">
                                                <div className="flex items-center gap-2 mb-3">
                                                    <Sparkles className="h-4 w-4 text-civant-teal" />
                                                    <h4 className="text-sm font-semibold text-slate-100">AI-Powered Forecast</h4>
                                                </div>
                                                
                                                {aiData.analysis && (
                                                    <div className="mb-3 p-3 bg-indigo-50 rounded-lg">
                                                        <div className="grid grid-cols-2 gap-2 text-xs">
                                                            <div>
                                                                <span className="text-slate-300">Avg Interval:</span>
                                                                <span className="ml-1 font-medium">{Math.round(aiData.analysis.avg_interval_days)} days</span>
                                                            </div>
                                                            <div>
                                                                <span className="text-slate-300">Trend:</span>
                                                                <span className="ml-1 font-medium capitalize">{aiData.analysis.trend}</span>
                                                            </div>
                                                            <div>
                                                                <span className="text-slate-300">Seasonality:</span>
                                                                <span className="ml-1 font-medium">{aiData.analysis.seasonality_detected ? 'Detected' : 'None'}</span>
                                                            </div>
                                                            <div>
                                                                <span className="text-slate-300">Data Quality:</span>
                                                                <span className="ml-1 font-medium capitalize">{aiData.analysis.data_quality}</span>
                                                            </div>
                                                        </div>
                                                    </div>
                                                )}
                                                
                                                <div className="space-y-3">
                                                    {aiData.predictions.slice(0, 3).map((pred, i) => (
                                                        <div key={i} className="p-3 bg-slate-900/60 rounded-lg">
                                                            <div className="flex items-center justify-between mb-2">
                                                                <div className="flex items-center gap-2">
                                                                    <Calendar className="h-3 w-3 text-slate-400" />
                                                                    <span className="font-medium text-slate-100">
                                                                        {format(new Date(pred.predicted_date), 'MMM d, yyyy')}
                                                                    </span>
                                                                </div>
                                                                <div className="flex gap-1">
                                                                    <Badge className={`${getLikelihoodBadge(pred.confidence_level)} text-xs`}>
                                                                        {pred.confidence_score ? `${Math.round(pred.confidence_score * 100)}%` : pred.confidence_level}
                                                                    </Badge>
                                                                    {pred.tender_type && (
                                                                        <Badge variant="outline" className="text-xs">
                                                                            {pred.tender_type === 'framework_renewal' && '🔄'}
                                                                            {pred.tender_type === 'annual_maintenance' && '🔧'}
                                                                            {pred.tender_type.replace(/_/g, ' ')}
                                                                        </Badge>
                                                                    )}
                                                                </div>
                                                                </div>

                                                                {pred.estimated_value_range && (
                                                                <div className="text-xs text-slate-300 mb-2">
                                                                    Value: {new Intl.NumberFormat('en', { style: 'currency', currency: 'EUR', notation: 'compact' }).format(pred.estimated_value_range.min)} - {new Intl.NumberFormat('en', { style: 'currency', currency: 'EUR', notation: 'compact' }).format(pred.estimated_value_range.max)}
                                                                </div>
                                                                )}

                                                                {pred.contract_basis && (
                                                                <div className="text-xs text-slate-300 mb-2">
                                                                    <span className="font-medium">Basis:</span> {pred.contract_basis}
                                                                </div>
                                                                )}

                                                                {pred.renewal_likelihood && (
                                                                <div className="text-xs text-slate-300 mb-2">
                                                                    <span className="font-medium">Renewal:</span> {pred.renewal_likelihood}
                                                                </div>
                                                                )}
                                                            
                                                            {pred.seasonality_factor && (
                                                                <div className="text-xs text-slate-300 mb-2">
                                                                    <span className="font-medium">Seasonality:</span> {pred.seasonality_factor}
                                                                </div>
                                                            )}
                                                            
                                                            {pred.key_indicators && pred.key_indicators.length > 0 && (
                                                                <div className="text-xs text-slate-300">
                                                                    <span className="font-medium">Key Factors:</span>
                                                                    <ul className="mt-1 ml-4 list-disc">
                                                                        {pred.key_indicators.slice(0, 2).map((indicator, j) => (
                                                                            <li key={j}>{indicator}</li>
                                                                        ))}
                                                                    </ul>
                                                                </div>
                                                            )}
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    );
                    })
                )}
            </div>
            
            {/* Info Box */}
            <Card className="border-indigo-100 bg-indigo-50">
                <CardContent className="p-6">
                    <div className="flex gap-4">
                        <Sparkles className="h-5 w-5 text-civant-teal mt-0.5" />
                        <div>
                            <h3 className="font-semibold text-indigo-900 mb-2">
                                How Predictions Work
                            </h3>
                            <ul className="space-y-2 text-sm text-indigo-800">
                                <li>• Analyzes historical tender patterns from each buyer</li>
                                <li>• Calculates average intervals between tenders</li>
                                <li>• Identifies most common CPV categories and value ranges</li>
                                <li>• Predicts likelihood based on time since last tender</li>
                            </ul>
                        </div>
                    </div>
                </CardContent>
            </Card>
            
            {/* Feedback Dialog */}
            {feedbackDialog.open && (
                <FeedbackDialog
                    prediction={feedbackDialog.prediction}
                    buyerName={feedbackDialog.buyer}
                    open={feedbackDialog.open}
                    onOpenChange={(open) => setFeedbackDialog({ ...feedbackDialog, open })}
                />
            )}
        </div>
    );
}
