import React, { useEffect, useState } from 'react';
import { civant } from '@/api/civantClient';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useTenant } from '@/lib/tenant';
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
import {
  Page,
  PageHeader,
  PageTitle,
  PageDescription,
  PageBody,
  Card,
  CardHeader,
  CardTitle,
  CardContent,
  Button,
  Badge
} from '@/components/ui';
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
  const { activeTenantId, isLoadingTenants } = useTenant();

  useEffect(() => {
    if (isLoadingTenants) return;
    if (!activeTenantId) return;
    setLoading(true);
    void loadData();
  }, [activeTenantId, isLoadingTenants]);

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
    const filteredTenders =
      selectedCountry === 'all' ? tenders : tenders.filter((tender) => tender.country === selectedCountry);

    const buyerPatterns = {};

    filteredTenders.forEach((tender) => {
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

      if (tender.cpv_codes) {
        const codes = tender.cpv_codes.split(',');
        codes.forEach((code) => {
          const mainCode = code.trim().substring(0, 8);
          buyerPatterns[buyer].cpvCodes[mainCode] = (buyerPatterns[buyer].cpvCodes[mainCode] || 0) + 1;
        });
      }

      if (tender.estimated_value) {
        buyerPatterns[buyer].avgValue.push(tender.estimated_value);
      }

      if (tender.publication_date) {
        const month = new Date(tender.publication_date).getMonth();
        buyerPatterns[buyer].publicationMonths[month] = (buyerPatterns[buyer].publicationMonths[month] || 0) + 1;
      }
    });

    const predictionsList = [];

    Object.values(buyerPatterns).forEach((pattern) => {
      if (pattern.tenders.length < 2) return;

      const sortedDates = pattern.tenders
        .filter((tender) => tender.publication_date)
        .map((tender) => new Date(tender.publication_date))
        .sort((a, b) => a - b);

      if (sortedDates.length < 2) return;

      const intervals = [];
      for (let index = 1; index < sortedDates.length; index += 1) {
        const months = differenceInMonths(sortedDates[index], sortedDates[index - 1]);
        if (months > 0) intervals.push(months);
      }

      if (intervals.length === 0) return;

      const avgInterval = intervals.reduce((a, b) => a + b, 0) / intervals.length;
      const lastTenderDate = sortedDates[sortedDates.length - 1];
      const monthsSinceLastTender = differenceInMonths(new Date(), lastTenderDate);

      const likelihood =
        monthsSinceLastTender >= avgInterval * 0.8 ? 'high' : monthsSinceLastTender >= avgInterval * 0.5 ? 'medium' : 'low';

      if (likelihood === 'low') return;

      const topCpvs = Object.entries(pattern.cpvCodes)
        .sort(([, a], [, b]) => b - a)
        .slice(0, 3)
        .map(([code]) => code);

      const avgValue =
        pattern.avgValue.length > 0
          ? pattern.avgValue.reduce((a, b) => a + b, 0) / pattern.avgValue.length
          : null;

      const topMonths = Object.entries(pattern.publicationMonths)
        .sort(([, a], [, b]) => b - a)
        .slice(0, 2)
        .map(([month]) => Number.parseInt(month, 10));

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
    });

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
    if (aiPredictions[buyer]) return;

    setLoadingAI(true);
    try {
      const response = await civant.functions.invoke('predictTenders', {
        buyer_name: buyer,
        country
      });

      if (response.data.success) {
        setAiPredictions((previous) => ({
          ...previous,
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
      high: 'bg-emerald-500/15 text-emerald-200 border-emerald-400/40',
      medium: 'bg-amber-500/15 text-amber-200 border-amber-400/40',
      low: 'bg-secondary text-secondary-foreground border-border'
    };
    return colors[likelihood] || colors.low;
  };

  const getCountryFlag = (country) => {
    if (country === 'FR') return '🇫🇷';
    if (country === 'IE') return '🇮🇪';
    if (country === 'ES') return '🇪🇸';
    return '🌍';
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <Page className="space-y-8">
      <PageHeader className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-4">
        <div className="space-y-3">
          <span className="inline-flex w-fit rounded-full border border-primary/30 bg-primary/15 px-3 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-primary">
            Civant Intelligence
          </span>
          <div className="space-y-2">
            <PageTitle className="flex items-center gap-2">
              <Sparkles className="h-6 w-6 text-primary" />
              Tender Panorama
            </PageTitle>
            <PageDescription>AI-powered forecasts based on historical contract award patterns.</PageDescription>
          </div>
        </div>

        <Select value={selectedCountry} onValueChange={setSelectedCountry}>
          <SelectTrigger className="w-full sm:w-48 bg-card/70 border-border text-card-foreground">
            <SelectValue placeholder="All countries" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Countries</SelectItem>
            <SelectItem value="FR">🇫🇷 France</SelectItem>
            <SelectItem value="IE">🇮🇪 Ireland</SelectItem>
            <SelectItem value="ES">🇪🇸 Spain</SelectItem>
          </SelectContent>
        </Select>
      </PageHeader>

      <PageBody>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
            <CardContent className="p-6">
              <div className="flex items-center gap-3">
                <div className="p-3 rounded-xl bg-primary/15 border border-primary/30">
                  <Target className="h-5 w-5 text-primary" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">High Likelihood</p>
                  <p className="text-2xl font-bold text-card-foreground">
                    {predictions.filter((prediction) => prediction.likelihood === 'high').length}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
            <CardContent className="p-6">
              <div className="flex items-center gap-3">
                <div className="p-3 rounded-xl bg-secondary border border-border/70">
                  <TrendingUp className="h-5 w-5 text-card-foreground" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Medium Likelihood</p>
                  <p className="text-2xl font-bold text-card-foreground">
                    {predictions.filter((prediction) => prediction.likelihood === 'medium').length}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
            <CardContent className="p-6">
              <div className="flex items-center gap-3">
                <div className="p-3 rounded-xl bg-secondary border border-border/70">
                  <Building2 className="h-5 w-5 text-primary" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Total Predictions</p>
                  <p className="text-2xl font-bold text-card-foreground">{predictions.length}</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="space-y-4 mt-4">
          {predictions.length === 0 ? (
            <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
              <CardContent className="py-14 text-center">
                <Sparkles className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
                <h3 className="text-lg font-semibold text-card-foreground mb-2">No predictions available</h3>
                <p className="text-muted-foreground">More historical data is needed to generate accurate predictions.</p>
              </CardContent>
            </Card>
          ) : (
            predictions.map((prediction, index) => {
              const aiData = aiPredictions[prediction.buyer];

              return (
                <Card
                  key={index}
                  className="border border-civant-border bg-civant-navy/55 shadow-none hover:border-primary/40 transition-colors"
                >
                  <CardContent className="p-6">
                    <div className="flex items-start gap-4">
                      <div className="flex flex-col items-center gap-2 pt-1">
                        <span className="text-2xl">{getCountryFlag(prediction.country)}</span>
                        <Badge className={`${getLikelihoodBadge(prediction.likelihood)} border text-xs uppercase`}>
                          {prediction.likelihood}
                        </Badge>
                      </div>

                      <div className="flex-1 min-w-0">
                        <div className="flex flex-col lg:flex-row lg:items-start lg:justify-between gap-3 mb-4">
                          <div className="min-w-0">
                            <h3 className="font-semibold text-card-foreground mb-1 truncate">{prediction.buyer}</h3>
                            <p className="text-sm text-muted-foreground">
                              Based on {prediction.historicalCount} historical tenders
                            </p>
                          </div>

                          <div className="flex flex-wrap gap-2">
                            <Button
                              variant="outline"
                              size="sm"
                              className="border-border/80 bg-card/40 hover:bg-card/70"
                              onClick={() =>
                                setFeedbackDialog({
                                  open: true,
                                  prediction: { predicted_date: format(prediction.predictedDate, 'yyyy-MM-dd') },
                                  buyer: prediction.buyer
                                })
                              }
                            >
                              <MessageSquare className="h-4 w-4 mr-1" />
                              Feedback
                            </Button>

                            {!aiData ? (
                              <Button
                                variant="secondary"
                                size="sm"
                                className="bg-primary/20 text-primary border border-primary/30 hover:bg-primary/25"
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
                            ) : null}

                            <Link to={createPageUrl(`Search?buyer=${encodeURIComponent(prediction.buyer)}`)}>
                              <Button variant="ghost" size="sm">
                                View History
                                <ArrowRight className="h-4 w-4 ml-1" />
                              </Button>
                            </Link>
                          </div>
                        </div>

                        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3 mb-4">
                          <div className="rounded-lg border border-border/70 bg-muted/25 p-3">
                            <div className="flex items-center gap-2 mb-1">
                              <Calendar className="h-4 w-4 text-muted-foreground" />
                              <p className="text-xs text-muted-foreground">Predicted Date</p>
                            </div>
                            <p className="text-sm font-semibold text-card-foreground">
                              {format(prediction.predictedDate, 'MMM yyyy')}
                            </p>
                          </div>

                          <div className="rounded-lg border border-border/70 bg-muted/25 p-3">
                            <div className="flex items-center gap-2 mb-1">
                              <Clock className="h-4 w-4 text-muted-foreground" />
                              <p className="text-xs text-muted-foreground">Average Interval</p>
                            </div>
                            <p className="text-sm font-semibold text-card-foreground">{prediction.avgInterval} months</p>
                          </div>

                          {prediction.avgValue ? (
                            <div className="rounded-lg border border-border/70 bg-muted/25 p-3">
                              <div className="flex items-center gap-2 mb-1">
                                <DollarSign className="h-4 w-4 text-muted-foreground" />
                                <p className="text-xs text-muted-foreground">Expected Value</p>
                              </div>
                              <p className="text-sm font-semibold text-card-foreground">
                                {new Intl.NumberFormat('en', {
                                  style: 'currency',
                                  currency: 'EUR',
                                  notation: 'compact',
                                  maximumFractionDigits: 1
                                }).format(prediction.avgValue)}
                              </p>
                            </div>
                          ) : null}

                          <div className="rounded-lg border border-border/70 bg-muted/25 p-3">
                            <div className="flex items-center gap-2 mb-1">
                              <Calendar className="h-4 w-4 text-muted-foreground" />
                              <p className="text-xs text-muted-foreground">Last Tender</p>
                            </div>
                            <p className="text-sm font-semibold text-card-foreground">
                              {format(prediction.lastTenderDate, 'MMM yyyy')}
                            </p>
                          </div>
                        </div>

                        {prediction.topCpvs.length > 0 ? (
                          <div className="mb-2">
                            <p className="text-xs text-muted-foreground mb-1">Likely Categories (CPV)</p>
                            <div className="flex flex-wrap gap-1">
                              {prediction.topCpvs.map((cpv, cpvIndex) => (
                                <Badge key={cpvIndex} variant="outline" className="text-xs border-border/70">
                                  {cpv}
                                </Badge>
                              ))}
                            </div>
                          </div>
                        ) : null}

                        {aiData && aiData.predictions ? (
                          <div className="mt-4 pt-4 border-t border-border/70">
                            <div className="flex items-center gap-2 mb-3">
                              <Sparkles className="h-4 w-4 text-primary" />
                              <h4 className="text-sm font-semibold text-card-foreground">AI-Powered Forecast</h4>
                            </div>

                            {aiData.analysis ? (
                              <div className="mb-3 p-3 rounded-lg border border-primary/20 bg-primary/10">
                                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs">
                                  <div>
                                    <span className="text-muted-foreground">Avg Interval:</span>
                                    <span className="ml-1 font-medium text-card-foreground">
                                      {Math.round(aiData.analysis.avg_interval_days || 0)} days
                                    </span>
                                  </div>
                                  <div>
                                    <span className="text-muted-foreground">Trend:</span>
                                    <span className="ml-1 font-medium text-card-foreground capitalize">
                                      {aiData.analysis.trend}
                                    </span>
                                  </div>
                                  <div>
                                    <span className="text-muted-foreground">Seasonality:</span>
                                    <span className="ml-1 font-medium text-card-foreground">
                                      {aiData.analysis.seasonality_detected ? 'Detected' : 'None'}
                                    </span>
                                  </div>
                                  <div>
                                    <span className="text-muted-foreground">Data Quality:</span>
                                    <span className="ml-1 font-medium text-card-foreground capitalize">
                                      {aiData.analysis.data_quality}
                                    </span>
                                  </div>
                                </div>
                              </div>
                            ) : null}

                            <div className="space-y-3">
                              {aiData.predictions.slice(0, 3).map((pred, predIndex) => (
                                <div key={predIndex} className="p-3 rounded-lg border border-border/70 bg-muted/25">
                                  <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 mb-2">
                                    <div className="flex items-center gap-2">
                                      <Calendar className="h-3 w-3 text-muted-foreground" />
                                      <span className="font-medium text-card-foreground">
                                        {format(new Date(pred.predicted_date), 'MMM d, yyyy')}
                                      </span>
                                    </div>
                                    <div className="flex flex-wrap gap-1">
                                      <Badge className={`${getLikelihoodBadge(pred.confidence_level)} text-xs`}>
                                        {pred.confidence_score
                                          ? `${Math.round(pred.confidence_score * 100)}%`
                                          : pred.confidence_level}
                                      </Badge>
                                      {pred.tender_type ? (
                                        <Badge variant="outline" className="text-xs border-border/70">
                                          {pred.tender_type === 'framework_renewal' && '🔄 '}
                                          {pred.tender_type === 'annual_maintenance' && '🔧 '}
                                          {pred.tender_type.replace(/_/g, ' ')}
                                        </Badge>
                                      ) : null}
                                    </div>
                                  </div>

                                  {pred.estimated_value_range ? (
                                    <div className="text-xs text-muted-foreground mb-2">
                                      <span className="font-medium text-card-foreground">Value:</span>{' '}
                                      {new Intl.NumberFormat('en', {
                                        style: 'currency',
                                        currency: 'EUR',
                                        notation: 'compact'
                                      }).format(pred.estimated_value_range.min)}
                                      {' - '}
                                      {new Intl.NumberFormat('en', {
                                        style: 'currency',
                                        currency: 'EUR',
                                        notation: 'compact'
                                      }).format(pred.estimated_value_range.max)}
                                    </div>
                                  ) : null}

                                  {pred.contract_basis ? (
                                    <div className="text-xs text-muted-foreground mb-2">
                                      <span className="font-medium text-card-foreground">Basis:</span> {pred.contract_basis}
                                    </div>
                                  ) : null}

                                  {pred.renewal_likelihood ? (
                                    <div className="text-xs text-muted-foreground mb-2">
                                      <span className="font-medium text-card-foreground">Renewal:</span> {pred.renewal_likelihood}
                                    </div>
                                  ) : null}

                                  {pred.seasonality_factor ? (
                                    <div className="text-xs text-muted-foreground mb-2">
                                      <span className="font-medium text-card-foreground">Seasonality:</span> {pred.seasonality_factor}
                                    </div>
                                  ) : null}

                                  {pred.key_indicators && pred.key_indicators.length > 0 ? (
                                    <div className="text-xs text-muted-foreground">
                                      <span className="font-medium text-card-foreground">Key Factors:</span>
                                      <ul className="mt-1 ml-4 list-disc">
                                        {pred.key_indicators.slice(0, 2).map((indicator, indicatorIndex) => (
                                          <li key={indicatorIndex}>{indicator}</li>
                                        ))}
                                      </ul>
                                    </div>
                                  ) : null}
                                </div>
                              ))}
                            </div>
                          </div>
                        ) : null}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              );
            })
          )}
        </div>

        <Card className="border border-primary/30 bg-primary/10 shadow-none mt-2">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-card-foreground">
              <Sparkles className="h-5 w-5 text-primary" />
              How Predictions Work
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li>Analyzes historical tender patterns from each buyer</li>
              <li>Calculates average intervals between tenders</li>
              <li>Identifies common CPV categories and value ranges</li>
              <li>Predicts likelihood based on time since last tender</li>
            </ul>
          </CardContent>
        </Card>
      </PageBody>

      {feedbackDialog.open ? (
        <FeedbackDialog
          prediction={feedbackDialog.prediction}
          buyerName={feedbackDialog.buyer}
          open={feedbackDialog.open}
          onOpenChange={(open) => setFeedbackDialog({ ...feedbackDialog, open })}
        />
      ) : null}
    </Page>
  );
}
