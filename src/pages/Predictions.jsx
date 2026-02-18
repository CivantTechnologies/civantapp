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
import { format, addDays, differenceInDays } from 'date-fns';

const COUNTRY_CODE_MAP = {
  IRELAND: 'IE',
  IRL: 'IE',
  IRE: 'IE',
  IE: 'IE',
  FRANCE: 'FR',
  FRA: 'FR',
  FR: 'FR',
  SPAIN: 'ES',
  ESPANA: 'ES',
  'ESPAÑA': 'ES',
  ESP: 'ES',
  ES: 'ES'
};

function inferCountryFromSource(source) {
  const normalized = String(source || '').trim().toUpperCase();
  if (!normalized) return '';
  if (normalized.includes('ETENDERS_IE')) return 'IE';
  if (normalized.includes('BOAMP_FR')) return 'FR';
  if (normalized.includes('PLACSP_ES')) return 'ES';
  return '';
}

function normalizeCountryCode(value, source) {
  const key = String(value || '').trim().toUpperCase();
  if (COUNTRY_CODE_MAP[key]) return COUNTRY_CODE_MAP[key];
  return inferCountryFromSource(source) || key;
}

function normalizeBuyerKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function parseCpvCodes(value) {
  const out = [];
  const seen = new Set();

  const pushCode = (raw) => {
    const digits = String(raw || '').replace(/\D/g, '').slice(0, 8);
    if (digits.length < 2 || seen.has(digits)) return;
    seen.add(digits);
    out.push(digits);
  };

  const walk = (input) => {
    if (!input) return;
    if (Array.isArray(input)) {
      input.forEach(walk);
      return;
    }
    if (typeof input === 'object') {
      Object.values(input).forEach(walk);
      return;
    }

    const text = String(input || '').trim();
    if (!text) return;

    if ((text.startsWith('[') && text.endsWith(']')) || (text.startsWith('{') && text.endsWith('}'))) {
      try {
        walk(JSON.parse(text));
      } catch {
        // continue with regex parse
      }
    }

    const matches = text.match(/\d{2,8}/g) || [];
    matches.forEach(pushCode);
  };

  walk(value);
  return out;
}

function parseAmount(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const normalized = String(value || '').replace(/[^0-9.-]/g, '');
  if (!normalized) return null;
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const half = Math.floor(sorted.length / 2);
  if (sorted.length % 2) return sorted[half];
  return (sorted[half - 1] + sorted[half]) / 2;
}

function pickBestBuyerName(namesByCount, fallback = 'Unknown') {
  let winner = fallback;
  let maxCount = -1;
  Object.entries(namesByCount || {}).forEach(([name, count]) => {
    const asNumber = Number(count || 0);
    if (asNumber > maxCount || (asNumber === maxCount && name.length > winner.length)) {
      winner = name;
      maxCount = asNumber;
    }
  });
  return winner;
}

function signalIndexKey(countryCode, buyerName) {
  const country = normalizeCountryCode(countryCode);
  const buyerKey = normalizeBuyerKey(buyerName);
  if (!country || !buyerKey) return '';
  return `${country}|${buyerKey}`;
}

export default function Predictions() {
  const [tenders, setTenders] = useState([]);
  const [externalSignalsByBuyer, setExternalSignalsByBuyer] = useState({});
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
    if (tenders.length > 0) generatePredictions();
    else setPredictions([]);
  }, [tenders, selectedCountry, externalSignalsByBuyer]);

  const loadData = async () => {
    try {
      const tendersData = await civant.entities.canonical_tenders.filter(
        { tenant_id: activeTenantId },
        '-last_seen_at',
        5000
      );

      const normalizedTenders = (Array.isArray(tendersData) ? tendersData : [])
        .map((tender) => {
          const country = normalizeCountryCode(
            tender.country || tender.country_code || tender.country_iso,
            tender.source
          );
          const publicationDate =
            String(tender.publication_date || tender.first_seen_at || tender.last_seen_at || '').slice(0, 10);
          const buyerName =
            tender.buyer_name_raw ||
            tender.buyer_name ||
            tender.buyer_name_norm ||
            'Unknown';
          const estimatedValue = parseAmount(
            tender.estimated_value ??
            tender.data?.estimated_value ??
            tender.normalized_json?.estimated_value
          );

          return {
            ...tender,
            id: tender.canonical_id || tender.id,
            buyer_name: buyerName,
            country,
            publication_date: publicationDate,
            cpv_codes: parseCpvCodes(tender.cpv_codes),
            estimated_value: estimatedValue
          };
        })
        .filter((tender) => Boolean(tender.id) && Boolean(tender.publication_date) && Boolean(tender.country));

      setTenders(normalizedTenders);

      const signalIndex = {};
      const rollupSources = [
        { entity: 'external_signal_rollup_ie', country: 'IE' },
        { entity: 'external_signal_rollup_fr', country: 'FR' },
        { entity: 'external_signal_rollup_es', country: 'ES' }
      ];

      const rollupResults = await Promise.allSettled(
        rollupSources.map(({ entity }) => civant.entities[entity].filter({ tenant_id: activeTenantId }, '-combined_external_strength_90d', 5000))
      );

      rollupResults.forEach((result, index) => {
        const { country, entity } = rollupSources[index];
        if (result.status !== 'fulfilled') {
          console.warn(`External rollup unavailable for ${country}: ${entity}`, result.reason);
          return;
        }

        const rows = Array.isArray(result.value) ? result.value : [];
        rows.forEach((row) => {
          const keys = [
            signalIndexKey(country, row.buyer_name_normalized),
            signalIndexKey(country, row.buyer_key),
            signalIndexKey(country, row.buyer_id)
          ].filter(Boolean);

          keys.forEach((key) => {
            if (!signalIndex[key]) signalIndex[key] = row;
          });
        });
      });

      setExternalSignalsByBuyer(signalIndex);
    } catch (error) {
      console.error('Error loading data:', error);
    } finally {
      setLoading(false);
    }
  };

  const generatePredictions = () => {
    const filteredTenders =
      selectedCountry === 'all'
        ? tenders
        : tenders.filter((tender) => normalizeCountryCode(tender.country) === selectedCountry);

    const buyerPatterns = {};

    filteredTenders.forEach((tender) => {
      const buyerName = String(tender.buyer_name || 'Unknown').trim() || 'Unknown';
      const country = normalizeCountryCode(tender.country, tender.source);
      const buyerKey = normalizeBuyerKey(buyerName);
      if (!country || !buyerKey) return;

      const key = `${country}|${buyerKey}`;
      if (!buyerPatterns[key]) {
        buyerPatterns[key] = {
          key,
          buyerKey,
          displayNames: {},
          country,
          tenders: [],
          cpvCodes: {},
          avgValue: [],
          publicationMonths: {}
        };
      }

      buyerPatterns[key].displayNames[buyerName] = (buyerPatterns[key].displayNames[buyerName] || 0) + 1;
      buyerPatterns[key].tenders.push(tender);

      parseCpvCodes(tender.cpv_codes).forEach((code) => {
        if (!code) return;
        const mainCode = code.trim().substring(0, 8);
        buyerPatterns[key].cpvCodes[mainCode] = (buyerPatterns[key].cpvCodes[mainCode] || 0) + 1;
      });

      if (Number.isFinite(tender.estimated_value) && tender.estimated_value > 0) {
        buyerPatterns[key].avgValue.push(Number(tender.estimated_value));
      }

      if (tender.publication_date) {
        const publicationDate = new Date(tender.publication_date);
        if (!Number.isNaN(publicationDate.getTime())) {
          const month = publicationDate.getMonth();
          buyerPatterns[key].publicationMonths[month] = (buyerPatterns[key].publicationMonths[month] || 0) + 1;
        }
      }
    });

    const predictionsList = [];

    Object.values(buyerPatterns).forEach((pattern) => {
      if (pattern.tenders.length < 2) return;

      const uniquePublicationDays = [...new Set(
        pattern.tenders
          .map((tender) => String(tender.publication_date || '').slice(0, 10))
          .filter(Boolean)
      )];

      const sortedDates = uniquePublicationDays
        .map((day) => new Date(day))
        .filter((dateValue) => !Number.isNaN(dateValue.getTime()))
        .sort((a, b) => a - b);

      if (sortedDates.length < 2) return;

      const intervals = [];
      for (let index = 1; index < sortedDates.length; index += 1) {
        const days = differenceInDays(sortedDates[index], sortedDates[index - 1]);
        if (days > 0) intervals.push(days);
      }

      if (!intervals.length) return;

      const avgIntervalDays = intervals.reduce((a, b) => a + b, 0) / intervals.length;
      const medianIntervalDays = median(intervals);
      if (!Number.isFinite(medianIntervalDays) || medianIntervalDays <= 0) return;

      const variance = intervals.reduce((sum, days) => sum + ((days - avgIntervalDays) ** 2), 0) / intervals.length;
      const stdDev = Math.sqrt(variance);
      const cadenceConsistency = clamp(1 - (stdDev / Math.max(avgIntervalDays, 1)), 0, 1);
      const sampleCoverage = clamp(intervals.length / 8, 0, 1);

      const lastTenderDate = sortedDates[sortedDates.length - 1];
      const daysSinceLastTender = Math.max(0, differenceInDays(new Date(), lastTenderDate));
      const recurrenceProgress = clamp(daysSinceLastTender / medianIntervalDays, 0, 1.35);

      const buyerSignal =
        externalSignalsByBuyer[signalIndexKey(pattern.country, pickBestBuyerName(pattern.displayNames))] || null;

      let signalBoost = 0;
      let signalEvidence = 0;
      if (buyerSignal) {
        const combinedStrength = Number(buyerSignal.combined_external_strength_90d || 0);
        const hiringCount30d = Number(buyerSignal.hiring_count_30d || 0);
        const fundingCount30d = Number(buyerSignal.funding_count_30d || 0);

        if (combinedStrength >= 0.45) signalBoost += 0.22;
        else if (combinedStrength >= 0.3) signalBoost += 0.14;
        else if (combinedStrength > 0) signalBoost += 0.08;

        if (hiringCount30d + fundingCount30d >= 2) signalBoost += 0.06;

        signalEvidence = clamp((combinedStrength * 1.5) + (hiringCount30d + fundingCount30d >= 2 ? 0.2 : 0), 0, 1);
      }

      const confidenceScore = clamp(
        (cadenceConsistency * 0.38) +
        (sampleCoverage * 0.27) +
        (Math.min(1, recurrenceProgress) * 0.2) +
        (signalEvidence * 0.15),
        0.1,
        0.99
      );

      const compositeScore = clamp(
        (recurrenceProgress * 0.82) +
        (confidenceScore * 0.18) +
        signalBoost,
        0,
        1.5
      );
      const likelihood = compositeScore >= 0.95 ? 'high' : compositeScore >= 0.65 ? 'medium' : 'low';

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

      const predictedDate = addDays(lastTenderDate, Math.max(1, Math.round(medianIntervalDays)));
      const avgIntervalMonths = Math.max(1, Math.round(avgIntervalDays / 30));

      predictionsList.push({
        buyer: pickBestBuyerName(pattern.displayNames),
        country: pattern.country,
        likelihood,
        predictedDate,
        historicalCount: pattern.tenders.length,
        avgInterval: avgIntervalMonths,
        topCpvs,
        avgValue,
        topMonths,
        lastTenderDate,
        confidenceScore,
        recurrenceProgress,
        cadenceConsistency,
        externalSignal: buyerSignal
          ? {
              strength90d: Number(buyerSignal.combined_external_strength_90d || 0),
              hiringCount30d: Number(buyerSignal.hiring_count_30d || 0),
              fundingCount30d: Number(buyerSignal.funding_count_30d || 0)
            }
          : null,
        compositeScore
      });
    });

    predictionsList.sort((a, b) => {
      const likelihoodScore = { high: 3, medium: 2, low: 1 };
      if (likelihoodScore[a.likelihood] !== likelihoodScore[b.likelihood]) {
        return likelihoodScore[b.likelihood] - likelihoodScore[a.likelihood];
      }
      if ((b.compositeScore || 0) !== (a.compositeScore || 0)) {
        return (b.compositeScore || 0) - (a.compositeScore || 0);
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
    const normalized = normalizeCountryCode(country);
    if (normalized === 'FR') return '🇫🇷';
    if (normalized === 'IE') return '🇮🇪';
    if (normalized === 'ES') return '🇪🇸';
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
            <PageDescription>AI-powered forecasts based on canonical tender cadence and external signals.</PageDescription>
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
                            <p className="text-xs text-muted-foreground mt-1">
                              Confidence {Math.round((prediction.confidenceScore || 0) * 100)}%
                            </p>
                            {prediction.externalSignal ? (
                              <p className="text-xs text-primary mt-1">
                                External signals: strength {prediction.externalSignal.strength90d.toFixed(2)} (H30
                                {prediction.externalSignal.hiringCount30d} / F30 {prediction.externalSignal.fundingCount30d})
                              </p>
                            ) : null}
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
