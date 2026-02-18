import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { format } from 'date-fns';
import { Building2, Calendar, Clock, Loader2, Target, TrendingUp } from 'lucide-react';

import { civant } from '@/api/civantClient';
import { useTenant } from '@/lib/tenant';
import { createPageUrl } from '../utils';
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Page,
  PageBody,
  PageDescription,
  PageHeader,
  PageTitle
} from '@/components/ui';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';

const COUNTRY_OPTIONS = [
  { value: 'all', label: 'All Countries' },
  { value: 'IE', label: 'Ireland' },
  { value: 'FR', label: 'France' },
  { value: 'ES', label: 'Spain' }
];

const COUNTRY_FLAGS = {
  IE: '🇮🇪',
  FR: '🇫🇷',
  ES: '🇪🇸'
};

const WINDOW_LABELS = {
  next_30: 'Next 30 days',
  m1_3: '1-3 months',
  m3_6: '3-6 months',
  m6_12: '6-12 months'
};

function getConfidenceBadgeClass(band) {
  switch (String(band || '').toLowerCase()) {
    case 'very high':
      return 'bg-civant-teal/15 text-civant-teal border-civant-teal/40';
    case 'high':
      return 'bg-blue-500/15 text-blue-300 border-blue-400/35';
    case 'medium':
      return 'bg-amber-500/15 text-amber-300 border-amber-400/35';
    default:
      return 'bg-slate-500/15 text-slate-300 border-slate-400/30';
  }
}

function formatPct(value) {
  const n = Number(value || 0);
  return `${Math.round(n * 100)}%`;
}

function formatDate(value) {
  if (!value) return 'N/A';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return format(d, 'MMM d, yyyy');
}

function toMap(rows, keyField) {
  const out = new Map();
  (rows || []).forEach((row) => {
    const key = row?.[keyField];
    if (!key) return;
    out.set(key, row);
  });
  return out;
}

function toGroupedDrivers(rows) {
  const out = new Map();
  (rows || []).forEach((row) => {
    const key = row?.prediction_id;
    if (!key) return;
    const list = out.get(key) || [];
    list.push(row);
    out.set(key, list);
  });

  for (const [key, list] of out.entries()) {
    list.sort((a, b) => Number(b?.contribution || 0) - Number(a?.contribution || 0));
    out.set(key, list);
  }

  return out;
}

export default function Predictions() {
  const { activeTenantId, isLoadingTenants } = useTenant();

  const [selectedCountry, setSelectedCountry] = useState('all');
  const [loading, setLoading] = useState(true);
  const [predictions, setPredictions] = useState([]);
  const [scorecardsByPrediction, setScorecardsByPrediction] = useState(new Map());
  const [driversByPrediction, setDriversByPrediction] = useState(new Map());
  const [latestRuns, setLatestRuns] = useState([]);

  useEffect(() => {
    if (isLoadingTenants) return;
    if (!activeTenantId) return;
    void loadForecast();
  }, [activeTenantId, isLoadingTenants, selectedCountry]);

  const loadForecast = async () => {
    setLoading(true);

    try {
      const baseQuery = { tenant_id: activeTenantId };
      if (selectedCountry !== 'all') {
        baseQuery.region = selectedCountry;
      }

      const predictionFields = [
        'prediction_id',
        'buyer_entity_id',
        'buyer_display_name',
        'cpv_cluster_id',
        'cpv_cluster_label',
        'region',
        'next_window_label',
        'expected_window_start',
        'expected_window_end',
        'probability',
        'confidence',
        'confidence_band',
        'forecast_score',
        'fallback_tier',
        'drivers_count',
        'evidence_count',
        'last_computed_at'
      ].join(',');

      const predictionRows = await civant.entities.predictions_current.filter(
        baseQuery,
        '-forecast_score,-probability',
        250,
        0,
        predictionFields
      );

      setPredictions(Array.isArray(predictionRows) ? predictionRows : []);

      const ids = (predictionRows || [])
        .map((row) => row?.prediction_id)
        .filter(Boolean);

      if (ids.length > 0) {
        const [scorecards, drivers] = await Promise.all([
          civant.entities.prediction_scorecard.filter(
            { tenant_id: activeTenantId, prediction_id: ids },
            '-total_score',
            500,
            0,
            'prediction_id,total_score,cycle_score,timing_score,behavioural_score,structural_score,external_signal_score,data_quality_score,created_at'
          ),
          civant.entities.prediction_drivers.filter(
            { tenant_id: activeTenantId, prediction_id: ids },
            '-contribution',
            2000,
            0,
            'prediction_id,driver_type,label,contribution,narrative,evidence_refs,created_at'
          )
        ]);

        setScorecardsByPrediction(toMap(scorecards || [], 'prediction_id'));
        setDriversByPrediction(toGroupedDrivers(drivers || []));
      } else {
        setScorecardsByPrediction(new Map());
        setDriversByPrediction(new Map());
      }

      const runs = await civant.entities.prediction_runs.filter(
        { tenant_id: activeTenantId },
        '-started_at',
        20,
        0,
        'run_id,run_type,status,started_at,finished_at,pairs_processed,error_message'
      );
      setLatestRuns(Array.isArray(runs) ? runs : []);
    } catch (error) {
      console.error('Failed to load forecast data:', error);
      setPredictions([]);
      setScorecardsByPrediction(new Map());
      setDriversByPrediction(new Map());
      setLatestRuns([]);
    } finally {
      setLoading(false);
    }
  };

  const stats = useMemo(() => {
    const high = predictions.filter((row) => {
      const confidence = Number(row?.confidence || 0);
      return confidence >= 60;
    }).length;

    const medium = predictions.filter((row) => {
      const confidence = Number(row?.confidence || 0);
      return confidence >= 40 && confidence < 60;
    }).length;

    return {
      total: predictions.length,
      high,
      medium
    };
  }, [predictions]);

  const latestRun = latestRuns[0] || null;

  return (
    <Page>
      <PageHeader>
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <PageTitle>Tender Panorama</PageTitle>
            <PageDescription>
              Read-only forecast outputs from the scheduled deterministic engine (no UI-triggered compute).
            </PageDescription>
          </div>

          <div className="flex items-center gap-3">
            <Select value={selectedCountry} onValueChange={setSelectedCountry}>
              <SelectTrigger className="w-[180px] bg-civant-navy/50 border-civant-border text-card-foreground">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {COUNTRY_OPTIONS.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      </PageHeader>

      <PageBody className="space-y-6">
        {latestRun ? (
          <Card className="bg-civant-navy/60 border-civant-border">
            <CardContent className="pt-5 flex flex-col gap-2 text-sm md:flex-row md:items-center md:justify-between">
              <div className="flex items-center gap-2">
                <Badge className={latestRun.status === 'success' ? 'bg-civant-teal/15 text-civant-teal border-civant-teal/35' : 'bg-rose-500/15 text-rose-300 border-rose-400/35'}>
                  {String(latestRun.status || 'unknown').toUpperCase()}
                </Badge>
                <span className="text-muted-foreground">{latestRun.run_type} run</span>
              </div>
              <div className="text-muted-foreground">
                Last run: {formatDate(latestRun.finished_at || latestRun.started_at)}
                {' · '}
                Pairs: {Number(latestRun.pairs_processed || 0)}
              </div>
            </CardContent>
          </Card>
        ) : null}

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card className="bg-civant-navy/60 border-civant-border">
            <CardContent className="pt-6 flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">High Confidence</p>
                <p className="text-3xl font-bold text-card-foreground">{stats.high}</p>
              </div>
              <Target className="w-8 h-8 text-civant-teal" />
            </CardContent>
          </Card>

          <Card className="bg-civant-navy/60 border-civant-border">
            <CardContent className="pt-6 flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Medium Confidence</p>
                <p className="text-3xl font-bold text-card-foreground">{stats.medium}</p>
              </div>
              <TrendingUp className="w-8 h-8 text-civant-teal" />
            </CardContent>
          </Card>

          <Card className="bg-civant-navy/60 border-civant-border">
            <CardContent className="pt-6 flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Total Forecasts</p>
                <p className="text-3xl font-bold text-card-foreground">{stats.total}</p>
              </div>
              <Building2 className="w-8 h-8 text-civant-teal" />
            </CardContent>
          </Card>
        </div>

        {loading ? (
          <Card className="bg-civant-navy/60 border-civant-border">
            <CardContent className="py-12 flex items-center justify-center gap-3 text-muted-foreground">
              <Loader2 className="w-5 h-5 animate-spin" />
              Loading forecast outputs...
            </CardContent>
          </Card>
        ) : null}

        {!loading && predictions.length === 0 ? (
          <Card className="bg-civant-navy/60 border-civant-border">
            <CardContent className="py-12 text-center text-muted-foreground">
              No forecast rows yet for this filter. Run the scheduled predictive jobs or execute a manual rollout.
            </CardContent>
          </Card>
        ) : null}

        {!loading && predictions.length > 0 ? (
          <div className="space-y-4">
            {predictions.map((prediction) => {
              const scorecard = scorecardsByPrediction.get(prediction.prediction_id);
              const drivers = (driversByPrediction.get(prediction.prediction_id) || []).slice(0, 3);

              return (
                <Card key={prediction.prediction_id} className="bg-civant-navy/65 border-civant-border">
                  <CardHeader className="pb-4">
                    <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                      <div>
                        <div className="flex flex-wrap items-center gap-2 mb-2">
                          <span className="text-xl">{COUNTRY_FLAGS[prediction.region] || '🌍'}</span>
                          <Badge className={getConfidenceBadgeClass(prediction.confidence_band)}>
                            {prediction.confidence_band}
                          </Badge>
                          <Badge variant="outline" className="border-civant-border text-muted-foreground">
                            {WINDOW_LABELS[prediction.next_window_label] || prediction.next_window_label}
                          </Badge>
                        </div>
                        <CardTitle className="text-card-foreground text-xl">
                          {prediction.buyer_display_name || prediction.buyer_entity_id}
                        </CardTitle>
                        <p className="text-sm text-muted-foreground mt-1">
                          {prediction.cpv_cluster_label || prediction.cpv_cluster_id} · {prediction.region}
                        </p>
                      </div>

                      <div className="flex items-center gap-2">
                        <Button variant="outline" className="border-civant-border" asChild>
                          <Link to={createPageUrl(`search?buyer=${encodeURIComponent(prediction.buyer_display_name || prediction.buyer_entity_id || '')}`)}>
                            View related tenders
                          </Link>
                        </Button>
                      </div>
                    </div>
                  </CardHeader>

                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                      <div className="rounded-lg border border-civant-border bg-civant-navy/60 p-3">
                        <p className="text-xs text-muted-foreground">Probability</p>
                        <p className="text-lg font-semibold text-card-foreground">{formatPct(prediction.probability)}</p>
                      </div>
                      <div className="rounded-lg border border-civant-border bg-civant-navy/60 p-3">
                        <p className="text-xs text-muted-foreground">Confidence</p>
                        <p className="text-lg font-semibold text-card-foreground">{Number(prediction.confidence || 0)}%</p>
                      </div>
                      <div className="rounded-lg border border-civant-border bg-civant-navy/60 p-3">
                        <p className="text-xs text-muted-foreground">Forecast Score</p>
                        <p className="text-lg font-semibold text-card-foreground">{Number(prediction.forecast_score || 0)}</p>
                      </div>
                      <div className="rounded-lg border border-civant-border bg-civant-navy/60 p-3">
                        <p className="text-xs text-muted-foreground">Expected Window</p>
                        <p className="text-sm font-semibold text-card-foreground">
                          {formatDate(prediction.expected_window_start)} - {formatDate(prediction.expected_window_end)}
                        </p>
                      </div>
                      <div className="rounded-lg border border-civant-border bg-civant-navy/60 p-3">
                        <p className="text-xs text-muted-foreground">Fallback Tier</p>
                        <p className="text-lg font-semibold text-card-foreground">{Number(prediction.fallback_tier || 0)}</p>
                      </div>
                    </div>

                    {scorecard ? (
                      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-2 text-xs">
                        <div className="rounded border border-civant-border p-2 text-muted-foreground">Cycle {scorecard.cycle_score}</div>
                        <div className="rounded border border-civant-border p-2 text-muted-foreground">Timing {scorecard.timing_score}</div>
                        <div className="rounded border border-civant-border p-2 text-muted-foreground">Behaviour {scorecard.behavioural_score}</div>
                        <div className="rounded border border-civant-border p-2 text-muted-foreground">Structural {scorecard.structural_score}</div>
                        <div className="rounded border border-civant-border p-2 text-muted-foreground">External {scorecard.external_signal_score}</div>
                        <div className="rounded border border-civant-border p-2 text-muted-foreground">Data quality {scorecard.data_quality_score}</div>
                      </div>
                    ) : null}

                    <div>
                      <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground mb-2">Top Drivers</p>
                      {drivers.length === 0 ? (
                        <p className="text-sm text-muted-foreground">No driver details yet.</p>
                      ) : (
                        <div className="space-y-2">
                          {drivers.map((driver, idx) => (
                            <div key={`${prediction.prediction_id}-${driver.driver_type}-${idx}`} className="rounded-lg border border-civant-border p-3">
                              <div className="flex items-center justify-between gap-2">
                                <p className="text-sm font-medium text-card-foreground">{driver.label}</p>
                                <Badge variant="outline" className="border-civant-border text-muted-foreground">
                                  {Number(driver.contribution || 0).toFixed(3)}
                                </Badge>
                              </div>
                              <p className="mt-1 text-sm text-muted-foreground">{driver.narrative}</p>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>

                    <div className="text-xs text-muted-foreground flex items-center gap-2">
                      <Calendar className="w-3.5 h-3.5" /> Last computed {formatDate(prediction.last_computed_at)}
                      <Clock className="w-3.5 h-3.5 ml-2" /> {Number(prediction.drivers_count || 0)} drivers · {Number(prediction.evidence_count || 0)} evidence refs
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        ) : null}
      </PageBody>
    </Page>
  );
}
