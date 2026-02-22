import React, { useEffect, useState } from 'react';
import { civant } from '@/api/civantClient';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Loader2, AlertCircle, RefreshCw, CheckCircle2, XCircle } from 'lucide-react';

function pretty(value) {
  return JSON.stringify(value, null, 2);
}

function unwrapResponse(response) {
  return response?.data ?? response ?? null;
}

export default function PipelineAdmin() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [runs, setRuns] = useState([]);
  const [queue, setQueue] = useState([]);
  const [predictions, setPredictions] = useState([]);
  const [lifecyclePredictions, setLifecyclePredictions] = useState([]);
  const [lifecycleQueue, setLifecycleQueue] = useState([]);
  const [lifecycleSummary, setLifecycleSummary] = useState({});
  const [duplicateSummary, setDuplicateSummary] = useState(null);
  const [selectedPrediction, setSelectedPrediction] = useState(null);
  const [busyQueueId, setBusyQueueId] = useState(null);
  const [busyLifecycleCandidateId, setBusyLifecycleCandidateId] = useState(null);

  const loadData = async () => {
    const response = unwrapResponse(await civant.functions.invoke('getPipelineAdmin', { action: 'overview' }));
    const data = response || {};
    setRuns(data.runs || []);
    setQueue(data.queue || []);
    setPredictions(data.predictions || []);
    setLifecyclePredictions(data.lifecycle_predictions || []);
    setLifecycleQueue(data.lifecycle_queue || []);
    setLifecycleSummary(data.lifecycle_summary || {});
    setDuplicateSummary(data.duplicateSummary || null);
  };

  useEffect(() => {
    const init = async () => {
      try {
        const me = unwrapResponse(await civant.auth.me());
        setUser(me);
        if (me?.role === 'admin') {
          await loadData();
        }
      } catch (error) {
        console.error(error);
      } finally {
        setLoading(false);
      }
    };
    init();
  }, []);

  const handleReview = async (item, decision) => {
    setBusyQueueId(item.id);
    try {
      await civant.functions.invoke('getPipelineAdmin', {
        action: 'review_decision',
        queue_id: item.id,
        decision
      });
      await loadData();
    } catch (error) {
      console.error('Review failed:', error);
    } finally {
      setBusyQueueId(null);
    }
  };

  const openPrediction = async (predictionId) => {
    try {
      const response = await civant.functions.invoke('getPipelineAdmin', {
        action: 'prediction_detail',
        prediction_id: predictionId
      });
      const payload = unwrapResponse(response);
      setSelectedPrediction(payload?.prediction || null);
    } catch (error) {
      console.error('Failed to load prediction detail:', error);
    }
  };

  const handleLifecycleReview = async (candidateId, decision) => {
    setBusyLifecycleCandidateId(candidateId);
    try {
      await civant.functions.invoke('getPipelineAdmin', {
        action: 'lifecycle_review_decision',
        candidate_id: candidateId,
        decision
      });
      await loadData();
    } catch (error) {
      console.error('Lifecycle review failed:', error);
    } finally {
      setBusyLifecycleCandidateId(null);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-civant-teal" />
      </div>
    );
  }

  if (user?.role !== 'admin') {
    return (
      <div className="text-center py-12">
        <AlertCircle className="h-12 w-12 mx-auto text-red-400 mb-4" />
        <h2 className="text-xl font-semibold text-slate-100">Access Denied</h2>
        <p className="text-slate-400 mt-2">This page is only accessible to administrators.</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">Pipeline Admin</h1>
          <p className="text-slate-400 mt-1">Ingestion runs, review queue, and prediction evidence</p>
        </div>
        <Button variant="outline" onClick={loadData}>
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-6 gap-4">
        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-base">Runs</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-bold">{runs.length}</p></CardContent>
        </Card>
        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-base">Review Queue</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-bold">{queue.length}</p></CardContent>
        </Card>
        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-base">Predictions</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-bold">{predictions.length}</p></CardContent>
        </Card>
        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-base">Lifecycle Rows</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-bold">{lifecyclePredictions.length}</p></CardContent>
        </Card>
        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-base">Lifecycle Queue</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-bold">{lifecycleQueue.length}</p></CardContent>
        </Card>
        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
          <CardHeader className="pb-2"><CardTitle className="text-base">Duplicate Rows</CardTitle></CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{duplicateSummary?.total_duplicates || 0}</p>
            <p className="text-xs text-slate-400 mt-1">In-file + DB duplicate detections</p>
          </CardContent>
        </Card>
      </div>

      {duplicateSummary && (
        <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
          <CardHeader><CardTitle>Duplicate Stats</CardTitle></CardHeader>
          <CardContent className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
            <div className="rounded-lg border border-civant-border bg-civant-navy/65 p-3">
              <p className="text-slate-400">Deduped in file</p>
              <p className="text-lg font-semibold">{duplicateSummary.deduped_in_file || 0}</p>
            </div>
            <div className="rounded-lg border border-civant-border bg-civant-navy/65 p-3">
              <p className="text-slate-400">Recovered with inferred IDs</p>
              <p className="text-lg font-semibold">{duplicateSummary.inferred_id_rows || 0}</p>
            </div>
            <div className="rounded-lg border border-civant-border bg-civant-navy/65 p-3">
              <p className="text-slate-400">Raw rows logged</p>
              <p className="text-lg font-semibold">{duplicateSummary.raw_documents_logged || 0}</p>
            </div>
          </CardContent>
        </Card>
      )}

      <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
        <CardHeader><CardTitle>Prediction Lifecycle Statuses</CardTitle></CardHeader>
        <CardContent className="grid grid-cols-1 md:grid-cols-4 gap-3">
          {Object.keys(lifecycleSummary).length === 0 && (
            <p className="text-slate-400 text-sm col-span-full">No lifecycle rows yet.</p>
          )}
          {Object.entries(lifecycleSummary).map(([status, count]) => (
            <div key={status} className="rounded-lg border border-civant-border bg-civant-navy/65 p-3">
              <p className="text-slate-400 text-xs uppercase tracking-wide">{status}</p>
              <p className="text-lg font-semibold">{count}</p>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
        <CardHeader><CardTitle>Ingestion Runs + Errors</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          {runs.length === 0 && <p className="text-slate-400">No runs yet.</p>}
          {runs.map((run) => (
            <div key={run.id || run.run_id} className="rounded-lg border border-civant-border bg-civant-navy/65 p-3">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">{run.status}</Badge>
                <span className="text-sm font-medium">{run.run_id}</span>
                <span className="text-xs text-slate-400">{run.source}</span>
                <Badge variant="outline">
                  duplicates: {run.duplicate_stats?.total_duplicates || 0}
                </Badge>
                <Badge variant="outline">
                  raw rows: {run.duplicate_stats?.raw_rows || 0}
                </Badge>
              </div>
              <div className="mt-2 text-xs text-slate-300">
                processed {run.duplicate_stats?.processed_rows || 0} rows, inferred IDs {run.duplicate_stats?.inferred_id_rows || 0}
              </div>
              {!!(run.errors && run.errors.length) && (
                <pre className="mt-2 text-xs bg-civant-navy/80 border border-civant-border rounded p-2 overflow-x-auto text-slate-200">{pretty(run.errors)}</pre>
              )}
            </div>
          ))}
        </CardContent>
      </Card>

      <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
        <CardHeader><CardTitle>Reconciliation Review Queue</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          {queue.length === 0 && <p className="text-slate-400">Queue is empty.</p>}
          {queue.map((item) => (
            <div key={item.id} className="rounded-lg border border-civant-border bg-civant-navy/65 p-3">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-sm font-medium">{item.id}</p>
                  <p className="text-xs text-slate-400">Status: {item.status}</p>
                </div>
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90"
                    disabled={busyQueueId === item.id}
                    onClick={() => handleReview(item, 'approve')}
                  >
                    <CheckCircle2 className="h-4 w-4 mr-1" />
                    Approve
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={busyQueueId === item.id}
                    onClick={() => handleReview(item, 'reject')}
                  >
                    <XCircle className="h-4 w-4 mr-1" />
                    Reject
                  </Button>
                </div>
              </div>
              <pre className="mt-2 text-xs bg-civant-navy/80 border border-civant-border rounded p-2 overflow-x-auto text-slate-200">{pretty(item.candidate_json)}</pre>
              <pre className="mt-2 text-xs bg-civant-navy/80 border border-civant-border rounded p-2 overflow-x-auto text-slate-200">{pretty(item.agent_output)}</pre>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
        <CardHeader><CardTitle>Prediction Reconciliation Queue</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          {lifecycleQueue.length === 0 && <p className="text-slate-400">No lifecycle candidates pending review.</p>}
          {lifecycleQueue.map((item) => (
            <div key={item.candidate_id || item.id} className="rounded-lg border border-civant-border bg-civant-navy/65 p-3">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-sm font-medium">Candidate {item.candidate_id || item.id}</p>
                  <p className="text-xs text-slate-400">
                    Lifecycle {item.lifecycle_id} | score {Number(item.match_score || 0).toFixed(3)}
                  </p>
                  <p className="text-xs text-slate-400">Status: {item.status}</p>
                </div>
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90"
                    disabled={busyLifecycleCandidateId === (item.candidate_id || item.id)}
                    onClick={() => handleLifecycleReview(item.candidate_id || item.id, 'approve')}
                  >
                    <CheckCircle2 className="h-4 w-4 mr-1" />
                    Approve
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={busyLifecycleCandidateId === (item.candidate_id || item.id)}
                    onClick={() => handleLifecycleReview(item.candidate_id || item.id, 'reject')}
                  >
                    <XCircle className="h-4 w-4 mr-1" />
                    Reject
                  </Button>
                </div>
              </div>
              <pre className="mt-2 text-xs bg-civant-navy/80 border border-civant-border rounded p-2 overflow-x-auto text-slate-200">{pretty(item.reasons)}</pre>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
        <CardHeader><CardTitle>Predictions</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          {predictions.length === 0 && <p className="text-slate-400">No predictions yet.</p>}
          {predictions.map((prediction) => (
            <div key={prediction.id} className="rounded-lg border border-civant-border bg-civant-navy/65 p-3 flex items-center justify-between">
              <div>
                <p className="text-sm font-medium">{prediction.category} / {prediction.cpv_family}</p>
                <p className="text-xs text-slate-400">Probability {Math.round((prediction.probability || 0) * 100)}%</p>
              </div>
              <Button size="sm" variant="outline" onClick={() => openPrediction(prediction.id)}>View breakdown</Button>
            </div>
          ))}

          {selectedPrediction && (
            <div className="rounded-lg border border-civant-teal/40 bg-civant-teal/10 p-3">
              <p className="font-medium text-civant-teal">Selected prediction: {selectedPrediction.id}</p>
              <pre className="mt-2 text-xs bg-civant-navy/80 border border-civant-border rounded p-2 overflow-x-auto text-slate-200">{pretty(selectedPrediction.confidence_breakdown)}</pre>
              <pre className="mt-2 text-xs bg-civant-navy/80 border border-civant-border rounded p-2 overflow-x-auto text-slate-200">{pretty(selectedPrediction.evidence)}</pre>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
