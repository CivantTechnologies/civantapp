import React, { useEffect, useState } from 'react';
import { civant } from '@/api/civantClient';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Loader2, AlertCircle, RefreshCw, CheckCircle2, XCircle } from 'lucide-react';

function pretty(value) {
  return JSON.stringify(value, null, 2);
}

export default function PipelineAdmin() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [runs, setRuns] = useState([]);
  const [queue, setQueue] = useState([]);
  const [predictions, setPredictions] = useState([]);
  const [selectedPrediction, setSelectedPrediction] = useState(null);
  const [busyQueueId, setBusyQueueId] = useState(null);

  const loadData = async () => {
    const response = await civant.functions.invoke('getPipelineAdmin', { action: 'overview' });
    const data = response?.data || {};
    setRuns(data.runs || []);
    setQueue(data.queue || []);
    setPredictions(data.predictions || []);
  };

  useEffect(() => {
    const init = async () => {
      try {
        const me = await civant.auth.me();
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
      setSelectedPrediction(response?.data?.prediction || null);
    } catch (error) {
      console.error('Failed to load prediction detail:', error);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-indigo-600" />
      </div>
    );
  }

  if (user?.role !== 'admin') {
    return (
      <div className="text-center py-12">
        <AlertCircle className="h-12 w-12 mx-auto text-red-400 mb-4" />
        <h2 className="text-xl font-semibold text-slate-900">Access Denied</h2>
        <p className="text-slate-500 mt-2">This page is only accessible to administrators.</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Pipeline Admin</h1>
          <p className="text-slate-500 mt-1">Ingestion runs, review queue, and prediction evidence</p>
        </div>
        <Button variant="outline" onClick={loadData}>
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="border-0 shadow-sm">
          <CardHeader className="pb-2"><CardTitle className="text-base">Runs</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-bold">{runs.length}</p></CardContent>
        </Card>
        <Card className="border-0 shadow-sm">
          <CardHeader className="pb-2"><CardTitle className="text-base">Review Queue</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-bold">{queue.length}</p></CardContent>
        </Card>
        <Card className="border-0 shadow-sm">
          <CardHeader className="pb-2"><CardTitle className="text-base">Predictions</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-bold">{predictions.length}</p></CardContent>
        </Card>
      </div>

      <Card className="border-0 shadow-sm">
        <CardHeader><CardTitle>Ingestion Runs + Errors</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          {runs.length === 0 && <p className="text-slate-500">No runs yet.</p>}
          {runs.map((run) => (
            <div key={run.id} className="rounded-lg border border-slate-200 p-3">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">{run.status}</Badge>
                <span className="text-sm font-medium">{run.run_id}</span>
                <span className="text-xs text-slate-500">{run.source}</span>
              </div>
              {!!(run.errors && run.errors.length) && (
                <pre className="mt-2 text-xs bg-slate-50 rounded p-2 overflow-x-auto">{pretty(run.errors)}</pre>
              )}
            </div>
          ))}
        </CardContent>
      </Card>

      <Card className="border-0 shadow-sm">
        <CardHeader><CardTitle>Reconciliation Review Queue</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          {queue.length === 0 && <p className="text-slate-500">Queue is empty.</p>}
          {queue.map((item) => (
            <div key={item.id} className="rounded-lg border border-slate-200 p-3">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-sm font-medium">{item.id}</p>
                  <p className="text-xs text-slate-500">Status: {item.status}</p>
                </div>
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    className="bg-emerald-600 hover:bg-emerald-700"
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
              <pre className="mt-2 text-xs bg-slate-50 rounded p-2 overflow-x-auto">{pretty(item.candidate_json)}</pre>
              <pre className="mt-2 text-xs bg-slate-50 rounded p-2 overflow-x-auto">{pretty(item.agent_output)}</pre>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card className="border-0 shadow-sm">
        <CardHeader><CardTitle>Predictions</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          {predictions.length === 0 && <p className="text-slate-500">No predictions yet.</p>}
          {predictions.map((prediction) => (
            <div key={prediction.id} className="rounded-lg border border-slate-200 p-3 flex items-center justify-between">
              <div>
                <p className="text-sm font-medium">{prediction.category} / {prediction.cpv_family}</p>
                <p className="text-xs text-slate-500">Probability {Math.round((prediction.probability || 0) * 100)}%</p>
              </div>
              <Button size="sm" variant="outline" onClick={() => openPrediction(prediction.id)}>View breakdown</Button>
            </div>
          ))}

          {selectedPrediction && (
            <div className="rounded-lg border border-indigo-200 bg-indigo-50 p-3">
              <p className="font-medium text-indigo-900">Selected prediction: {selectedPrediction.id}</p>
              <pre className="mt-2 text-xs bg-white rounded p-2 overflow-x-auto">{pretty(selectedPrediction.confidence_breakdown)}</pre>
              <pre className="mt-2 text-xs bg-white rounded p-2 overflow-x-auto">{pretty(selectedPrediction.evidence)}</pre>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
