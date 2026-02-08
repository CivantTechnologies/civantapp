import React, { useEffect, useMemo, useState } from 'react';
import { civant } from '@/api/civantClient';
import { AlertCircle, Copy, Loader2, Network, ShieldCheck } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';

const MERMAID_SCRIPT_ID = 'mermaid-cdn-script';
const MERMAID_SCRIPT_SRC = 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js';

const ARCHITECTURE_DIAGRAM = `flowchart LR
  subgraph S[Data Sources]
    S1[Tender portals\\n(TED, eTenders, EU-Supply, national portals)]
    S2[Datadumps / CSV exports\\n(open data, buyer exports)]
    S3[Buyer websites\\n(procurement plans, IT pages)]
    S4[Vendor signals\\n(case studies, product pages)]
    S5[Social & news signals\\n(press releases, policy, funding)]
  end

  subgraph I[Ingestion & Scraping Layer]
    I1[Connectors\\nAPI / RSS / HTML scraping]
    I2[Dump Loader\\nfile upload / scheduled pull]
    I3[Scheduler + Rate limiter\\nretries, backoff, quotas]
    I4[Run Orchestrator\\nrun_id, cursor, checkpoints]
  end

  S1 --> I1
  S2 --> I2
  S3 --> I1
  S4 --> I1
  S5 --> I1
  I1 --> I4
  I2 --> I4
  I3 --> I4

  subgraph R[Raw + Staging Storage]
    R1[(Raw Store\\nblob: JSON/HTML/PDF/CSV)]
    R2[(Staging Records\\nparsed JSON + validation)]
    R3[(Ingestion Runs\\nstatus, cursor, metrics, errors)]
  end

  I4 --> R1
  I4 --> R3
  R1 --> P1

  subgraph P[Processing & Normalisation]
    P1[Parsing\\nextract fields, tables, entities]
    P2[Normalisation\\nCPV cleaning, currency, dates]
    P3[Entity Resolution\\nbuyers, suppliers, frameworks]
    P4[Deduplication + Upsert\\nsource_id -> canonical_id]
    P5[Quality Scoring\\ncompleteness, consistency]
  end

  P1 --> R2
  R2 --> P2
  P2 --> P3
  P3 --> P4
  P4 --> P5

  subgraph C[Canonical Data Layer]
    C1[(Canonical Tenders\\ncontract notices + awards)]
    C2[(Buyer Graph\\norg profiles + relationships)]
    C3[(Supplier Graph\\nwinners, incumbents)]
    C4[(Frameworks/DPS\\nparent agreements + call-offs)]
    C5[(Change Log\\nhistory of updates per record)]
  end

  P4 --> C1
  P3 --> C2
  P3 --> C3
  P3 --> C4
  P4 --> C5
  P5 --> C1

  subgraph F[Feature & Signal Engine]
    F1[Time-series Aggregations\\nweekly/monthly counts by CPV/buyer]
    F2[Text Embeddings\\nsimilarity for titles/specs]
    F3[Buyer Behaviour Features\\ncadence, seasonality, lead times]
    F4[Competition Features\\nbids, SMEs, value bands]
    F5[Market Signal Features\\nweb/news/vendor/buyer-plan signals]
  end

  C1 --> F1
  C1 --> F2
  C2 --> F3
  C1 --> F4
  R1 --> F5
  C2 --> F5

  subgraph FS[Feature Store]
    FS1[(Offline Store\\ntraining snapshots)]
    FS2[(Online Store\\nlow-latency serving)]
    FS3[Feature Registry\\nschemas, ownership, freshness SLA]
  end

  F1 --> FS1
  F2 --> FS1
  F3 --> FS1
  F4 --> FS1
  F5 --> FS1
  FS1 --> FS2
  FS3 --> FS1
  FS3 --> FS2

  subgraph M[AI + Prediction Layer]
    M1[Reconciliation Model\\nmatch/merge records across sources]
    M2[Classification Model\\ncategory: video platform, LMS, AV, etc.]
    M3[Forecast Model\\nlikelihood of tender in T+30/60/90/180 days]
    M4[Value & Timing Estimators\\nexpected € band, likely publish quarter]
    M5[Explainability Engine\\ntop drivers + evidence links]
  end

  FS2 --> M1
  FS2 --> M2
  FS2 --> M3
  FS2 --> M4
  F5 --> M5

  subgraph MO[Model Ops]
    MO1[(Model Registry\\nversions, metadata, lineage)]
    MO2[Deployment Manager\\nbatch + real-time endpoints]
    MO3[Evaluation Harness\\nbacktests, shadow, A/B]
    MO4[Calibration & Guardrails\\nthresholds, fallback rules]
  end

  M2 --> MO3
  M3 --> MO3
  M4 --> MO3
  MO3 --> MO1
  MO1 --> MO2
  MO4 --> MO2

  subgraph K[Confidence & Risk Scoring]
    K1[Data Confidence\\ncoverage, recency, missing fields]
    K2[Signal Confidence\\nstrength, agreement, source quality]
    K3[Model Confidence\\ncalibration, uncertainty, stability]
    K4[Overall Confidence Score\\n0-100 + banding]
  end

  M3 --> K3
  M4 --> K3
  P5 --> K1
  F5 --> K2
  K1 --> K4
  K2 --> K4
  K3 --> K4

  subgraph H[Human Review & Overrides]
    H1[Analyst Queue\\nhigh-value / low-confidence cases]
    H2[Decision Actions\\napprove, suppress, edit rationale]
    H3[(Override Log\\nauditable changes + reasons)]
  end

  K4 --> H1
  M5 --> H1
  H1 --> H2
  H2 --> H3

  subgraph O[Reports & Product Outputs]
    O1[(Predictions Store\\nper buyer/CPV/category/time window)]
    O2[Opportunity Radar\\nranked list + rationale]
    O3[Institution Reports\\nCivant-style: who/why/when/how sure]
    O4[Alerts\\nemail/Slack/CRM triggers]
    O5[API + Dashboard\\nfilters, drill-down, evidence view]
  end

  MO2 --> O1
  K4 --> O1
  M5 --> O2
  M5 --> O3
  O1 --> O2
  O1 --> O4
  O1 --> O5
  C1 --> O5
  C2 --> O5
  H2 --> O1

  subgraph L[Learning Loop]
    L1[User Feedback\\nrelevance, won/lost, false positives]
    L2[Ground Truth Updates\\nawards, cancellations, outcomes]
    L3[Model Retraining\\nschedule + evaluation]
  end

  O5 --> L1
  C1 --> L2
  L1 --> L3
  L2 --> L3
  L3 --> M2
  L3 --> M3
  L3 --> M4
  L3 --> FS1
  L3 --> MO3

  subgraph V[Observability & Monitoring]
    V1[Pipeline Monitoring\\nlatency, failures, retries]
    V2[Data Drift Monitoring\\nschema + distribution drift]
    V3[Model Monitoring\\nperformance, calibration, decay]
    V4[Alerting + Incident Mgmt\\nSLOs, paging, runbooks]
  end

  I4 --> V1
  P5 --> V2
  M3 --> V3
  M4 --> V3
  O4 --> V4
  V1 --> V4
  V2 --> V4
  V3 --> V4

  subgraph G[Governance, Security & Audit]
    G1[Data Lineage\\nsource-to-report traceability]
    G2[Access Control\\nRBAC, tenant isolation]
    G3[PII/Retention Policies\\nmasking, TTL, legal hold]
    G4[(Audit Trail\\nwho changed what and why)]
  end

  R1 --> G1
  C5 --> G1
  O3 --> G1
  O5 --> G2
  R1 --> G3
  H3 --> G4
  MO1 --> G4`;

function MermaidDiagram({ chart }) {
    const [svg, setSvg] = useState('');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const renderId = useMemo(() => `mermaid-${Math.random().toString(36).slice(2, 10)}`, []);

    useEffect(() => {
        let cancelled = false;

        const render = async () => {
            setLoading(true);
            setError('');

            try {
                const win = /** @type {any} */ (window);

                if (!win.mermaid) {
                    await new Promise((resolve, reject) => {
                        let script = /** @type {HTMLScriptElement | null} */ (document.getElementById(MERMAID_SCRIPT_ID));
                        if (script) {
                            if (win.mermaid) {
                                resolve();
                                return;
                            }
                            script.addEventListener('load', resolve, { once: true });
                            script.addEventListener('error', () => reject(new Error('Could not load Mermaid.')), { once: true });
                            return;
                        }

                        script = document.createElement('script');
                        script.id = MERMAID_SCRIPT_ID;
                        script.src = MERMAID_SCRIPT_SRC;
                        script.async = true;
                        script.onload = resolve;
                        script.onerror = () => reject(new Error('Could not load Mermaid.'));
                        document.head.appendChild(script);
                    });
                }

                win.mermaid.initialize({
                    startOnLoad: false,
                    securityLevel: 'loose',
                    theme: 'dark',
                    flowchart: { useMaxWidth: false, htmlLabels: true }
                });

                const { svg: nextSvg } = await win.mermaid.render(renderId, chart);
                if (!cancelled) {
                    setSvg(nextSvg);
                }
            } catch (e) {
                if (!cancelled) {
                    setError(e?.message || 'Failed to render Mermaid diagram.');
                }
            } finally {
                if (!cancelled) {
                    setLoading(false);
                }
            }
        };

        render();

        return () => {
            cancelled = true;
        };
    }, [chart, renderId]);

    if (loading) {
        return (
            <div className="flex items-center justify-center min-h-[320px]">
                <Loader2 className="h-6 w-6 animate-spin text-civant-teal" />
            </div>
        );
    }

    if (error) {
        return (
            <div className="space-y-3">
                <div className="rounded-xl border border-amber-400/40 bg-amber-500/15 p-4 text-amber-100 text-sm">
                    <div className="flex items-center gap-2 font-medium">
                        <AlertCircle className="h-4 w-4" />
                        Could not render Mermaid
                    </div>
                    <p className="mt-2">{error}</p>
                </div>
                <pre className="rounded-xl bg-slate-900 text-slate-100 p-4 overflow-x-auto text-xs leading-relaxed">
                    {chart}
                </pre>
            </div>
        );
    }

    return (
        <div className="overflow-auto rounded-xl border border-civant-border bg-civant-navy/75 p-4">
            <div className="min-w-[1280px]" dangerouslySetInnerHTML={{ __html: svg }} />
        </div>
    );
}

export default function Architecture() {
    const [user, setUser] = useState(null);
    const [loadingUser, setLoadingUser] = useState(true);

    useEffect(() => {
        const loadUser = async () => {
            try {
                const currentUser = await civant.auth.me();
                setUser(currentUser);
            } catch {
                setUser(null);
            } finally {
                setLoadingUser(false);
            }
        };

        loadUser();
    }, []);

    const copyDiagram = async () => {
        try {
            await navigator.clipboard.writeText(ARCHITECTURE_DIAGRAM);
        } catch {
            // No-op if clipboard is unavailable.
        }
    };

    if (loadingUser) {
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
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
                <div>
                    <h1 className="text-2xl font-bold text-slate-100">System Architecture</h1>
                    <p className="text-slate-400 mt-1">End-to-end Civant data and AI pipeline</p>
                </div>
                <div className="flex items-center gap-2">
                    <Badge className="bg-civant-teal/20 text-civant-teal border border-civant-teal/40">
                        <ShieldCheck className="h-3.5 w-3.5 mr-1" />
                        Admin
                    </Badge>
                    <Button variant="outline" onClick={copyDiagram}>
                        <Copy className="h-4 w-4 mr-2" />
                        Copy Mermaid
                    </Button>
                </div>
            </div>

            <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-lg">
                        <Network className="h-5 w-5 text-civant-teal" />
                        Architecture Diagram
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    <MermaidDiagram chart={ARCHITECTURE_DIAGRAM} />
                </CardContent>
            </Card>
        </div>
    );
}
