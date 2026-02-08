import { createClientFromRequest } from './civantSdk.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

type ConnectorResult = {
    error?: string;
    fetched_count?: number;
    inserted_count?: number;
    updated_count?: number;
    versioned_count?: number;
    success?: boolean;
    [key: string]: unknown;
};

type RunAllResults = {
    started_at: string;
    finished_at?: string;
    mode?: string;
    connectors: Record<string, ConnectorResult>;
    alerts?: Record<string, unknown>;
    totals?: {
        fetched: number;
        inserted: number;
        updated: number;
        versioned: number;
    };
};

// Orchestrator function to run all connectors
Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 403 });
        }
        
        const body = await req.json().catch(() => ({})) as { mode?: string; days_since?: number; years_back?: number };
        const mode = body.mode || 'incremental'; // 'incremental' or 'backfill'
        const daysSince = Number(body.days_since || 1825); // 5 years default
        const yearsBack = Number(body.years_back || 10);
        
        const results: RunAllResults = {
            started_at: new Date().toISOString(),
            connectors: {}
        };
        
        // Prepare connector parameters based on mode
        const connectorParams = mode === 'backfill' 
            ? { mode: 'backfill', years_back: yearsBack, limit: 500 }
            : { mode: 'incremental', days_since: daysSince, limit: 100 };
        
        // Run BOAMP FR
        try {
            const boampResult = await civant.functions.invoke('fetchBoampFr', connectorParams);
            results.connectors.BOAMP_FR = boampResult.data || boampResult;
        } catch (e: unknown) {
            results.connectors.BOAMP_FR = { error: getErrorMessage(e) };
        }
        
        // Run TED IE
        try {
            const tedIeResult = await civant.functions.invoke('fetchTed', {
                ...connectorParams,
                country: 'IE'
            });
            results.connectors.TED_IE = tedIeResult.data || tedIeResult;
        } catch (e: unknown) {
            results.connectors.TED_IE = { error: getErrorMessage(e) };
        }
        
        // Run TED FR
        try {
            const tedFrResult = await civant.functions.invoke('fetchTed', {
                ...connectorParams,
                country: 'FR'
            });
            results.connectors.TED_FR = tedFrResult.data || tedFrResult;
        } catch (e: unknown) {
            results.connectors.TED_FR = { error: getErrorMessage(e) };
        }
        
        // Run Ireland source
        try {
            const ieResult = await civant.functions.invoke('fetchIreland', connectorParams);
            results.connectors.ETENDERS_IE = ieResult.data || ieResult;
        } catch (e: unknown) {
            results.connectors.ETENDERS_IE = { error: getErrorMessage(e) };
        }
        
        // Process alerts after ingestion
        try {
            const alertsResult = await civant.functions.invoke('processAlerts', {});
            results.alerts = alertsResult.data || alertsResult;
        } catch (e: unknown) {
            results.alerts = { error: getErrorMessage(e) };
        }
        
        results.finished_at = new Date().toISOString();
        
        // Calculate totals
        let totalFetched = 0;
        let totalInserted = 0;
        let totalUpdated = 0;
        let totalVersioned = 0;
        
        Object.values(results.connectors).forEach((c: ConnectorResult) => {
            if (c && !c.error) {
                totalFetched += Number(c.fetched_count || 0);
                totalInserted += Number(c.inserted_count || 0);
                totalUpdated += Number(c.updated_count || 0);
                totalVersioned += Number(c.versioned_count || 0);
            }
        });
        
        results.totals = {
            fetched: totalFetched,
            inserted: totalInserted,
            updated: totalUpdated,
            versioned: totalVersioned
        };
        
        results.mode = mode;
        
        return Response.json(results);
        
    } catch (error: unknown) {
        return Response.json({ error: getErrorMessage(error) }, { status: 500 });
    }
});
