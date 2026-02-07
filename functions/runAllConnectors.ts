import { createClientFromRequest } from './civantSdk.ts';

// Orchestrator function to run all connectors
Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 403 });
        }
        
        const body = await req.json().catch(() => ({}));
        const mode = body.mode || 'incremental'; // 'incremental' or 'backfill'
        const daysSince = body.days_since || 1825; // 5 years default
        const yearsBack = body.years_back || 10;
        
        const results = {
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
        } catch (e) {
            results.connectors.BOAMP_FR = { error: e.message };
        }
        
        // Run TED IE
        try {
            const tedIeResult = await civant.functions.invoke('fetchTed', {
                ...connectorParams,
                country: 'IE'
            });
            results.connectors.TED_IE = tedIeResult.data || tedIeResult;
        } catch (e) {
            results.connectors.TED_IE = { error: e.message };
        }
        
        // Run TED FR
        try {
            const tedFrResult = await civant.functions.invoke('fetchTed', {
                ...connectorParams,
                country: 'FR'
            });
            results.connectors.TED_FR = tedFrResult.data || tedFrResult;
        } catch (e) {
            results.connectors.TED_FR = { error: e.message };
        }
        
        // Run Ireland source
        try {
            const ieResult = await civant.functions.invoke('fetchIreland', connectorParams);
            results.connectors.ETENDERS_IE = ieResult.data || ieResult;
        } catch (e) {
            results.connectors.ETENDERS_IE = { error: e.message };
        }
        
        // Process alerts after ingestion
        try {
            const alertsResult = await civant.functions.invoke('processAlerts', {});
            results.alerts = alertsResult.data || alertsResult;
        } catch (e) {
            results.alerts = { error: e.message };
        }
        
        results.finished_at = new Date().toISOString();
        
        // Calculate totals
        let totalFetched = 0;
        let totalInserted = 0;
        let totalUpdated = 0;
        let totalVersioned = 0;
        
        Object.values(results.connectors).forEach(c => {
            if (c && !c.error) {
                totalFetched += c.fetched_count || 0;
                totalInserted += c.inserted_count || 0;
                totalUpdated += c.updated_count || 0;
                totalVersioned += c.versioned_count || 0;
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
        
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});