import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant } from './requireAdmin.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';

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

async function invokeFunctionWithTenant(
    req: Request,
    tenantId: string,
    functionName: string,
    payload: Record<string, unknown>
) {
    const url = new URL(req.url);
    const appId = req.headers.get('X-App-Id')
        || url.searchParams.get('app_id')
        || Deno.env.get('CIVANT_APP_ID')
        || '';
    if (!appId) {
        throw new Error('Civant app id is required. Provide X-App-Id header or CIVANT_APP_ID env var.');
    }

    const baseUrl = Deno.env.get('CIVANT_API_BASE_URL')
        || Deno.env.get('CIVANT_APP_BASE_URL')
        || `${url.protocol}//${url.host}`;

    const authHeader = String(req.headers.get('Authorization') || '');
    const response = await fetch(`${baseUrl}/apps/${appId}/functions/${functionName}`, {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
            'X-App-Id': appId,
            'x-tenant-id': tenantId,
            ...(authHeader ? { Authorization: authHeader } : {})
        },
        body: JSON.stringify(payload || {})
    });

    const raw = await response.text();
    const data = raw ? (() => {
        try {
            return JSON.parse(raw);
        } catch {
            return raw;
        }
    })() : null;

    if (!response.ok) {
        throw new Error(
            typeof data === 'object' && data && 'error' in data
                ? String((data as Record<string, unknown>).error)
                : `Function ${functionName} failed with status ${response.status}`
        );
    }

    return data;
}

// Orchestrator function to run all connectors
Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const tenantId = getTenantFromHeader(req);
        await requireAdminForTenant({ civant, req, tenantId });
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
            const boampResult = await invokeFunctionWithTenant(req, tenantId, 'fetchBoampFr', connectorParams);
            results.connectors.BOAMP_FR = boampResult.data || boampResult;
        } catch (e: unknown) {
            results.connectors.BOAMP_FR = { error: getErrorMessage(e) };
        }
        
        // Run TED IE
        try {
            const tedIeResult = await invokeFunctionWithTenant(req, tenantId, 'fetchTed', {
                ...connectorParams,
                country: 'IE'
            });
            results.connectors.TED_IE = tedIeResult.data || tedIeResult;
        } catch (e: unknown) {
            results.connectors.TED_IE = { error: getErrorMessage(e) };
        }
        
        // Run TED FR
        try {
            const tedFrResult = await invokeFunctionWithTenant(req, tenantId, 'fetchTed', {
                ...connectorParams,
                country: 'FR'
            });
            results.connectors.TED_FR = tedFrResult.data || tedFrResult;
        } catch (e: unknown) {
            results.connectors.TED_FR = { error: getErrorMessage(e) };
        }
        
        // Run Ireland source
        try {
            const ieResult = await invokeFunctionWithTenant(req, tenantId, 'fetchIreland', connectorParams);
            results.connectors.ETENDERS_IE = ieResult.data || ieResult;
        } catch (e: unknown) {
            results.connectors.ETENDERS_IE = { error: getErrorMessage(e) };
        }
        
        
        // Run Ireland incremental eTenders
        try {
            const ieInc = await invokeFunctionWithTenant(req, tenantId, 'fetchEtendersIeIncremental', connectorParams);
            results.connectors.ETENDERS_IE_INCREMENTAL = ieInc.data || ieInc;
        } catch (e: unknown) {
            results.connectors.ETENDERS_IE_INCREMENTAL = { error: getErrorMessage(e) };
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
