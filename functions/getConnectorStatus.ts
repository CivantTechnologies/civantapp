import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant, resolveTenantId } from './requireAdmin.ts';

const CONNECTOR_MAP: Record<string, { key: string; displayName: string }> = {
  BOAMP_FR: { key: 'BOAMP_FR', displayName: 'BOAMP France' },
  TED: { key: 'TED', displayName: 'TED (EU)' },
  TED_IE: { key: 'TED_IE', displayName: 'TED Ireland' },
  TED_FR: { key: 'TED_FR', displayName: 'TED France' },
  ETENDERS_IE: { key: 'ETENDERS_IE', displayName: 'eTenders Ireland' }
};

function toDisplay(connectorKey: string) {
  if (CONNECTOR_MAP[connectorKey]) return CONNECTOR_MAP[connectorKey];
  return { key: connectorKey, displayName: connectorKey };
}

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);

    const body = await req.json().catch(() => ({}));
    const tenantId = resolveTenantId(body.tenantId || body.tenant_id || req.headers.get('X-Tenant-Id'));

    await requireAdminForTenant({ civant, req, tenantId });

    const [configs, runs] = await Promise.all([
      civant.asServiceRole.entities.ConnectorConfig.filter({ tenant_id: tenantId }, '-updated_at', 200)
        .catch(() => civant.asServiceRole.entities.ConnectorConfig.list('-updated_at', 200)),
      civant.asServiceRole.entities.ConnectorRuns.filter({ tenant_id: tenantId }, '-started_at', 300)
        .catch(() => civant.asServiceRole.entities.ConnectorRuns.list('-started_at', 300))
    ]);

    const configList = Array.isArray(configs) ? configs : [];
    const runList = Array.isArray(runs) ? runs : [];

    const keys = new Set<string>();
    configList.forEach((c: Record<string, unknown>) => keys.add(String(c.connector_id || c.source || '')));
    runList.forEach((r: Record<string, unknown>) => keys.add(String(r.source || '')));

    const normalized = Array.from(keys)
      .filter(Boolean)
      .map((key) => {
        const display = toDisplay(key);
        const cfg = configList.find((c: Record<string, unknown>) => String(c.connector_id || c.source || '') === key) as Record<string, unknown> | undefined;
        const sourceCandidates = key === 'TED_IE' || key === 'TED_FR' ? ['TED', key] : [key];
        const keyRuns = runList.filter((r: Record<string, unknown>) => sourceCandidates.includes(String(r.source || '')));
        const latestRun = keyRuns[0];
        const latestSuccess = keyRuns.find((r: Record<string, unknown>) => String(r.status || '') === 'success');

        const enabled = cfg?.enabled !== false;
        const status = !enabled
          ? 'disabled'
          : String(latestRun?.status || cfg?.status || 'no_data');

        return {
          key: display.key,
          displayName: display.displayName,
          status,
          lastRunAt: String(latestRun?.started_at || cfg?.last_run_at || '') || null,
          lastSuccessAt: String(cfg?.last_successful_run || latestSuccess?.started_at || '') || null,
          lastError: String(cfg?.last_error || latestRun?.error_summary || '') || null,
          schedule: String(cfg?.fetch_interval || cfg?.schedule || '') || null
        };
      })
      .sort((a, b) => a.key.localeCompare(b.key));

    return Response.json(normalized);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to get connector status' }, { status });
  }
});
