import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant } from './requireAdmin.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

function destructiveOpsEnabled() {
  return String(Deno.env.get('ENABLE_DESTRUCTIVE_OPS') || '').trim().toLowerCase() === 'true';
}

type DeletionLog = {
  tenders: number;
  versions: number;
  runs: number;
  alerts: number;
  alertEvents: number;
  competitors: number;
  connectorConfigs: number;
};

Deno.serve(async (req) => {
  const deletionLog: DeletionLog = {
    tenders: 0,
    versions: 0,
    runs: 0,
    alerts: 0,
    alertEvents: 0,
    competitors: 0,
    connectorConfigs: 0
  };
  const skippedTables = ['TenderEnrichment', 'PredictionFeedback'];

  try {
    if (!destructiveOpsEnabled()) {
      return Response.json({ error: 'Not found' }, { status: 404 });
    }

    const civant = createClientFromRequest(req);
    const tenantId = getTenantFromHeader(req);
    const user = await requireAdminForTenant({ civant, req, tenantId });

    const body = await req.json().catch(() => ({}));
    const confirm = String((body as { confirm?: unknown }).confirm || '').trim();
    const expectedConfirm = `PURGE_${tenantId}`;
    if (confirm !== expectedConfirm) {
      return Response.json({ error: 'Invalid purge confirmation token' }, { status: 403 });
    }

    const entities = civant.asServiceRole.entities as Record<string, {
      filter: (where: Record<string, unknown>) => Promise<Array<Record<string, unknown>>>;
      delete: (id: string) => Promise<unknown>;
    }>;

    const deleteTenantRows = async (tableName: string): Promise<number> => {
      const rows = await entities[tableName].filter({ tenant_id: tenantId });
      let deleted = 0;
      for (const row of rows) {
        const rowId = String(row?.id || '').trim();
        if (!rowId) continue;
        await entities[tableName].delete(rowId);
        deleted++;
      }
      return deleted;
    };

    deletionLog.tenders = await deleteTenantRows('TendersCurrent');
    deletionLog.versions = await deleteTenantRows('TenderVersions');
    deletionLog.runs = await deleteTenantRows('ConnectorRuns');
    deletionLog.alerts = await deleteTenantRows('Alerts');
    deletionLog.alertEvents = await deleteTenantRows('AlertEvents');
    deletionLog.competitors = await deleteTenantRows('Competitors');
    deletionLog.connectorConfigs = await deleteTenantRows('ConnectorConfig');

    console.warn('[purgeAllData] tenant purge executed', {
      tenantId,
      userId: user.userId,
      email: user.email,
      deleted: deletionLog,
      skippedTables
    });

    return Response.json({
      success: true,
      message: 'Tenant data purged successfully',
      tenantId,
      deleted: deletionLog,
      skippedTables,
      total: Object.values(deletionLog).reduce((a, b) => a + b, 0)
    });
  } catch (error: unknown) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({
      success: false,
      error: getErrorMessage(error),
      partialDeletion: deletionLog
    }, { status });
  }
});
