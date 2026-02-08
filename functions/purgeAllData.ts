import { createClientFromRequest } from './civantSdk.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 403 });
        }
        
        const deletionLog = {
            tenders: 0,
            versions: 0,
            runs: 0,
            alerts: 0,
            alertEvents: 0,
            competitors: 0,
            enrichments: 0,
            feedback: 0,
            connectorConfigs: 0
        };
        
        try {
            // Delete TendersCurrent
            const tenders = await civant.asServiceRole.entities.TendersCurrent.list();
            for (const tender of tenders) {
                await civant.asServiceRole.entities.TendersCurrent.delete(tender.id);
                deletionLog.tenders++;
            }
            
            // Delete TenderVersions
            const versions = await civant.asServiceRole.entities.TenderVersions.list();
            for (const version of versions) {
                await civant.asServiceRole.entities.TenderVersions.delete(version.id);
                deletionLog.versions++;
            }
            
            // Delete ConnectorRuns
            const runs = await civant.asServiceRole.entities.ConnectorRuns.list();
            for (const run of runs) {
                await civant.asServiceRole.entities.ConnectorRuns.delete(run.id);
                deletionLog.runs++;
            }
            
            // Delete Alerts
            const alerts = await civant.asServiceRole.entities.Alerts.list();
            for (const alert of alerts) {
                await civant.asServiceRole.entities.Alerts.delete(alert.id);
                deletionLog.alerts++;
            }
            
            // Delete AlertEvents
            const events = await civant.asServiceRole.entities.AlertEvents.list();
            for (const event of events) {
                await civant.asServiceRole.entities.AlertEvents.delete(event.id);
                deletionLog.alertEvents++;
            }
            
            // Delete Competitors
            const competitors = await civant.asServiceRole.entities.Competitors.list();
            for (const competitor of competitors) {
                await civant.asServiceRole.entities.Competitors.delete(competitor.id);
                deletionLog.competitors++;
            }
            
            // Delete TenderEnrichment
            const enrichments = await civant.asServiceRole.entities.TenderEnrichment.list();
            for (const enrichment of enrichments) {
                await civant.asServiceRole.entities.TenderEnrichment.delete(enrichment.id);
                deletionLog.enrichments++;
            }
            
            // Delete PredictionFeedback
            const feedback = await civant.asServiceRole.entities.PredictionFeedback.list();
            for (const fb of feedback) {
                await civant.asServiceRole.entities.PredictionFeedback.delete(fb.id);
                deletionLog.feedback++;
            }
            
            // Delete ConnectorConfig
            const configs = await civant.asServiceRole.entities.ConnectorConfig.list();
            for (const config of configs) {
                await civant.asServiceRole.entities.ConnectorConfig.delete(config.id);
                deletionLog.connectorConfigs++;
            }
            
            return Response.json({
                success: true,
                message: 'All data purged successfully',
                deleted: deletionLog,
                total: Object.values(deletionLog).reduce((a, b) => a + b, 0)
            });
            
        } catch (error: unknown) {
            return Response.json({
                success: false,
                error: getErrorMessage(error),
                partialDeletion: deletionLog
            }, { status: 500 });
        }
        
    } catch (error: unknown) {
        return Response.json({ error: getErrorMessage(error) }, { status: 500 });
    }
});
