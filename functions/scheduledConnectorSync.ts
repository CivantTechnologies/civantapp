import { createClientFromRequest } from './civantSdk.ts';

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (user?.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 403 });
        }
        
        // Fetch all connector configurations
        const configs = await civant.asServiceRole.entities.ConnectorConfig.list();
        const results = [];
        const now = new Date();
        
        for (const config of configs) {
            // Skip disabled connectors or manual-only connectors
            if (!config.enabled || config.fetch_interval === 'manual') {
                results.push({
                    connector_id: config.connector_id,
                    skipped: true,
                    reason: !config.enabled ? 'disabled' : 'manual_only'
                });
                continue;
            }
            
            // Check if connector is due to run
            let isDue = false;
            let intervalHours = 0;
            
            switch (config.fetch_interval) {
                case 'hourly':
                    intervalHours = 1;
                    break;
                case 'daily':
                    intervalHours = 24;
                    break;
                case 'weekly':
                    intervalHours = 24 * 7;
                    break;
            }
            
            if (!config.last_successful_run) {
                isDue = true; // Never run before
            } else {
                const lastRun = new Date(config.last_successful_run);
                const hoursSinceLastRun = (now - lastRun) / (1000 * 60 * 60);
                isDue = hoursSinceLastRun >= intervalHours;
            }
            
            // Also retry if last run failed and it's been at least 1 hour
            if (!isDue && config.last_error && config.last_error_at) {
                const lastError = new Date(config.last_error_at);
                const hoursSinceError = (now - lastError) / (1000 * 60 * 60);
                if (hoursSinceError >= 1) {
                    isDue = true;
                }
            }
            
            if (!isDue) {
                results.push({
                    connector_id: config.connector_id,
                    skipped: true,
                    reason: 'not_due_yet'
                });
                continue;
            }
            
            // Run the connector
            try {
                const params = { days_since: 7, limit: 100, mode: 'incremental' };
                let response;
                
                switch (config.connector_id) {
                    case 'BOAMP_FR':
                        response = await civant.asServiceRole.functions.invoke('fetchBoampFr', params);
                        break;
                    case 'TED_FR':
                        response = await civant.asServiceRole.functions.invoke('fetchTed', { ...params, country: 'FR' });
                        break;
                    case 'TED_IE':
                        response = await civant.asServiceRole.functions.invoke('fetchTed', { ...params, country: 'IE' });
                        break;
                    case 'ETENDERS_IE':
                        response = await civant.asServiceRole.functions.invoke('fetchIreland', params);
                        break;
                }
                
                const updateData = {
                    total_runs: (config.total_runs || 0) + 1
                };
                
                if (response?.data?.success) {
                    updateData.last_successful_run = now.toISOString();
                    updateData.successful_runs = (config.successful_runs || 0) + 1;
                    updateData.last_error = null;
                    updateData.last_error_at = null;
                    
                    results.push({
                        connector_id: config.connector_id,
                        success: true,
                        fetched: response.data.fetched_count || 0,
                        inserted: response.data.inserted_count || 0,
                        updated: response.data.updated_count || 0
                    });
                } else {
                    const errorMsg = response?.data?.error || 'Unknown error';
                    updateData.last_error = errorMsg;
                    updateData.last_error_at = now.toISOString();
                    
                    results.push({
                        connector_id: config.connector_id,
                        success: false,
                        error: errorMsg
                    });
                }
                
                await civant.asServiceRole.entities.ConnectorConfig.update(config.id, updateData);
                
            } catch (error) {
                // Update config with error
                await civant.asServiceRole.entities.ConnectorConfig.update(config.id, {
                    last_error: error.message,
                    last_error_at: now.toISOString(),
                    total_runs: (config.total_runs || 0) + 1
                });
                
                results.push({
                    connector_id: config.connector_id,
                    success: false,
                    error: error.message
                });
            }
        }
        
        // Summary
        const summary = {
            total_connectors: configs.length,
            ran: results.filter(r => !r.skipped).length,
            skipped: results.filter(r => r.skipped).length,
            successful: results.filter(r => r.success === true).length,
            failed: results.filter(r => r.success === false).length
        };
        
        return Response.json({
            success: true,
            timestamp: now.toISOString(),
            summary,
            results
        });
        
    } catch (error) {
        return Response.json({ 
            success: false,
            error: error.message 
        }, { status: 500 });
    }
});