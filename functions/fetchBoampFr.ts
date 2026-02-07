import { createClientFromRequest } from './civantSdk.ts';

// BOAMP OpenDataSoft API endpoint
const BOAMP_API_URL = 'https://www.data.gouv.fr/api/1/datasets/aife-5f26fe1d2c9aa8d76b3a90dd/resources/';
const BOAMP_OPENDATASOFT_URL = 'https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records';

// Generate fingerprint for change detection
function generateFingerprint(record) {
    const key = [
        record.title || '',
        record.buyer_name || '',
        record.publication_date || '',
        record.deadline_date || '',
        record.cpv_codes || '',
        String(record.estimated_value || '')
    ].join('|');
    
    // Simple hash function
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
        const char = key.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
    }
    return hash.toString(16);
}

// Normalize BOAMP record to canonical format
function normalizeBoampRecord(raw) {
    const sourceNoticeId = raw.idweb || raw.id || String(Date.now());
    
    // Parse dates
    let publicationDate = null;
    if (raw.dateparution) {
        publicationDate = raw.dateparution.split('T')[0];
    }
    
    let deadlineDate = null;
    if (raw.datelimitereponse) {
        deadlineDate = raw.datelimitereponse.split('T')[0];
    }
    
    // Parse CPV codes
    let cpvCodes = '';
    if (raw.codecpv) {
        cpvCodes = Array.isArray(raw.codecpv) ? raw.codecpv.join(',') : raw.codecpv;
    }
    
    // Determine notice type
    let noticeType = 'unknown';
    const natureStr = (raw.nature || '').toLowerCase();
    if (natureStr.includes('attrib') || natureStr.includes('award')) {
        noticeType = 'award';
    } else if (natureStr.includes('rectif') || natureStr.includes('corrig')) {
        noticeType = 'corrigendum';
    } else if (natureStr.includes('avis') || natureStr.includes('appel') || natureStr.includes('marché')) {
        noticeType = 'tender';
    }
    
    // Parse estimated value
    let estimatedValue = null;
    if (raw.montant || raw.montantmarche) {
        estimatedValue = parseFloat(raw.montant || raw.montantmarche) || null;
    }
    
    return {
        tender_uid: `BOAMP_FR:${sourceNoticeId}`,
        source: 'BOAMP_FR',
        source_notice_id: sourceNoticeId,
        country: 'FR',
        notice_type: noticeType,
        title: raw.objet || raw.intitule || 'Untitled',
        buyer_name: raw.nomacheteur || raw.organisme || '',
        cpv_codes: cpvCodes,
        publication_date: publicationDate,
        deadline_date: deadlineDate,
        estimated_value: estimatedValue,
        currency: 'EUR',
        url: raw.urlavis || `https://www.boamp.fr/avis/detail/${sourceNoticeId}`,
        raw_json: JSON.stringify(raw)
    };
}

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 403 });
        }
        
        const body = await req.json().catch(() => ({}));
        const mode = body.mode || 'incremental';
        const limit = body.limit || 100;
        const offset = body.offset || 0;
        
        // Calculate date range
        let daysSince = body.days_since || 1825; // 5 years default
        if (mode === 'backfill') {
            const yearsBack = body.years_back || 10;
            daysSince = yearsBack * 365;
        }
        
        const sinceDate = new Date();
        sinceDate.setDate(sinceDate.getDate() - daysSince);
        const sinceDateStr = sinceDate.toISOString().split('T')[0];
        
        // Create connector run log
        const runLog = await civant.asServiceRole.entities.ConnectorRuns.create({
            source: 'BOAMP_FR',
            country: 'FR',
            started_at: new Date().toISOString(),
            status: 'partial',
            fetched_count: 0,
            inserted_count: 0,
            updated_count: 0,
            versioned_count: 0
        });
        
        let fetchedCount = 0;
        let insertedCount = 0;
        let updatedCount = 0;
        let versionedCount = 0;
        let errors = [];
        
        try {
            // Fetch from BOAMP OpenDataSoft API
            const apiUrl = `${BOAMP_OPENDATASOFT_URL}?limit=${limit}&offset=${offset}&where=dateparution >= '${sinceDateStr}'&order_by=dateparution DESC`;

            let response;
            try {
                response = await fetch(apiUrl, {
                    headers: { 'Accept': 'application/json' },
                    signal: AbortSignal.timeout(30000) // 30s timeout
                });
            } catch (fetchError) {
                throw new Error(`Network error: ${fetchError.message}`);
            }

            if (!response.ok) {
                const errorText = await response.text().catch(() => 'Unknown error');
                throw new Error(`BOAMP API error: ${response.status} ${response.statusText} - ${errorText}`);
            }

            let data;
            try {
                data = await response.json();
            } catch (parseError) {
                throw new Error(`Failed to parse API response: ${parseError.message}`);
            }

            const records = data.results || data.records || [];
            fetchedCount = records.length;

            if (!Array.isArray(records)) {
                throw new Error('API returned invalid data format');
            }
            
            // Process each record
            for (const raw of records) {
                try {
                    if (!raw || typeof raw !== 'object') {
                        errors.push(`Invalid record format: ${JSON.stringify(raw).substring(0, 100)}`);
                        continue;
                    }

                    const normalized = normalizeBoampRecord(raw);

                    if (!normalized.tender_uid || !normalized.title) {
                        errors.push(`Missing required fields in record: ${raw.idweb || 'unknown'}`);
                        continue;
                    }

                    const fingerprint = generateFingerprint(normalized);
                    normalized.fingerprint = fingerprint;
                    
                    // Check if tender exists
                    const existing = await civant.asServiceRole.entities.TendersCurrent.filter({
                        tender_uid: normalized.tender_uid
                    });
                    
                    const now = new Date().toISOString();
                    
                    if (existing.length === 0) {
                        // New tender
                        normalized.first_seen_at = now;
                        normalized.last_seen_at = now;
                        normalized.version_count = 1;
                        
                        await civant.asServiceRole.entities.TendersCurrent.create(normalized);
                        
                        // Create initial version record
                        await civant.asServiceRole.entities.TenderVersions.create({
                            tender_uid: normalized.tender_uid,
                            version_number: 1,
                            change_date: now,
                            change_type: 'new_notice',
                            new_value: JSON.stringify(normalized),
                            fingerprint: fingerprint
                        });
                        
                        insertedCount++;
                    } else {
                        // Existing tender - check for changes
                        const current = existing[0];
                        
                        // Update last_seen_at
                        const updateData = { last_seen_at: now };
                        
                        if (current.fingerprint !== fingerprint) {
                            // Fingerprint changed - create new version
                            const newVersionNum = (current.version_count || 1) + 1;
                            
                            // Determine change type
                            let changeType = 'field_changed';
                            if (current.deadline_date !== normalized.deadline_date) {
                                changeType = 'deadline_changed';
                            }
                            
                            // Create version record
                            await civant.asServiceRole.entities.TenderVersions.create({
                                tender_uid: normalized.tender_uid,
                                version_number: newVersionNum,
                                change_date: now,
                                change_type: changeType,
                                old_value: JSON.stringify({
                                    title: current.title,
                                    buyer_name: current.buyer_name,
                                    deadline_date: current.deadline_date,
                                    estimated_value: current.estimated_value,
                                    fingerprint: current.fingerprint
                                }),
                                new_value: JSON.stringify({
                                    title: normalized.title,
                                    buyer_name: normalized.buyer_name,
                                    deadline_date: normalized.deadline_date,
                                    estimated_value: normalized.estimated_value,
                                    fingerprint: fingerprint
                                }),
                                fingerprint: fingerprint
                            });
                            
                            updateData.fingerprint = fingerprint;
                            updateData.version_count = newVersionNum;
                            updateData.title = normalized.title;
                            updateData.buyer_name = normalized.buyer_name;
                            updateData.deadline_date = normalized.deadline_date;
                            updateData.estimated_value = normalized.estimated_value;
                            updateData.raw_json = normalized.raw_json;
                            
                            versionedCount++;
                        }
                        
                        await civant.asServiceRole.entities.TendersCurrent.update(current.id, updateData);
                        updatedCount++;
                        }
                        } catch (recordError) {
                        errors.push(`Record processing error (${raw.idweb || 'unknown'}): ${recordError.message}`);
                        if (errors.length > 50) {
                        throw new Error('Too many record errors, aborting');
                        }
                        }
                        }
            
            // Update run log
            await civant.asServiceRole.entities.ConnectorRuns.update(runLog.id, {
                finished_at: new Date().toISOString(),
                status: errors.length === 0 ? 'success' : 'partial',
                fetched_count: fetchedCount,
                inserted_count: insertedCount,
                updated_count: updatedCount,
                versioned_count: versionedCount,
                error_summary: errors.length > 0 ? errors.slice(0, 10).join('\n') : null
            });
            
            return Response.json({
                success: true,
                mode: mode,
                date_from: sinceDateStr,
                fetched_count: fetchedCount,
                inserted_count: insertedCount,
                updated_count: updatedCount,
                versioned_count: versionedCount,
                errors: errors.slice(0, 10),
                run_id: runLog.id,
                has_more: fetchedCount === limit
            });
            
        } catch (fetchError) {
            await civant.asServiceRole.entities.ConnectorRuns.update(runLog.id, {
                finished_at: new Date().toISOString(),
                status: 'fail',
                error_summary: fetchError.message
            });
            
            return Response.json({
                success: false,
                error: fetchError.message,
                run_id: runLog.id
            }, { status: 500 });
        }
        
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});