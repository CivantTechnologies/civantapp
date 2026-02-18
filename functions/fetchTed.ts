// @ts-nocheck
import { createClientFromRequest } from './civantSdk.ts';
import { offloadPayload } from './payloadOffload.ts';

// TED API - EU Publications Office
// Using the TED Open Data API via data.europa.eu
const TED_API_URL = 'https://ted.europa.eu/api/v3.0/notices/search';
const TED_OPEN_DATA_URL = 'https://data.europa.eu/api/hub/search/search';

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
    
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
        const char = key.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
    }
    return hash.toString(16);
}

// Normalize TED record to canonical format
function normalizeTedRecord(raw, country) {
    // Handle different TED data structures
    const sourceNoticeId = raw.id || raw.notice_id || raw.doc_id || raw.ted_notice_number || String(Date.now() + Math.random());
    
    // Parse title
    let title = 'Untitled';
    if (raw.title) {
        title = typeof raw.title === 'object' ? (raw.title.en || raw.title.fr || Object.values(raw.title)[0] || 'Untitled') : raw.title;
    } else if (raw.notice_title) {
        title = raw.notice_title;
    } else if (raw.object_contract?.title) {
        title = raw.object_contract.title;
    }
    
    // Parse buyer name
    let buyerName = '';
    if (raw.contracting_body?.official_name) {
        buyerName = raw.contracting_body.official_name;
    } else if (raw.buyer_name) {
        buyerName = raw.buyer_name;
    } else if (raw.ca_name) {
        buyerName = raw.ca_name;
    } else if (raw.organisation_name) {
        buyerName = raw.organisation_name;
    }
    
    // Parse dates
    let publicationDate = null;
    if (raw.publication_date) {
        publicationDate = raw.publication_date.split('T')[0];
    } else if (raw.dispatch_date) {
        publicationDate = raw.dispatch_date.split('T')[0];
    } else if (raw.date_pub) {
        publicationDate = raw.date_pub.split('T')[0];
    }
    
    let deadlineDate = null;
    if (raw.submission_deadline) {
        deadlineDate = raw.submission_deadline.split('T')[0];
    } else if (raw.deadline_receipt_tenders) {
        deadlineDate = raw.deadline_receipt_tenders.split('T')[0];
    } else if (raw.dt_date_for_submission) {
        deadlineDate = raw.dt_date_for_submission.split('T')[0];
    }
    
    // Parse CPV codes
    let cpvCodes = '';
    if (raw.cpv_codes) {
        cpvCodes = Array.isArray(raw.cpv_codes) ? raw.cpv_codes.join(',') : raw.cpv_codes;
    } else if (raw.cpv) {
        cpvCodes = Array.isArray(raw.cpv) ? raw.cpv.map(c => c.code || c).join(',') : raw.cpv;
    }
    
    // Determine notice type
    let noticeType = 'unknown';
    const typeStr = (raw.notice_type || raw.type_of_notice || raw.form_type || '').toLowerCase();
    if (typeStr.includes('award') || typeStr.includes('result')) {
        noticeType = 'award';
    } else if (typeStr.includes('corrig') || typeStr.includes('modif')) {
        noticeType = 'corrigendum';
    } else if (typeStr.includes('contract') || typeStr.includes('tender') || typeStr.includes('notice')) {
        noticeType = 'tender';
    }
    
    // Parse estimated value
    let estimatedValue = null;
    if (raw.estimated_value?.value) {
        estimatedValue = parseFloat(raw.estimated_value.value);
    } else if (raw.value_magnitude) {
        estimatedValue = parseFloat(raw.value_magnitude);
    } else if (raw.estimated_total?.value) {
        estimatedValue = parseFloat(raw.estimated_total.value);
    }
    
    // Build URL
    let url = raw.url || raw.notice_url || '';
    if (!url && sourceNoticeId) {
        url = `https://ted.europa.eu/udl?uri=TED:NOTICE:${sourceNoticeId}:TEXT:EN:HTML`;
    }
    
    return {
        tender_uid: `TED:${sourceNoticeId}`,
        source: 'TED',
        source_notice_id: sourceNoticeId,
        country: country,
        notice_type: noticeType,
        title: title,
        buyer_name: buyerName,
        cpv_codes: cpvCodes,
        publication_date: publicationDate,
        deadline_date: deadlineDate,
        estimated_value: estimatedValue,
        currency: raw.currency || 'EUR',
        url: url,
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
        const tenantId = String(
            body.tenant_id
            || req.headers.get('X-Tenant-Id')
            || Deno.env.get('DEFAULT_TENANT_ID')
            || 'civant_default'
        );
        const country = body.country || 'IE';
        const mode = body.mode || 'incremental';
        const limit = body.limit || 100;
        
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
            tenant_id: tenantId,
            source: 'TED',
            country: country,
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
            let records = [];
            
            // Use TED's official search API
            const tedSearchUrl = 'https://ted.europa.eu/api/v3.0/notices/search';
            
            const searchBody = {
                q: "*",
                scope: 3,
                limit: limit,
                fields: ["ND", "PD", "OJ", "TW", "AA", "CY", "OL", "HD"],
                sortField: "PD",
                reverseOrder: true,
                filters: {
                    country: [country],
                    publicationDate: {
                        startDate: sinceDateStr
                    }
                }
            };
            
            try {
                const tedResponse = await fetch(tedSearchUrl, {
                    method: 'POST',
                    headers: {
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(searchBody),
                    signal: AbortSignal.timeout(45000) // 45s timeout for TED
                });

                if (!tedResponse.ok) {
                    const errorText = await tedResponse.text().catch(() => 'Unknown error');
                    throw new Error(`TED API returned ${tedResponse.status}: ${errorText.substring(0, 200)}`);
                }

                const tedData = await tedResponse.json();
                records = tedData.notices || tedData.results || [];

                if (!Array.isArray(records)) {
                    throw new Error('TED API returned invalid data structure');
                }
            } catch (tedError) {
                errors.push(`TED API error: ${tedError.message}`);
                // Continue without data rather than failing completely
                records = [];
            }
            
            fetchedCount = records.length;
            
            // Process each record
            for (const raw of records) {
                try {
                    if (!raw || typeof raw !== 'object') {
                        errors.push(`Invalid record format: ${JSON.stringify(raw).substring(0, 100)}`);
                        continue;
                    }

                    const normalized = normalizeTedRecord(raw, country);
                    const data = { ...normalized };
                    if (typeof data.raw_json === 'string') {
                        try {
                            data.raw_json = JSON.parse(data.raw_json);
                        } catch {
                            // keep as string if not valid JSON
                        }
                    }

                    if (!normalized.tender_uid || !normalized.title) {
                        errors.push(`Missing required fields in TED record: ${raw.id || 'unknown'}`);
                        continue;
                    }

                    const fingerprint = generateFingerprint(normalized);
                    normalized.fingerprint = fingerprint;
                    
                    // Check if tender exists
                    const existing = await civant.asServiceRole.entities.TendersCurrent.filter({
                        tenant_id: tenantId,
                        tender_uid: normalized.tender_uid
                    });
                    
                    const now = new Date().toISOString();
                    
                    const publishedIso = normalized.publication_date
                        ? `${normalized.publication_date}T00:00:00.000Z`
                        : now;

                    const offload = await offloadPayload({
                        civant,
                        tenantId,
                        tableName: 'TendersCurrent',
                        primaryKey: normalized.tender_uid,
                        payload: data
                    });
                    const payloadMeta = offload.offloaded ? {
                        raw_object_key: offload.raw_object_key,
                        payload_hash_sha256: offload.payload_hash_sha256,
                        payload_bytes: offload.payload_bytes,
                        payload_stored_at: offload.payload_stored_at
                    } : {};

                    if (existing.length === 0) {
                        // New tender
                        data.first_seen_at = now;
                        data.last_seen_at = now;
                        data.version_count = 1;

                        await civant.asServiceRole.entities.TendersCurrent.create({
                            tenant_id: tenantId,
                            tender_id: normalized.tender_uid,
                            source: normalized.source,
                            published_at: publishedIso,
                            data,
                            ...payloadMeta
                        });
                        
                        // Create initial version record
                        await civant.asServiceRole.entities.TenderVersions.create({
                            tenant_id: tenantId,
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
                        const updateData = {
                            tenant_id: tenantId,
                            published_at: publishedIso,
                            data,
                            ...payloadMeta
                        };
                        
                        if (current.fingerprint !== fingerprint) {
                            const newVersionNum = (current.version_count || 1) + 1;
                            
                            let changeType = 'field_changed';
                            if (current.deadline_date !== normalized.deadline_date) {
                                changeType = 'deadline_changed';
                            }
                            
                            await civant.asServiceRole.entities.TenderVersions.create({
                                tenant_id: tenantId,
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
                            
                            data.fingerprint = fingerprint;
                            data.version_count = newVersionNum;
                            data.title = normalized.title;
                            data.buyer_name = normalized.buyer_name;
                            data.deadline_date = normalized.deadline_date;
                            data.estimated_value = normalized.estimated_value;
                            data.raw_json = data.raw_json ?? normalized.raw_json;
                            
                            versionedCount++;
                        }

                        data.last_seen_at = now;
                        await civant.asServiceRole.entities.TendersCurrent.update(current.id, updateData);
                        updatedCount++;
                        }
                        } catch (recordError) {
                        if (recordError?.payloadOffloadFatal) {
                        throw recordError;
                        }
                        errors.push(`Record processing error (${raw.id || 'unknown'}): ${recordError.message}`);
                        if (errors.length > 50) {
                        throw new Error('Too many record errors, aborting');
                        }
                        }
                        }
            
            // Update run log
            await civant.asServiceRole.entities.ConnectorRuns.update(runLog.id, {
                tenant_id: tenantId,
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
                country: country,
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
                tenant_id: tenantId,
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
