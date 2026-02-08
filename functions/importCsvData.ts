import { createClientFromRequest } from './civantSdk.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

type ImportedTenderRecord = {
    tender_uid?: string;
    source?: string;
    source_notice_id?: string;
    country?: string;
    notice_type?: string;
    title?: string;
    buyer_name?: string;
    cpv_codes?: string;
    publication_date?: string;
    deadline_date?: string;
    estimated_value?: number;
    currency?: string;
    url?: string;
};

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Unauthorized - Admin access required' }, { status: 403 });
        }
        
        const { file_url, data_type } = await req.json() as { file_url?: string; data_type?: string };
        
        if (!file_url) {
            return Response.json({ error: 'file_url is required' }, { status: 400 });
        }
        
        const type = data_type || 'tenders';
        
        // Define schema based on data type
        const tenderSchema = {
            type: "object",
            properties: {
                tender_uid: { type: "string" },
                source: { type: "string" },
                source_notice_id: { type: "string" },
                country: { type: "string" },
                notice_type: { type: "string" },
                title: { type: "string" },
                buyer_name: { type: "string" },
                cpv_codes: { type: "string" },
                publication_date: { type: "string" },
                deadline_date: { type: "string" },
                estimated_value: { type: "number" },
                currency: { type: "string" },
                url: { type: "string" }
            }
        };
        
        // Extract data from file
        const extractResult = await civant.integrations.Core.ExtractDataFromUploadedFile({
            file_url: file_url,
            json_schema: tenderSchema
        });
        
        if (extractResult.status === 'error') {
            return Response.json({ 
                error: 'Failed to extract data from file',
                details: extractResult.details
            }, { status: 400 });
        }
        
        const records: ImportedTenderRecord[] = Array.isArray(extractResult.output) ? extractResult.output : [extractResult.output];
        
        if (records.length === 0) {
            return Response.json({ error: 'No valid records found in file' }, { status: 400 });
        }
        
        // Process and insert records
        let inserted = 0;
        let updated = 0;
        const errors: Array<{ record: string; error: string }> = [];
        
        for (const record of records) {
            try {
                // Generate fingerprint
                const fingerprintData = [
                    record.title,
                    record.buyer_name,
                    record.deadline_date,
                    record.estimated_value
                ].filter(Boolean).join('|');
                
                const encoder = new TextEncoder();
                const data = encoder.encode(fingerprintData);
                const hashBuffer = await crypto.subtle.digest('SHA-256', data);
                const hashArray = Array.from(new Uint8Array(hashBuffer));
                const fingerprint = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
                
                // Prepare tender data
                const tenderData = {
                    tender_uid: record.tender_uid || `IMPORT:${record.source_notice_id || Date.now()}`,
                    source: record.source || 'CSV_IMPORT',
                    source_notice_id: record.source_notice_id || `import_${Date.now()}_${inserted}`,
                    country: record.country || 'IE',
                    notice_type: record.notice_type || 'tender',
                    title: record.title,
                    buyer_name: record.buyer_name,
                    cpv_codes: record.cpv_codes,
                    publication_date: record.publication_date,
                    deadline_date: record.deadline_date,
                    estimated_value: record.estimated_value,
                    currency: record.currency || 'EUR',
                    url: record.url,
                    fingerprint: fingerprint,
                    first_seen_at: new Date().toISOString(),
                    last_seen_at: new Date().toISOString(),
                    version_count: 1
                };
                
                // Check if exists
                const existing = await civant.asServiceRole.entities.TendersCurrent.filter({
                    tender_uid: tenderData.tender_uid
                }) as Array<Record<string, any>>;
                
                if (existing.length > 0) {
                    // Update if fingerprint changed
                    if (existing[0].fingerprint !== fingerprint) {
                        await civant.asServiceRole.entities.TendersCurrent.update(existing[0].id, {
                            ...tenderData,
                            version_count: (existing[0].version_count || 1) + 1
                        });
                        updated++;
                    }
                } else {
                    // Insert new
                    await civant.asServiceRole.entities.TendersCurrent.create(tenderData);
                    inserted++;
                }
            } catch (error: unknown) {
                errors.push({
                    record: String(record.title || record.source_notice_id || 'unknown_record'),
                    error: getErrorMessage(error)
                });
            }
        }
        
        return Response.json({
            success: true,
            total_records: records.length,
            inserted: inserted,
            updated: updated,
            errors: errors.length,
            error_details: errors.slice(0, 5)
        });
        
    } catch (error: unknown) {
        return Response.json({ 
            error: getErrorMessage(error) || 'Import failed'
        }, { status: 500 });
    }
});
