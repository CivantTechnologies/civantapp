import { createClientFromRequest } from './civantSdk.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }
        
        const body = await req.json() as { report_type?: string; filters?: Record<string, unknown> };
        const { report_type, filters } = body;
        void report_type;
        
        // Fetch tenders based on filters
        const tenders = (filters 
            ? await civant.entities.TendersCurrent.filter(filters)
            : await civant.entities.TendersCurrent.list('-publication_date', 500)) as Array<Record<string, any>>;
        
        // Generate CSV content
        const headers = [
            'Title',
            'Buyer',
            'Country',
            'Source',
            'Publication Date',
            'Deadline Date',
            'Estimated Value',
            'Currency',
            'CPV Codes',
            'Notice Type',
            'URL'
        ];
        
        const rows = tenders.map((t: Record<string, any>) => [
            t.title || '',
            t.buyer_name || '',
            t.country || '',
            t.source || '',
            t.publication_date || '',
            t.deadline_date || '',
            t.estimated_value || '',
            t.currency || '',
            t.cpv_codes || '',
            t.notice_type || '',
            t.url || ''
        ]);
        
        const csvContent = [
            headers.join(','),
            ...rows.map((row: Array<string | number>) => row.map((cell: string | number) => 
                `"${String(cell).replace(/"/g, '""')}"`
            ).join(','))
        ].join('\n');
        
        // Get Google Drive access token
        const accessToken = await civant.asServiceRole.connectors.getAccessToken('googledrive');
        
        // Create file metadata
        const fileName = `Tender_Radar_Export_${new Date().toISOString().split('T')[0]}.csv`;
        const metadata = {
            name: fileName,
            mimeType: 'text/csv'
        };
        
        // Create multipart upload
        const boundary = '-------314159265358979323846';
        const delimiter = `\r\n--${boundary}\r\n`;
        const closeDelimiter = `\r\n--${boundary}--`;
        
        const multipartRequestBody = 
            delimiter +
            'Content-Type: application/json; charset=UTF-8\r\n\r\n' +
            JSON.stringify(metadata) +
            delimiter +
            'Content-Type: text/csv\r\n\r\n' +
            csvContent +
            closeDelimiter;
        
        const driveResponse = await fetch(
            'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart',
            {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${accessToken}`,
                    'Content-Type': `multipart/related; boundary=${boundary}`
                },
                body: multipartRequestBody
            }
        );
        
        if (!driveResponse.ok) {
            const error = await driveResponse.text();
            throw new Error(`Drive API error: ${error}`);
        }
        
        const file = await driveResponse.json() as { id?: string; name?: string };
        
        return Response.json({
            success: true,
            file_id: file.id,
            file_name: file.name,
            file_link: `https://drive.google.com/file/d/${file.id}/view`,
            message: 'Report exported to Google Drive',
            record_count: tenders.length
        });
        
    } catch (error: unknown) {
        return Response.json({ 
            success: false, 
            error: getErrorMessage(error)
        }, { status: 500 });
    }
});
