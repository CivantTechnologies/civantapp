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
        
        const body = await req.json() as { tender_id?: string };
        const { tender_id } = body;
        
        if (!tender_id) {
            return Response.json({ error: 'tender_id is required' }, { status: 400 });
        }
        
        // Fetch tender details
        const tenders = await civant.entities.TendersCurrent.filter({ id: tender_id }) as Array<Record<string, unknown>>;
        if (tenders.length === 0) {
            return Response.json({ error: 'Tender not found' }, { status: 404 });
        }
        
        const tender = tenders[0] as Record<string, any>;
        
        if (!tender.deadline_date) {
            return Response.json({ error: 'Tender has no deadline date' }, { status: 400 });
        }
        
        // Get Google Calendar access token
        const accessToken = await civant.asServiceRole.connectors.getAccessToken('googlecalendar');
        
        // Parse deadline date
        const deadlineDate = new Date(tender.deadline_date);
        const startDateTime = new Date(deadlineDate);
        startDateTime.setHours(9, 0, 0, 0);
        const endDateTime = new Date(deadlineDate);
        endDateTime.setHours(10, 0, 0, 0);
        
        // Create calendar event
        const event = {
            summary: `📋 Tender Deadline: ${tender.title}`,
            description: `Tender: ${tender.title}\nBuyer: ${tender.buyer_name || 'N/A'}\nSource: ${tender.source}\nURL: ${tender.url || 'N/A'}\n\nTracked by Civant`,
            start: {
                dateTime: startDateTime.toISOString(),
                timeZone: 'Europe/Dublin'
            },
            end: {
                dateTime: endDateTime.toISOString(),
                timeZone: 'Europe/Dublin'
            },
            reminders: {
                useDefault: false,
                overrides: [
                    { method: 'email', minutes: 24 * 60 },
                    { method: 'popup', minutes: 60 }
                ]
            }
        };
        
        const calendarResponse = await fetch(
            'https://www.googleapis.com/calendar/v3/calendars/primary/events',
            {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${accessToken}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(event)
            }
        );
        
        if (!calendarResponse.ok) {
            const error = await calendarResponse.text();
            throw new Error(`Calendar API error: ${error}`);
        }
        
        const calendarEvent = await calendarResponse.json() as { id?: string; htmlLink?: string };
        
        return Response.json({
            success: true,
            event_id: calendarEvent.id,
            event_link: calendarEvent.htmlLink,
            message: 'Tender deadline added to calendar'
        });
        
    } catch (error: unknown) {
        return Response.json({ 
            success: false, 
            error: getErrorMessage(error)
        }, { status: 500 });
    }
});
