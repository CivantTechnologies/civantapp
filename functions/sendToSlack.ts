import { createClientFromRequest } from './civantSdk.ts';

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }
        
        const body = await req.json();
        const { tender_id, channel } = body;
        
        if (!tender_id) {
            return Response.json({ error: 'tender_id is required' }, { status: 400 });
        }
        
        // Fetch tender details
        const tenders = await civant.entities.TendersCurrent.filter({ id: tender_id });
        if (tenders.length === 0) {
            return Response.json({ error: 'Tender not found' }, { status: 404 });
        }
        
        const tender = tenders[0];
        
        // Get Slack access token
        const accessToken = await civant.asServiceRole.connectors.getAccessToken('slack');
        
        // Format tender information for Slack
        const flag = tender.country === 'FR' ? '🇫🇷' : tender.country === 'IE' ? '🇮🇪' : '🌍';
        const deadlineText = tender.deadline_date 
            ? `*Deadline:* ${new Date(tender.deadline_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}`
            : '*Deadline:* Not specified';
        
        const valueText = tender.estimated_value
            ? `*Value:* ${new Intl.NumberFormat('en', { style: 'currency', currency: tender.currency || 'EUR', maximumFractionDigits: 0 }).format(tender.estimated_value)}`
            : '';
        
        const blocks = [
            {
                type: 'section',
                text: {
                    type: 'mrkdwn',
                    text: `${flag} *${tender.title}*`
                }
            },
            {
                type: 'section',
                fields: [
                    {
                        type: 'mrkdwn',
                        text: `*Buyer:*\n${tender.buyer_name || 'N/A'}`
                    },
                    {
                        type: 'mrkdwn',
                        text: `*Source:*\n${tender.source}`
                    },
                    {
                        type: 'mrkdwn',
                        text: deadlineText
                    },
                    ...(valueText ? [{
                        type: 'mrkdwn',
                        text: valueText
                    }] : [])
                ]
            }
        ];
        
        if (tender.url) {
            blocks.push({
                type: 'actions',
                elements: [
                    {
                        type: 'button',
                        text: {
                            type: 'plain_text',
                            text: 'View Original Notice'
                        },
                        url: tender.url,
                        action_id: 'view_tender'
                    }
                ]
            });
        }
        
        // Post to Slack
        const slackResponse = await fetch('https://slack.com/api/chat.postMessage', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                channel: channel || '#general',
                text: `New tender: ${tender.title}`,
                blocks: blocks
            })
        });
        
        const slackData = await slackResponse.json();
        
        if (!slackData.ok) {
            throw new Error(`Slack API error: ${slackData.error}`);
        }
        
        return Response.json({
            success: true,
            message: 'Tender sent to Slack',
            channel: slackData.channel,
            timestamp: slackData.ts
        });
        
    } catch (error) {
        return Response.json({ 
            success: false, 
            error: error.message 
        }, { status: 500 });
    }
});