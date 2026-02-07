import { createClientFromRequest } from './civantSdk.ts';

// Process alerts and send email notifications for matching tenders
Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 403 });
        }
        
        // Get all active alerts
        const alerts = await civant.asServiceRole.entities.Alerts.filter({
            active: true
        });
        
        // Get recent tenders (last 24 hours)
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        
        const allTenders = await civant.asServiceRole.entities.TendersCurrent.list('-first_seen_at', 500);
        const recentTenders = allTenders.filter(t => 
            t.first_seen_at && new Date(t.first_seen_at) >= yesterday
        );
        
        let matchCount = 0;
        let emailsSent = 0;
        const errors = [];
        
        for (const alert of alerts) {
            try {
                // Find matching tenders
                const matches = recentTenders.filter(tender => {
                    // Country filter
                    if (alert.country && tender.country !== alert.country) {
                        return false;
                    }
                    
                    // Keywords filter
                    if (alert.keywords) {
                        const keywords = alert.keywords.toLowerCase().split(',').map(k => k.trim());
                        const title = (tender.title || '').toLowerCase();
                        const buyer = (tender.buyer_name || '').toLowerCase();
                        const hasKeyword = keywords.some(kw => 
                            title.includes(kw) || buyer.includes(kw)
                        );
                        if (!hasKeyword) return false;
                    }
                    
                    // Buyer contains filter
                    if (alert.buyer_contains) {
                        const buyerFilter = alert.buyer_contains.toLowerCase();
                        const buyer = (tender.buyer_name || '').toLowerCase();
                        if (!buyer.includes(buyerFilter)) return false;
                    }
                    
                    // CPV filter
                    if (alert.cpv_contains) {
                        const cpv = tender.cpv_codes || '';
                        if (!cpv.includes(alert.cpv_contains)) return false;
                    }
                    
                    // Deadline within days
                    if (alert.deadline_within_days && tender.deadline_date) {
                        const deadline = new Date(tender.deadline_date);
                        const now = new Date();
                        const daysUntilDeadline = Math.ceil((deadline - now) / (1000 * 60 * 60 * 24));
                        if (daysUntilDeadline < 0 || daysUntilDeadline > alert.deadline_within_days) {
                            return false;
                        }
                    }
                    
                    return true;
                });
                
                // Check for existing alert events to avoid duplicates
                for (const tender of matches) {
                    const existingEvents = await civant.asServiceRole.entities.AlertEvents.filter({
                        alert_id: alert.id,
                        tender_uid: tender.tender_uid
                    });
                    
                    if (existingEvents.length === 0) {
                        // Create alert event
                        const event = await civant.asServiceRole.entities.AlertEvents.create({
                            alert_id: alert.id,
                            tender_uid: tender.tender_uid,
                            matched_at: new Date().toISOString(),
                            sent: false
                        });
                        
                        matchCount++;
                        
                        // Send email notification
                        try {
                            const emailBody = `
                                <h2>New Tender Match: ${alert.alert_name}</h2>
                                <p>A new tender matching your alert criteria has been published:</p>
                                <hr>
                                <h3>${tender.title}</h3>
                                <p><strong>Buyer:</strong> ${tender.buyer_name || 'Not specified'}</p>
                                <p><strong>Country:</strong> ${tender.country === 'FR' ? 'France 🇫🇷' : 'Ireland 🇮🇪'}</p>
                                <p><strong>Publication Date:</strong> ${tender.publication_date || 'Not specified'}</p>
                                <p><strong>Deadline:</strong> ${tender.deadline_date || 'Not specified'}</p>
                                ${tender.estimated_value ? `<p><strong>Estimated Value:</strong> €${tender.estimated_value.toLocaleString()}</p>` : ''}
                                ${tender.cpv_codes ? `<p><strong>CPV Codes:</strong> ${tender.cpv_codes}</p>` : ''}
                                <hr>
                                <p><a href="${tender.url || '#'}">View Original Notice</a></p>
                                <p style="color: #666; font-size: 12px;">
                                    You received this email because of your alert "${alert.alert_name}" on Civant.
                                </p>
                            `;
                            
                            await civant.integrations.Core.SendEmail({
                                to: alert.user_email,
                                subject: `[Civant] New Match: ${tender.title.substring(0, 50)}...`,
                                body: emailBody
                            });
                            
                            // Update event as sent
                            await civant.asServiceRole.entities.AlertEvents.update(event.id, {
                                sent: true,
                                sent_at: new Date().toISOString()
                            });
                            
                            emailsSent++;
                        } catch (emailError) {
                            errors.push(`Email error for ${alert.user_email}: ${emailError.message}`);
                        }
                    }
                }
                
                // Update alert last_checked_at
                await civant.asServiceRole.entities.Alerts.update(alert.id, {
                    last_checked_at: new Date().toISOString()
                });
                
            } catch (alertError) {
                errors.push(`Alert ${alert.id} error: ${alertError.message}`);
            }
        }
        
        return Response.json({
            success: true,
            alerts_processed: alerts.length,
            matches_found: matchCount,
            emails_sent: emailsSent,
            errors: errors.slice(0, 10)
        });
        
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});