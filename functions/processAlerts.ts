import { createClientFromRequest } from './civantSdk.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);
const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;

function normalizeTenantId(value: unknown) {
    const normalized = String(value || '').trim().toLowerCase();
    return TENANT_ID_PATTERN.test(normalized) ? normalized : '';
}

type AlertRecord = {
    id: string;
    tenant_id?: string;
    alert_name?: string;
    user_email?: string;
    country?: string;
    keywords?: string;
    buyer_contains?: string;
    cpv_contains?: string;
    deadline_within_days?: number;
    active?: boolean;
};

type TenderRecord = {
    id: string;
    tenant_id?: string;
    tender_uid?: string;
    title?: string;
    buyer_name?: string;
    country?: string;
    publication_date?: string;
    deadline_date?: string;
    estimated_value?: number;
    cpv_codes?: string;
    url?: string;
    first_seen_at?: string;
};

// Process alerts and send email notifications for matching tenders
Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await civant.auth.me();
        
        if (!user || user.role !== 'admin') {
            return Response.json({ error: 'Admin access required' }, { status: 403 });
        }
        
        const tenants = await civant.asServiceRole.entities.tenants.list('created_at', 500) as Array<{ id?: string }>;

        let alertsProcessed = 0;
        let matchCount = 0;
        let emailsSent = 0;
        const errors: string[] = [];

        for (const tenant of Array.isArray(tenants) ? tenants : []) {
            const tenantId = normalizeTenantId(tenant?.id);
            if (!tenantId) continue;

            // Get tenant-scoped alerts and tenders only.
            const alerts = await civant.asServiceRole.entities.Alerts.filter({
                tenant_id: tenantId,
                active: true
            }) as AlertRecord[];

            const allTenders = await civant.asServiceRole.entities.TendersCurrent.filter({
                tenant_id: tenantId
            }, '-first_seen_at', 500) as TenderRecord[];

            const yesterday = new Date();
            yesterday.setDate(yesterday.getDate() - 1);
            const recentTenders = allTenders.filter((t: TenderRecord) =>
                t.first_seen_at && new Date(t.first_seen_at) >= yesterday
            );

            for (const alert of alerts) {
                try {
                    const alertTenantId = normalizeTenantId(alert.tenant_id);
                    if (!alertTenantId || alertTenantId !== tenantId) {
                        throw new Error(`Alert tenant mismatch for alert ${alert.id}`);
                    }
                    alertsProcessed++;

                    // Find matching tenders
                    const matches = recentTenders.filter((tender: TenderRecord) => {
                        // Country filter
                        if (alert.country && tender.country !== alert.country) {
                            return false;
                        }

                        // Keywords filter
                        if (alert.keywords) {
                            const keywords = alert.keywords.toLowerCase().split(',').map((k: string) => k.trim());
                            const title = (tender.title || '').toLowerCase();
                            const buyer = (tender.buyer_name || '').toLowerCase();
                            const hasKeyword = keywords.some((kw: string) =>
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
                            const daysUntilDeadline = Math.ceil((deadline.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
                            if (daysUntilDeadline < 0 || daysUntilDeadline > alert.deadline_within_days) {
                                return false;
                            }
                        }

                        return true;
                    });

                    // Check for existing alert events to avoid duplicates
                    for (const tender of matches) {
                        const tenderTenantId = normalizeTenantId(tender.tenant_id);
                        if (!tenderTenantId || tenderTenantId !== alertTenantId) {
                            throw new Error(`Tender tenant mismatch before email send (alert=${alert.id}, tender=${tender.tender_uid || tender.id})`);
                        }

                        const existingEvents = await civant.asServiceRole.entities.AlertEvents.filter({
                            tenant_id: alertTenantId,
                            alert_id: alert.id,
                            tender_uid: tender.tender_uid
                        });

                        if (existingEvents.length === 0) {
                            // Create alert event
                            const event = await civant.asServiceRole.entities.AlertEvents.create({
                                tenant_id: alertTenantId,
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
                                    subject: `[Civant] New Match: ${String(tender.title || 'Tender').substring(0, 50)}...`,
                                    body: emailBody
                                });

                                // Update event as sent
                                await civant.asServiceRole.entities.AlertEvents.update(event.id, {
                                    sent: true,
                                    sent_at: new Date().toISOString()
                                });

                                emailsSent++;
                            } catch (emailError: unknown) {
                                errors.push(`Email error for ${alert.user_email}: ${getErrorMessage(emailError)}`);
                            }
                        }
                    }

                    // Update alert last_checked_at
                    await civant.asServiceRole.entities.Alerts.update(alert.id, {
                        last_checked_at: new Date().toISOString()
                    });

                } catch (alertError: unknown) {
                    errors.push(`Alert ${alert.id} error: ${getErrorMessage(alertError)}`);
                }
            }
        }
        
        return Response.json({
            success: true,
            alerts_processed: alertsProcessed,
            matches_found: matchCount,
            emails_sent: emailsSent,
            errors: errors.slice(0, 10)
        });
        
    } catch (error: unknown) {
        return Response.json({ error: getErrorMessage(error) }, { status: 500 });
    }
});
