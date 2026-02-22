import { createClientFromRequest } from './civantSdk.ts';
import { requireAuthenticatedUser } from './requireAdmin.ts';

const getErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

Deno.serve(async (req) => {
    try {
        const civant = createClientFromRequest(req);
        const user = await requireAuthenticatedUser({ civant, req });
        const tenantId = String(user.tenantId || '').trim().toLowerCase();
        if (!tenantId) {
            return Response.json({ error: 'Missing tenant context' }, { status: 400 });
        }
        
        const { company_name } = await req.json() as { company_name?: string };
        
        if (!company_name) {
            return Response.json({ error: 'company_name is required' }, { status: 400 });
        }

        // Call the database function via Supabase REST API (PostgREST RPC)
        const supabaseUrl = Deno.env.get('SUPABASE_URL') || Deno.env.get('VITE_SUPABASE_URL');
        const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

        if (!supabaseUrl || !serviceRoleKey) {
            // Fallback: use the civant SDK to query entities directly
            return await fallbackAnalysis(civant, company_name, tenantId);
        }

        const rpcUrl = `${supabaseUrl}/rest/v1/rpc/get_competitor_intelligence`;
        const rpcResponse = await fetch(rpcUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${serviceRoleKey}`,
                'apikey': serviceRoleKey,
                'Prefer': 'return=representation'
            },
            body: JSON.stringify({
                p_tenant_id: tenantId,
                p_search_term: company_name
            })
        });

        if (!rpcResponse.ok) {
            const errorText = await rpcResponse.text();
            console.error('RPC call failed:', rpcResponse.status, errorText);
            return await fallbackAnalysis(civant, company_name, tenantId);
        }

        const result = await rpcResponse.json();

        if (!result || !result.success) {
            return Response.json({
                success: true,
                company_name,
                found_tenders: 0,
                message: result?.message || 'No awards found for this competitor',
                analysis: null
            });
        }

        // Transform database result to match the UI's expected format
        const summary = result.summary;
        const renewals = result.renewal_opportunities || [];
        const buyers = result.buyer_relationships || [];
        const categories = result.category_breakdown || [];
        const trend = result.yearly_trend || [];
        const contracts = result.recent_contracts || [];
        const countryCounts = (summary.countries || {}) as Record<string, number>;
        const activeCountries = Object.entries(countryCounts)
            .filter(([, count]) => Number(count || 0) > 0)
            .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
            .map(([country]) => country);
        const strongestRegion = activeCountries[0] || 'IE';

        // Determine overall trend from yearly data
        let overallTrend = 'stable';
        if (trend.length >= 3) {
            const recent = trend.slice(-2);
            const earlier = trend.slice(-4, -2);
            const recentAvg = recent.reduce((s: number, t: { awards: number }) => s + t.awards, 0) / recent.length;
            const earlierAvg = earlier.length > 0 
                ? earlier.reduce((s: number, t: { awards: number }) => s + t.awards, 0) / earlier.length 
                : recentAvg;
            if (recentAvg > earlierAvg * 1.2) overallTrend = 'growing';
            else if (recentAvg < earlierAvg * 0.8) overallTrend = 'declining';
        }

        return Response.json({
            success: true,
            company_name,
            found_tenders: summary.total_awards,
            // Summary stats for the header
            summary: {
                total_awards: summary.total_awards,
                total_value_eur: summary.total_value_eur,
                distinct_buyers: summary.distinct_buyers,
                active_contracts: summary.active_contracts,
                years_active: summary.years_active,
                has_frameworks: summary.has_frameworks,
                avg_contract_value_eur: summary.avg_contract_value_eur,
                max_contract_value_eur: summary.max_contract_value_eur,
                expiring_3m: summary.expiring_3m,
                expiring_6m: summary.expiring_6m,
                expiring_12m: summary.expiring_12m
            },
            // Renewal opportunities with window classification
            renewal_opportunities: renewals.map((r: Record<string, unknown>) => ({
                buyer_name: r.buyer_name,
                value_eur: r.value_eur,
                end_date: r.end_date,
                cpv_cluster: r.cpv_cluster,
                days_until_expiry: r.days_until_expiry,
                window_class: r.window_class,
                repeat_wins: r.repeat_wins,
                framework_flag: r.framework_flag,
                procedure_type: r.procedure_type,
                duration_months: r.duration_months,
                incumbent_strength: (r.repeat_wins as number) >= 3 ? 'strong_incumbent' 
                    : (r.repeat_wins as number) >= 2 ? 'moderate_incumbent' 
                    : 'low_lock_in'
            })),
            // Buyer relationships
            buyer_relationships: buyers.map((b: Record<string, unknown>) => ({
                buyer_name: b.buyer_name,
                award_count: b.award_count,
                total_value: b.total_value,
                relationship_strength: b.relationship_strength,
                active_contracts: b.active_contracts,
                first_award: b.first_award,
                last_award: b.last_award
            })),
            // Category breakdown
            category_breakdown: categories.map((c: Record<string, unknown>) => ({
                cluster: c.cluster,
                award_count: c.award_count,
                total_value: c.total_value,
                distinct_buyers: c.distinct_buyers,
                active_contracts: c.active_contracts
            })),
            // Yearly trend for chart
            yearly_trend: trend,
            // Recent contracts
            recent_contracts: contracts.map((c: Record<string, unknown>) => ({
                buyer_name: c.buyer_name,
                value_eur: c.value_eur,
                award_date: c.award_date,
                end_date: c.end_date,
                cpv_cluster: c.cpv_cluster,
                framework_flag: c.framework_flag,
                procedure_type: c.procedure_type,
                duration_months: c.duration_months
            })),
            // Overall trend
            trend: overallTrend,
            // Legacy compatibility: analysis object for existing UI
            analysis: {
                win_analysis: {
                    total_tenders: summary.total_awards,
                    estimated_wins: summary.total_awards,
                    win_rate_percentage: 100,
                    pattern: `Active in ${summary.distinct_buyers} public bodies across ${summary.years_active} years`
                },
                preferred_cpv_codes: categories.map((c: Record<string, unknown>) => ({
                    cpv_code: formatClusterName(c.cluster as string),
                    frequency: c.award_count,
                    success_rate: `${c.active_contracts} active`
                })),
                geographic_presence: {
                    primary_countries: activeCountries,
                    strongest_region: strongestRegion
                },
                value_analysis: {
                    typical_range: `€${formatValue(summary.avg_contract_value_eur)} avg`,
                    average_contract_value: summary.avg_contract_value_eur,
                    largest_win: summary.max_contract_value_eur
                },
                key_buyers: buyers.slice(0, 5).map((b: Record<string, unknown>) => ({
                    buyer_name: b.buyer_name,
                    frequency: b.award_count,
                    relationship_strength: b.relationship_strength
                })),
                strengths: generateStrengths(summary, categories, buyers),
                weaknesses: generateWeaknesses(summary, categories, renewals),
                strategic_insights: generateInsights(summary, renewals, buyers, categories),
                trend: overallTrend
            }
        });
        
    } catch (error: unknown) {
        console.error('Competitor analysis error:', error);
        return Response.json({ 
            error: getErrorMessage(error) || 'Analysis failed'
        }, { status: 500 });
    }
});

function formatClusterName(cluster: string): string {
    if (!cluster) return 'Unknown';
    return cluster
        .replace('cluster_', '')
        .split('_')
        .map(w => w.charAt(0).toUpperCase() + w.slice(1))
        .join(' ');
}

function formatValue(value: number): string {
    if (!value) return '0';
    if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
    if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
    if (value >= 1_000) return `${(value / 1_000).toFixed(0)}K`;
    return value.toString();
}

function generateStrengths(
    summary: Record<string, number>, 
    categories: Array<Record<string, unknown>>,
    buyers: Array<Record<string, unknown>>
): string[] {
    const strengths: string[] = [];
    
    if (summary.total_awards > 20) {
        strengths.push(`Extensive track record with ${summary.total_awards} public contracts over ${summary.years_active} years`);
    } else if (summary.total_awards > 5) {
        strengths.push(`Established presence with ${summary.total_awards} public contracts`);
    }
    
    if (summary.has_frameworks > 0) {
        strengths.push(`Holds ${summary.has_frameworks} framework agreements, indicating pre-qualified status`);
    }
    
    const strongBuyers = buyers.filter((b: Record<string, unknown>) => b.relationship_strength === 'strong');
    if (strongBuyers.length > 0) {
        const names = strongBuyers.slice(0, 3).map((b: Record<string, unknown>) => b.buyer_name).join(', ');
        strengths.push(`Strong repeat relationships with ${names}`);
    }
    
    if (summary.active_contracts > 3) {
        strengths.push(`Currently active on ${summary.active_contracts} contracts, showing ongoing delivery capability`);
    }
    
    if (categories.length > 2) {
        strengths.push(`Diversified across ${categories.length} procurement categories`);
    }
    
    if (summary.max_contract_value_eur > 50_000_000) {
        strengths.push(`Proven capability on large-scale contracts (up to €${formatValue(summary.max_contract_value_eur)})`);
    }
    
    return strengths;
}

function generateWeaknesses(
    summary: Record<string, number>,
    categories: Array<Record<string, unknown>>,
    renewals: Array<Record<string, unknown>>
): string[] {
    const weaknesses: string[] = [];
    
    const dominantCategory = categories[0];
    if (dominantCategory && categories.length > 1) {
        const dominantPct = ((dominantCategory.award_count as number) / summary.total_awards * 100);
        if (dominantPct > 70) {
            weaknesses.push(`Heavy concentration in ${formatClusterName(dominantCategory.cluster as string)} (${Math.round(dominantPct)}% of awards) — vulnerable to category-specific downturns`);
        }
    }
    
    if (renewals.length > 0) {
        const imminentCount = renewals.filter((r: Record<string, unknown>) => r.window_class === 'imminent').length;
        if (imminentCount > 0) {
            weaknesses.push(`${imminentCount} contract(s) expiring imminently — capacity may be stretched during re-competition`);
        }
    }
    
    if (summary.distinct_buyers < 5 && summary.total_awards > 5) {
        weaknesses.push(`Concentrated buyer base (${summary.distinct_buyers} buyers) — loss of a single relationship would be significant`);
    }
    
    return weaknesses;
}

function generateInsights(
    summary: Record<string, number>,
    renewals: Array<Record<string, unknown>>,
    buyers: Array<Record<string, unknown>>,
    categories: Array<Record<string, unknown>>
): string[] {
    const insights: string[] = [];
    
    if (renewals.length > 0) {
        const totalRenewalValue = renewals.reduce((s: number, r: Record<string, unknown>) => s + ((r.value_eur as number) || 0), 0);
        insights.push(`${renewals.length} contracts worth €${formatValue(totalRenewalValue)} expiring in next 12 months — these are competitive entry points`);
        
        const lowLockIn = renewals.filter((r: Record<string, unknown>) => (r.repeat_wins as number) <= 1);
        if (lowLockIn.length > 0) {
            insights.push(`${lowLockIn.length} expiring contract(s) have low incumbent lock-in (single award) — strongest displacement opportunities`);
        }
    }
    
    const emergingBuyers = buyers.filter((b: Record<string, unknown>) => b.relationship_strength === 'emerging' && (b.total_value as number) > 10_000_000);
    if (emergingBuyers.length > 0) {
        insights.push(`Watch for vulnerability at ${emergingBuyers.slice(0, 2).map((b: Record<string, unknown>) => b.buyer_name).join(', ')} — high-value but shallow relationships`);
    }
    
    const weakCategories = categories.filter((c: Record<string, unknown>) => (c.award_count as number) <= 2);
    if (weakCategories.length > 0) {
        insights.push(`Competitor is weakest in ${weakCategories.map((c: Record<string, unknown>) => formatClusterName(c.cluster as string)).join(', ')} — consider targeting these categories at shared buyers`);
    }
    
    return insights;
}

async function fallbackAnalysis(
    civant: ReturnType<typeof createClientFromRequest>,
    companyName: string,
    tenantId: string
) {
    const allTenders = await civant.entities.TendersCurrent.filter(
        { tenant_id: tenantId },
        '-publication_date',
        2000
    ) as Array<Record<string, unknown>>;
    
    const relatedTenders = allTenders.filter((t: Record<string, unknown>) => 
        (t.title as string)?.toLowerCase().includes(companyName.toLowerCase()) ||
        (t.buyer_name as string)?.toLowerCase().includes(companyName.toLowerCase())
    );
    
    if (relatedTenders.length === 0) {
        return Response.json({
            success: true,
            company_name: companyName,
            found_tenders: 0,
            message: 'No tenders found for this competitor (using fallback analysis)'
        });
    }
    
    return Response.json({
        success: true,
        company_name: companyName,
        found_tenders: relatedTenders.length,
        message: 'Using legacy analysis — database function not available',
        analysis: null
    });
}
