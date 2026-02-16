import React, { useMemo, useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useTenant } from '@/lib/tenant';
import { 
    ArrowLeft, 
    ExternalLink, 
    Calendar,
    Building2,
    Clock,
    Tag,
    DollarSign,
    FileText,
    Link2,
    Bell,
    Loader2,
    MessageSquare,
    CalendarPlus,
    Sparkles
} from 'lucide-react';
import EnrichmentPanel from '../components/tender/EnrichmentPanel';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { format, formatDistanceToNow } from 'date-fns';

export default function TenderDetail() {
    const [tender, setTender] = useState(null);
    const [linkedNotices, setLinkedNotices] = useState([]);
    const [linkedNoticeCount, setLinkedNoticeCount] = useState(0);
    const [evidenceOpen, setEvidenceOpen] = useState(true);
    const [loading, setLoading] = useState(true);
    const [integrationLoading, setIntegrationLoading] = useState(null);
    const [enrichment, setEnrichment] = useState(null);
    const [enriching, setEnriching] = useState(false);
    const { activeTenantId, isLoadingTenants } = useTenant();
    
    const urlParams = new URLSearchParams(window.location.search);
    const tenderId = urlParams.get('id');
    const actionableTenderId = useMemo(() => {
        if (!tender) return null;

        if (Array.isArray(linkedNotices) && linkedNotices.length > 0) {
            const preferred = linkedNotices.find((item) => item?.source && item?.source !== 'TED') || linkedNotices[0];
            const source = String(preferred?.source || '').trim();
            const sourceNoticeId = String(preferred?.source_notice_id || '').trim();
            if (source && sourceNoticeId) {
                return `${source}:${sourceNoticeId}`;
            }
        }

        return tender.id || tender.canonical_id || null;
    }, [linkedNotices, tender]);
    
    useEffect(() => {
        if (isLoadingTenants) return;
        if (!activeTenantId) return;
        if (tenderId) {
            loadTender();
        }
    }, [tenderId, activeTenantId, isLoadingTenants]);
    
    const loadTender = async () => {
        try {
            const data = await civant.entities.canonical_tenders.filter({ id: tenderId });
            if (data.length > 0) {
                setTender(data[0]);

                const canonicalId = data[0].canonical_id || data[0].id;
                const links = await civant.entities.canonical_notice_links.filter(
                    { canonical_id: canonicalId },
                    '-linked_at',
                    250
                );
                setLinkedNoticeCount(Array.isArray(links) ? links.length : 0);
                const noticeIds = links
                    .map((item) => item.notice_id)
                    .filter((id) => typeof id === 'string' && id.length > 0);

                let noticeRows = [];
                if (noticeIds.length > 0) {
                    noticeRows = await civant.entities.notices.filter(
                        { notice_id: { $in: noticeIds } },
                        '-publication_date',
                        Math.min(noticeIds.length, 250)
                    );
                }

                const linkByNoticeId = new Map(links.map((item) => [item.notice_id, item]));
                const mergedNotices = noticeRows
                    .map((notice) => ({
                        ...notice,
                        link_tier: linkByNoticeId.get(notice.notice_id)?.link_tier || null,
                        match_score: linkByNoticeId.get(notice.notice_id)?.match_score || null,
                        linked_at: linkByNoticeId.get(notice.notice_id)?.linked_at || null
                    }))
                    .sort((a, b) => {
                        const left = new Date(a.publication_date || a.ingested_at || 0).getTime();
                        const right = new Date(b.publication_date || b.ingested_at || 0).getTime();
                        return right - left;
                    });
                setLinkedNotices(mergedNotices);

                // Load enrichment data
                const enrichmentData = await civant.entities.TenderEnrichment.filter({
                    tender_uid: canonicalId
                });
                if (enrichmentData.length > 0) {
                    setEnrichment(enrichmentData[0]);
                }
            }
        } catch (error) {
            console.error('Error loading tender:', error);
        } finally {
            setLoading(false);
        }
    };
    
    const getSourceBadge = (source) => {
        const colors = {
            'BOAMP_FR': 'bg-secondary text-secondary-foreground border-border',
            'TED': 'bg-primary/20 text-primary border-primary/30',
            'ETENDERS_IE': 'bg-primary/15 text-card-foreground border-border',
            'PLACSP_ES': 'bg-primary/10 text-card-foreground border-border'
        };
        return colors[source] || 'bg-secondary text-secondary-foreground border-border';
    };

    const getCoverageBadge = (coverageStatus) => {
        if (coverageStatus === 'linked') return 'bg-emerald-500/15 text-emerald-200 border-emerald-400/40';
        if (coverageStatus === 'ted_only') return 'bg-violet-500/15 text-violet-200 border-violet-400/40';
        return 'bg-slate-900/60 text-slate-300 border-slate-700';
    };

    const getVerificationBadge = (verificationLevel) => {
        if (verificationLevel === 'verified') return 'bg-emerald-500/15 text-emerald-200 border-emerald-400/40';
        if (verificationLevel === 'partially_verified') return 'bg-amber-500/15 text-amber-200 border-amber-400/40';
        return 'bg-slate-900/60 text-slate-300 border-slate-700';
    };
    
    const getCountryFlag = (country) => {
        return country === 'FR' ? '🇫🇷' : country === 'IE' ? '🇮🇪' : country === 'ES' ? '🇪🇸' : '🌍';
    };
    
    const getNoticeTypeBadge = (type) => {
        const colors = {
            'tender': 'bg-primary/20 text-primary border-primary/30',
            'award': 'bg-primary/15 text-card-foreground border-border',
            'corrigendum': 'bg-secondary text-secondary-foreground border-border',
            'unknown': 'bg-secondary text-secondary-foreground border-border'
        };
        return colors[type] || colors.unknown;
    };
    
    const handleAddToCalendar = async () => {
        if (!actionableTenderId) return;
        setIntegrationLoading('calendar');
        try {
            const response = await civant.functions.invoke('addToCalendar', { tender_id: actionableTenderId });
            if (response.data.success) {
                alert('Tender deadline added to your Google Calendar!');
            } else {
                alert('Failed to add to calendar: ' + response.data.error);
            }
        } catch (error) {
            alert('Error: ' + error.message + '\n\nPlease ensure Google Calendar integration is authorized by your admin.');
        } finally {
            setIntegrationLoading(null);
        }
    };
    
    const handleEnrichTender = async () => {
        if (!actionableTenderId) return;
        setEnriching(true);
        try {
            const response = await civant.functions.invoke('enrichTender', { tender_id: actionableTenderId });
            if (response.data.success) {
                setEnrichment(response.data.enrichment);
                alert('Tender enriched successfully with AI insights!');
            } else {
                alert('Enrichment already exists or failed: ' + (response.data.message || response.data.error));
            }
        } catch (error) {
            alert('Error: ' + error.message);
        } finally {
            setEnriching(false);
        }
    };
    
    const handleSendToSlack = async () => {
        const channel = prompt('Enter Slack channel (e.g., #tenders or leave empty for #general):');
        if (channel === null) return;
        if (!actionableTenderId) return;
        
        setIntegrationLoading('slack');
        try {
            const response = await civant.functions.invoke('sendToSlack', { 
                tender_id: actionableTenderId,
                channel: channel || '#general'
            });
            if (response.data.success) {
                alert(`Tender posted to Slack channel!`);
            } else {
                alert('Failed to send to Slack: ' + response.data.error);
            }
        } catch (error) {
            alert('Error: ' + error.message + '\n\nPlease ensure Slack integration is authorized by your admin.');
        } finally {
            setIntegrationLoading(null);
        }
    };
    
    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-indigo-600" />
            </div>
        );
    }
    
    if (!tender) {
        return (
            <div className="text-center py-12">
                <FileText className="h-12 w-12 mx-auto text-slate-300 mb-4" />
                <h2 className="text-xl font-semibold text-card-foreground">Tender not found</h2>
                <p className="text-muted-foreground mt-2">The requested tender could not be found.</p>
                <Link to={createPageUrl('Search')}>
                    <Button className="mt-4">
                        <ArrowLeft className="h-4 w-4 mr-2" />
                        Back to Search
                    </Button>
                </Link>
            </div>
        );
    }
    
    return (
        <div className="space-y-6 max-w-4xl mx-auto">
            {/* Back button */}
            <Link to={createPageUrl('Search')}>
                <Button variant="ghost" size="sm">
                    <ArrowLeft className="h-4 w-4 mr-2" />
                    Back to Search
                </Button>
            </Link>
            
            {/* Header */}
            <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-4">
                <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                        <span className="text-2xl">{getCountryFlag(tender.country)}</span>
                        <Badge className={`${getSourceBadge(tender.source)} border`}>
                            {tender.source}
                        </Badge>
                        <Badge className={`${getNoticeTypeBadge(tender.notice_type)} border`}>
                            {tender.notice_type}
                        </Badge>
                        {tender.version_count > 1 && (
                            <Badge variant="secondary">
                                {tender.version_count} versions
                            </Badge>
                        )}
                        {tender.coverage_status ? (
                            <Badge className={`${getCoverageBadge(tender.coverage_status)} border`}>
                                {String(tender.coverage_status).replace('_', ' ')}
                            </Badge>
                        ) : null}
                        {tender.verification_level ? (
                            <Badge className={`${getVerificationBadge(tender.verification_level)} border`}>
                                {String(tender.verification_level).replace('_', ' ')}
                            </Badge>
                        ) : null}
                        {enrichment?.confidence_score != null && (
                            <Badge variant="primary">
                                AI confidence {Math.round(Number(enrichment.confidence_score) * 100)}%
                            </Badge>
                        )}
                    </div>
                    <h1 className="text-2xl font-bold text-card-foreground">{tender.title}</h1>
                    {tender.coverage_status === 'ted_only' ? (
                        <p className="text-sm text-violet-300 mt-2">
                            TED-only coverage: this tender is currently verified from TED and may not yet have a linked national notice.
                        </p>
                    ) : null}
                </div>
                
                <div className="flex flex-wrap gap-2">
                    {tender.url && (
                        <a href={tender.url} target="_blank" rel="noopener noreferrer">
                            <Button variant="outline">
                                <ExternalLink className="h-4 w-4 mr-2" />
                                View Original
                            </Button>
                        </a>
                    )}
                    <Button 
                        variant="outline"
                        onClick={handleEnrichTender}
                        disabled={enriching || enrichment || !actionableTenderId}
                    >
                        {enriching ? (
                            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                        ) : (
                            <Sparkles className="h-4 w-4 mr-2" />
                        )}
                        {enrichment ? 'Enriched' : 'AI Enrich'}
                    </Button>
                    {tender.deadline_date && (
                        <Button 
                            variant="outline"
                            onClick={handleAddToCalendar}
                            disabled={integrationLoading === 'calendar' || !actionableTenderId}
                        >
                            {integrationLoading === 'calendar' ? (
                                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                            ) : (
                                <CalendarPlus className="h-4 w-4 mr-2" />
                            )}
                            Add to Calendar
                        </Button>
                    )}
                    <Button 
                        variant="outline"
                        onClick={handleSendToSlack}
                        disabled={integrationLoading === 'slack' || !actionableTenderId}
                    >
                        {integrationLoading === 'slack' ? (
                            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                        ) : (
                            <MessageSquare className="h-4 w-4 mr-2" />
                        )}
                        Send to Slack
                    </Button>
                    <Link to={createPageUrl(`Alerts?buyer=${encodeURIComponent(tender.buyer_name || '')}&keyword=${encodeURIComponent(tender.title?.split(' ').slice(0, 3).join(' ') || '')}`)}>
                        <Button variant="primary">
                            <Bell className="h-4 w-4 mr-2" />
                            Create Alert
                        </Button>
                    </Link>
                </div>
            </div>
            
            {/* AI Enrichment Panel */}
            {enrichment && (
                <Card>
                    <CardContent className="p-6">
                        <EnrichmentPanel enrichment={enrichment} />
                    </CardContent>
                </Card>
            )}

            {/* Main Info Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Details Card */}
                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="text-lg font-semibold">Details</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="flex items-start gap-3">
                            <Building2 className="h-5 w-5 text-muted-foreground mt-0.5" />
                            <div>
                                <p className="text-xs text-muted-foreground uppercase tracking-wide">Buyer</p>
                                <p className="font-medium text-card-foreground">{tender.buyer_name || 'Not specified'}</p>
                            </div>
                        </div>
                        
                        <div className="flex items-start gap-3">
                            <Calendar className="h-5 w-5 text-muted-foreground mt-0.5" />
                            <div>
                                <p className="text-xs text-muted-foreground uppercase tracking-wide">Publication Date</p>
                                <p className="font-medium text-card-foreground">
                                    {tender.publication_date 
                                        ? format(new Date(tender.publication_date), 'MMMM d, yyyy')
                                        : 'Not specified'
                                    }
                                </p>
                            </div>
                        </div>
                        
                        <div className="flex items-start gap-3">
                            <Clock className="h-5 w-5 text-muted-foreground mt-0.5" />
                            <div>
                                <p className="text-xs text-muted-foreground uppercase tracking-wide">Deadline</p>
                                <p className="font-medium text-card-foreground">
                                    {tender.deadline_date 
                                        ? format(new Date(tender.deadline_date), 'MMMM d, yyyy')
                                        : 'Not specified'
                                    }
                                </p>
                                {tender.deadline_date && new Date(tender.deadline_date) > new Date() && (
                                    <p className="text-sm text-primary">
                                        {formatDistanceToNow(new Date(tender.deadline_date), { addSuffix: true })}
                                    </p>
                                )}
                            </div>
                        </div>
                        
                        {tender.estimated_value && (
                            <div className="flex items-start gap-3">
                                <DollarSign className="h-5 w-5 text-muted-foreground mt-0.5" />
                                <div>
                                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Estimated Value</p>
                                    <p className="font-medium text-card-foreground">
                                        {new Intl.NumberFormat('en', { 
                                            style: 'currency', 
                                            currency: tender.currency || 'EUR',
                                            maximumFractionDigits: 0
                                        }).format(tender.estimated_value)}
                                    </p>
                                </div>
                            </div>
                        )}
                        
                        {tender.cpv_codes && (
                            <div className="flex items-start gap-3">
                                <Tag className="h-5 w-5 text-muted-foreground mt-0.5" />
                                <div>
                                    <p className="text-xs text-muted-foreground uppercase tracking-wide">CPV Codes</p>
                                    <div className="flex flex-wrap gap-1 mt-1">
                                        {tender.cpv_codes.split(',').map((code, i) => (
                                            <Badge key={i} variant="ghost" className="text-xs">
                                                {code.trim()}
                                            </Badge>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        )}
                    </CardContent>
                </Card>
                
                {/* Metadata Card */}
                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="text-lg font-semibold">Tracking Info</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div>
                            <p className="text-xs text-muted-foreground uppercase tracking-wide">Canonical ID</p>
                            <Input value={tender.canonical_id || tender.id || ''} readOnly className="mt-1 font-mono text-xs" />
                        </div>
                        
                        <div>
                            <p className="text-xs text-muted-foreground uppercase tracking-wide">Coverage</p>
                            <Input value={tender.coverage_status || 'national_only'} readOnly className="mt-1 font-mono text-xs" />
                        </div>
                        
                        <div>
                            <p className="text-xs text-muted-foreground uppercase tracking-wide">First Seen</p>
                            <p className="text-sm text-card-foreground mt-1">
                                {tender.first_seen_at 
                                    ? format(new Date(tender.first_seen_at), 'MMM d, yyyy HH:mm')
                                    : '-'
                                }
                            </p>
                        </div>
                        
                        <div>
                            <p className="text-xs text-muted-foreground uppercase tracking-wide">Last Seen</p>
                            <p className="text-sm text-card-foreground mt-1">
                                {tender.last_seen_at 
                                    ? format(new Date(tender.last_seen_at), 'MMM d, yyyy HH:mm')
                                    : '-'
                                }
                            </p>
                        </div>
                        
                        <div>
                            <p className="text-xs text-muted-foreground uppercase tracking-wide">Verification</p>
                            <Input value={tender.verification_level || 'unverified'} readOnly className="mt-1 font-mono text-xs" />
                        </div>

                        {Array.isArray(tender.ted_notice_ids) && tender.ted_notice_ids.length > 0 ? (
                            <div>
                                <p className="text-xs text-muted-foreground uppercase tracking-wide">TED Notice IDs</p>
                                <p className="text-sm text-card-foreground mt-1 break-all">
                                    {tender.ted_notice_ids.join(', ')}
                                </p>
                            </div>
                        ) : null}
                    </CardContent>
                </Card>
            </div>
            
            {/* Sources */}
            <Card>
                <CardHeader className="pb-3">
                    <button
                        type="button"
                        className="w-full text-left"
                        onClick={() => setEvidenceOpen((prev) => !prev)}
                    >
                        <CardTitle className="text-lg font-semibold flex items-center justify-between gap-2">
                            <span className="flex items-center gap-2">
                                <Link2 className="h-5 w-5" />
                                Evidence / Sources
                            </span>
                            <span className="text-xs text-muted-foreground">
                                {linkedNotices.length}/{linkedNoticeCount} visible
                            </span>
                        </CardTitle>
                    </button>
                </CardHeader>
                {evidenceOpen ? (
                    <CardContent>
                        {linkedNotices.length === 0 ? (
                            linkedNoticeCount > 0 ? (
                                <div className="text-center py-6">
                                    <p className="text-muted-foreground">Linked notices exist but are not visible in this session.</p>
                                    <p className="text-xs text-muted-foreground mt-1">
                                        This can happen due to tenant permissions (RLS) or restricted source visibility.
                                    </p>
                                </div>
                            ) : (
                                <p className="text-muted-foreground text-center py-6">No linked source notices available</p>
                            )
                        ) : (
                            <div className="space-y-4">
                                {linkedNotices.map((notice) => {
                                    return (
                                        <div
                                            key={notice.notice_id}
                                            className="relative pl-6 pb-4 border-l-2 border-border last:border-l-transparent last:pb-0"
                                        >
                                            <div className="absolute left-0 top-0 transform -translate-x-1/2 w-3 h-3 rounded-full bg-background border-2 border-primary" />

                                            <div className="flex flex-col sm:flex-row sm:items-center gap-2 mb-2">
                                                <Badge variant="secondary" className="w-fit">
                                                    {notice.source || 'UNKNOWN'}
                                                </Badge>
                                                <span className="text-sm text-muted-foreground">
                                                    source id: {notice.source_notice_id || 'n/a'}
                                                </span>
                                                <span className="text-sm text-muted-foreground">
                                                    notice id: {notice.notice_id || 'n/a'}
                                                </span>
                                                {notice.link_tier ? (
                                                    <span className="text-sm text-muted-foreground">
                                                        tier: {notice.link_tier}
                                                    </span>
                                                ) : null}
                                                {notice.match_score != null ? (
                                                    <span className="text-sm text-muted-foreground">
                                                        score: {Number(notice.match_score).toFixed(2)}
                                                    </span>
                                                ) : null}
                                                <span className="text-sm text-muted-foreground">
                                                    {notice.publication_date
                                                        ? format(new Date(notice.publication_date), 'MMM d, yyyy')
                                                        : notice.ingested_at
                                                        ? format(new Date(notice.ingested_at), 'MMM d, yyyy HH:mm')
                                                        : ''
                                                    }
                                                </span>
                                            </div>

                                            <p className="text-sm text-card-foreground">
                                                {notice.title || 'No title provided'}
                                            </p>
                                            {notice.source_url ? (
                                                <a
                                                    href={notice.source_url}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="inline-flex items-center text-sm text-primary mt-2 hover:underline"
                                                >
                                                    View source notice
                                                    <ExternalLink className="h-3.5 w-3.5 ml-1" />
                                                </a>
                                            ) : null}
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                    </CardContent>
                ) : null}
            </Card>
        </div>
    );
}
