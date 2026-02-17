import React, { useMemo, useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useTenant } from '@/lib/tenant';
import {
    ArrowLeft,
    ExternalLink,
    FileText,
    Link2,
    Bell,
    Loader2,
    MessageSquare,
    CalendarPlus,
    Sparkles,
    Copy,
    Check,
    MoreHorizontal
} from 'lucide-react';
import EnrichmentPanel from '../components/tender/EnrichmentPanel';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger
} from '@/components/ui/dropdown-menu';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { format, formatDistanceToNowStrict } from 'date-fns';

const COVERAGE_LABELS = {
    linked: 'Linked',
    ted_only: 'TED only',
    national_only: 'National'
};

const VERIFICATION_LABELS = {
    verified: 'Verified',
    partially_verified: 'Partially verified',
    unverified: 'Unverified'
};

function KeyFactItem({ label, value, helper = null }) {
    return (
        <div className="min-w-0">
            <p className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</p>
            <div className="mt-1 text-sm font-medium text-card-foreground truncate">{value}</div>
            {helper ? <p className="mt-1 text-xs text-primary truncate">{helper}</p> : null}
        </div>
    );
}

function DetailRow({ label, value, mono = false }) {
    return (
        <div className="flex items-start justify-between gap-4 py-3 border-b border-border/40 last:border-b-0">
            <p className="text-xs uppercase tracking-wide text-muted-foreground">{label}</p>
            <div className={`text-sm text-card-foreground text-right ${mono ? 'font-mono' : 'font-medium'}`}>{value}</div>
        </div>
    );
}

export default function TenderDetail() {
    const [tender, setTender] = useState(null);
    const [linkedNotices, setLinkedNotices] = useState([]);
    const [linkedNoticeCount, setLinkedNoticeCount] = useState(0);
    const [evidenceOpen, setEvidenceOpen] = useState(true);
    const [loading, setLoading] = useState(true);
    const [integrationLoading, setIntegrationLoading] = useState(null);
    const [enrichment, setEnrichment] = useState(null);
    const [enriching, setEnriching] = useState(false);
    const [canonicalCopied, setCanonicalCopied] = useState(false);
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

    const canonicalId = useMemo(() => String(tender?.canonical_id || tender?.id || ''), [tender]);

    const effectiveCountry = useMemo(() => {
        const explicit = String(tender?.country || '').trim().toUpperCase();
        if (explicit) return explicit;
        const source = String(tender?.source || '').trim().toUpperCase();
        if (source.endsWith('_IE')) return 'IE';
        if (source.endsWith('_FR')) return 'FR';
        if (source.endsWith('_ES')) return 'ES';
        return '';
    }, [tender]);

    const coverageLabel = useMemo(() => {
        const key = String(tender?.coverage_status || 'national_only').toLowerCase();
        return COVERAGE_LABELS[key] || key.replace('_', ' ');
    }, [tender]);

    const verificationLabel = useMemo(() => {
        const key = String(tender?.verification_level || 'unverified').toLowerCase();
        return VERIFICATION_LABELS[key] || key.replace('_', ' ');
    }, [tender]);

    const cpvCodes = useMemo(() => {
        if (!tender?.cpv_codes) return [];
        if (Array.isArray(tender.cpv_codes)) {
            return tender.cpv_codes.map((v) => String(v || '').trim()).filter(Boolean);
        }
        return String(tender.cpv_codes)
            .split(',')
            .map((code) => code.trim())
            .filter(Boolean);
    }, [tender]);

    const tenderReference = useMemo(() => {
        const explicit = tender?.reference || tender?.tender_reference || tender?.reference_no || tender?.source_notice_id;
        if (explicit) return String(explicit).trim();

        const title = String(tender?.title || '').trim();
        const match = title.match(/^([A-Za-z]{2,6}\s*\d{1,8})\s*[-–:]/);
        if (match?.[1]) return match[1].replace(/\s+/g, ' ').toUpperCase();

        const cftMatch = title.match(/^(CFT\s*\d{1,8})\b/i);
        if (cftMatch?.[1]) return cftMatch[1].replace(/\s+/g, ' ').toUpperCase();

        return 'Reference not provided';
    }, [tender]);

    const publishedDate = useMemo(() => {
        if (!tender?.publication_date) return null;
        const parsed = new Date(tender.publication_date);
        return Number.isNaN(parsed.getTime()) ? null : parsed;
    }, [tender]);

    const deadlineDate = useMemo(() => {
        if (!tender?.deadline_date) return null;
        const parsed = new Date(tender.deadline_date);
        return Number.isNaN(parsed.getTime()) ? null : parsed;
    }, [tender]);

    const deadlineHelper = useMemo(() => {
        if (!deadlineDate) return null;
        const now = new Date();
        if (deadlineDate > now) {
            return `Closes in ${formatDistanceToNowStrict(deadlineDate)}`;
        }
        return `Closed ${formatDistanceToNowStrict(deadlineDate, { addSuffix: true })}`;
    }, [deadlineDate]);

    const formattedEstimatedValue = useMemo(() => {
        if (!tender?.estimated_value) return 'Not specified';
        try {
            return new Intl.NumberFormat('en', {
                style: 'currency',
                currency: tender.currency || 'EUR',
                maximumFractionDigits: 0
            }).format(Number(tender.estimated_value));
        } catch {
            return String(tender.estimated_value);
        }
    }, [tender]);

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

                const canonicalIdValue = data[0].canonical_id || data[0].id;
                const links = await civant.entities.canonical_notice_links.filter(
                    { canonical_id: canonicalIdValue },
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

                const enrichmentData = await civant.entities.TenderEnrichment.filter({
                    tender_uid: canonicalIdValue
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
            BOAMP_FR: 'bg-secondary/70 text-secondary-foreground border-border/80',
            TED: 'bg-primary/15 text-primary border-primary/30',
            ETENDERS_IE: 'bg-primary/12 text-card-foreground border-border/80',
            PLACSP_ES: 'bg-primary/10 text-card-foreground border-border/80'
        };
        return colors[source] || 'bg-secondary/60 text-secondary-foreground border-border/70';
    };

    const getCoverageBadge = (coverageStatus) => {
        if (coverageStatus === 'linked') return 'bg-emerald-500/12 text-emerald-200 border-emerald-400/35';
        if (coverageStatus === 'ted_only') return 'bg-violet-500/12 text-violet-200 border-violet-400/35';
        return 'bg-slate-900/50 text-slate-300 border-slate-700';
    };

    const getVerificationBadge = (verificationLevel) => {
        if (verificationLevel === 'verified') return 'bg-emerald-500/12 text-emerald-200 border-emerald-400/35';
        if (verificationLevel === 'partially_verified') return 'bg-amber-500/12 text-amber-200 border-amber-400/35';
        return 'bg-slate-900/50 text-slate-300 border-slate-700';
    };

    const getCountryFlag = (country) => {
        return country === 'FR' ? '🇫🇷' : country === 'IE' ? '🇮🇪' : country === 'ES' ? '🇪🇸' : '🌍';
    };

    const handleAddToCalendar = async () => {
        if (!actionableTenderId) return;
        setIntegrationLoading('calendar');
        try {
            const response = await civant.functions.invoke('addToCalendar', { tender_id: actionableTenderId });
            if (response.data.success) {
                alert('Tender deadline added to your Google Calendar!');
            } else {
                alert(`Failed to add to calendar: ${response.data.error}`);
            }
        } catch (error) {
            alert(`Error: ${error.message}\n\nPlease ensure Google Calendar integration is authorized by your admin.`);
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
                alert(`Enrichment already exists or failed: ${response.data.message || response.data.error}`);
            }
        } catch (error) {
            alert(`Error: ${error.message}`);
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
                alert('Tender posted to Slack channel!');
            } else {
                alert(`Failed to send to Slack: ${response.data.error}`);
            }
        } catch (error) {
            alert(`Error: ${error.message}\n\nPlease ensure Slack integration is authorized by your admin.`);
        } finally {
            setIntegrationLoading(null);
        }
    };

    const handleCopyCanonicalId = async () => {
        if (!canonicalId) return;
        try {
            await navigator.clipboard.writeText(canonicalId);
            setCanonicalCopied(true);
            setTimeout(() => setCanonicalCopied(false), 1400);
        } catch (error) {
            console.warn('Clipboard copy failed', error);
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
        <div className="space-y-8 max-w-6xl mx-auto">
            <header className="space-y-6">
                <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
                    <Link
                        to={createPageUrl('Search')}
                        className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-card-foreground transition-colors"
                    >
                        <ArrowLeft className="h-4 w-4" />
                        Back to Search
                    </Link>

                    <div className="flex flex-wrap items-center gap-2 xl:justify-end">
                        <Button asChild variant="primary" className="min-w-[150px]">
                            <Link to={createPageUrl(`Alerts?buyer=${encodeURIComponent(tender.buyer_name || '')}&keyword=${encodeURIComponent(tender.title?.split(' ').slice(0, 3).join(' ') || '')}`)}>
                                <Bell className="h-4 w-4" />
                                Create Alert
                            </Link>
                        </Button>

                        {tender.url ? (
                            <Button asChild variant="outline">
                                <a href={tender.url} target="_blank" rel="noopener noreferrer">
                                    <ExternalLink className="h-4 w-4" />
                                    View Original
                                </a>
                            </Button>
                        ) : null}

                        <Button
                            variant="outline"
                            onClick={handleEnrichTender}
                            disabled={enriching || enrichment || !actionableTenderId}
                        >
                            {enriching ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                                <Sparkles className="h-4 w-4" />
                            )}
                            {enrichment ? 'Enriched' : 'AI Enrich'}
                        </Button>

                        {tender.deadline_date ? (
                            <Button
                                variant="outline"
                                onClick={handleAddToCalendar}
                                disabled={integrationLoading === 'calendar' || !actionableTenderId}
                            >
                                {integrationLoading === 'calendar' ? (
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                ) : (
                                    <CalendarPlus className="h-4 w-4" />
                                )}
                                Add to Calendar
                            </Button>
                        ) : null}

                        <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                                <Button variant="outline" size="icon" aria-label="More actions">
                                    <MoreHorizontal className="h-4 w-4" />
                                </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end" className="w-52">
                                <DropdownMenuLabel>More actions</DropdownMenuLabel>
                                <DropdownMenuSeparator />
                                <DropdownMenuItem
                                    onSelect={(event) => {
                                        event.preventDefault();
                                        handleSendToSlack();
                                    }}
                                    disabled={integrationLoading === 'slack' || !actionableTenderId}
                                >
                                    {integrationLoading === 'slack' ? (
                                        <Loader2 className="h-4 w-4 animate-spin" />
                                    ) : (
                                        <MessageSquare className="h-4 w-4" />
                                    )}
                                    Send to Slack
                                </DropdownMenuItem>
                            </DropdownMenuContent>
                        </DropdownMenu>
                    </div>
                </div>

                <div className="space-y-2 min-w-0">
                    <p className="text-sm text-muted-foreground">{tenderReference}</p>
                    <h1
                        className="text-3xl font-bold text-card-foreground leading-tight"
                        style={{ display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}
                    >
                        {tender.title}
                    </h1>
                    {tender.coverage_status === 'ted_only' ? (
                        <p className="text-sm text-violet-300">
                            TED-only notice. Civant will keep monitoring national sources to link verification evidence.
                        </p>
                    ) : null}
                </div>

                <div className="rounded-2xl border border-border/70 bg-card/40 px-4 py-4">
                    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
                        <KeyFactItem
                            label="Published"
                            value={publishedDate ? format(publishedDate, 'MMM d, yyyy') : 'Not specified'}
                        />
                        <KeyFactItem
                            label="Deadline"
                            value={deadlineDate ? format(deadlineDate, 'MMM d, yyyy') : 'Not specified'}
                            helper={deadlineHelper}
                        />
                        <KeyFactItem label="Estimated value" value={formattedEstimatedValue} />
                        <div className="min-w-0">
                            <p className="text-[11px] uppercase tracking-wide text-muted-foreground">CPV codes</p>
                            <div className="mt-1 flex flex-wrap items-center gap-1.5">
                                {cpvCodes.length === 0 ? (
                                    <span className="text-sm font-medium text-card-foreground">Not specified</span>
                                ) : (
                                    <>
                                        {cpvCodes.slice(0, 2).map((code) => (
                                            <Badge key={code} variant="ghost" className="text-xs h-6 px-2 border border-border/60 bg-background/40">
                                                {code}
                                            </Badge>
                                        ))}
                                        {cpvCodes.length > 2 ? (
                                            <Popover>
                                                <PopoverTrigger asChild>
                                                    <Button variant="ghost" size="sm" className="h-6 px-2 text-xs border border-border/60 bg-background/40">
                                                        +{cpvCodes.length - 2}
                                                    </Button>
                                                </PopoverTrigger>
                                                <PopoverContent align="start" className="w-56">
                                                    <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-2">All CPV codes</p>
                                                    <div className="flex flex-wrap gap-1.5">
                                                        {cpvCodes.map((code) => (
                                                            <Badge key={code} variant="secondary" className="text-xs">{code}</Badge>
                                                        ))}
                                                    </div>
                                                </PopoverContent>
                                            </Popover>
                                        ) : null}
                                    </>
                                )}
                            </div>
                        </div>
                        <div className="min-w-0">
                            <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Status</p>
                            <div className="mt-1 flex flex-wrap items-center gap-1.5">
                                <Badge className={`${getSourceBadge(tender.source)} border h-6 px-2 text-[10px] font-medium`}>
                                    <span className="mr-1">{getCountryFlag(effectiveCountry)}</span>
                                    {tender.source}
                                </Badge>
                                <Badge className={`${getCoverageBadge(tender.coverage_status)} border h-6 px-2 text-[10px] font-medium`}>
                                    {coverageLabel}
                                </Badge>
                                <Badge className={`${getVerificationBadge(tender.verification_level)} border h-6 px-2 text-[10px] font-medium`}>
                                    {verificationLabel}
                                </Badge>
                            </div>
                        </div>
                    </div>
                </div>

                <div className="h-px bg-gradient-to-r from-transparent via-border/80 to-transparent" />
            </header>

            {/* AI Enrichment Panel */}
            {enrichment ? (
                <Card>
                    <CardContent className="p-6">
                        <EnrichmentPanel enrichment={enrichment} />
                    </CardContent>
                </Card>
            ) : null}

            {/* Main Info Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="text-lg font-semibold">Details</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <DetailRow label="Buyer" value={tender.buyer_name || 'Not specified'} />
                        <DetailRow label="Country" value={effectiveCountry || 'Not specified'} />
                        <DetailRow label="Source" value={String(tender.source || 'Not specified')} />
                        <DetailRow label="Reference" value={tenderReference} />
                        <DetailRow label="Tender status" value={tender.is_open ? 'Open' : 'Closed'} />
                        <DetailRow label="Notice links visible" value={`${linkedNotices.length}/${linkedNoticeCount}`} />
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="text-lg font-semibold">Data Integrity</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <DetailRow
                            label="Canonical ID"
                            mono
                            value={
                                <div className="inline-flex items-center justify-end gap-1.5">
                                    <span>{canonicalId || '-'}</span>
                                    <Button
                                        type="button"
                                        variant="ghost"
                                        size="icon"
                                        className="h-6 w-6 -mr-1"
                                        onClick={handleCopyCanonicalId}
                                        disabled={!canonicalId}
                                        aria-label="Copy canonical ID"
                                    >
                                        {canonicalCopied ? (
                                            <Check className="h-3.5 w-3.5 text-emerald-300" />
                                        ) : (
                                            <Copy className="h-3.5 w-3.5" />
                                        )}
                                    </Button>
                                </div>
                            }
                        />
                        <DetailRow label="Coverage" value={coverageLabel} />
                        <DetailRow label="Verification" value={verificationLabel} />
                        <DetailRow
                            label="First seen"
                            value={tender.first_seen_at ? format(new Date(tender.first_seen_at), 'MMM d, yyyy HH:mm') : '-'}
                        />
                        <DetailRow
                            label="Last seen"
                            value={tender.last_seen_at ? format(new Date(tender.last_seen_at), 'MMM d, yyyy HH:mm') : '-'}
                        />
                        <DetailRow label="Notice links" value={String(tender.notice_count ?? linkedNoticeCount ?? 0)} />
                        {Array.isArray(tender.ted_notice_ids) && tender.ted_notice_ids.length > 0 ? (
                            <DetailRow label="TED Notice IDs" value={tender.ted_notice_ids.join(', ')} mono />
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
                                <p className="text-muted-foreground text-center py-6">
                                    No linked source notices yet. Civant continuously monitors for updates.
                                </p>
                            )
                        ) : (
                            <div className="space-y-4">
                                {linkedNotices.map((notice) => (
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
                                                    : ''}
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
                                ))}
                            </div>
                        )}
                    </CardContent>
                ) : null}
            </Card>
        </div>
    );
}
