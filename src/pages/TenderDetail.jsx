import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { 
    ArrowLeft, 
    ExternalLink, 
    Calendar,
    Building2,
    Clock,
    Tag,
    DollarSign,
    FileText,
    History,
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
    const [versions, setVersions] = useState([]);
    const [loading, setLoading] = useState(true);
    const [integrationLoading, setIntegrationLoading] = useState(null);
    const [enrichment, setEnrichment] = useState(null);
    const [enriching, setEnriching] = useState(false);
    
    const urlParams = new URLSearchParams(window.location.search);
    const tenderId = urlParams.get('id');
    
    useEffect(() => {
        if (tenderId) {
            loadTender();
        }
    }, [tenderId]);
    
    const loadTender = async () => {
        try {
            const data = await civant.entities.TendersCurrent.filter({ id: tenderId });
            if (data.length > 0) {
                setTender(data[0]);
                
                // Load versions
                const versionData = await civant.entities.TenderVersions.filter({
                    tender_uid: data[0].tender_uid
                });
                setVersions(versionData.sort((a, b) => b.version_number - a.version_number));

                // Load enrichment data
                const enrichmentData = await civant.entities.TenderEnrichment.filter({
                    tender_uid: data[0].tender_uid
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
            'ETENDERS_IE': 'bg-primary/15 text-card-foreground border-border'
        };
        return colors[source] || 'bg-secondary text-secondary-foreground border-border';
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
        setIntegrationLoading('calendar');
        try {
            const response = await civant.functions.invoke('addToCalendar', { tender_id: tender.id });
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
        setEnriching(true);
        try {
            const response = await civant.functions.invoke('enrichTender', { tender_id: tender.id });
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
        
        setIntegrationLoading('slack');
        try {
            const response = await civant.functions.invoke('sendToSlack', { 
                tender_id: tender.id,
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
    
    const getChangeTypeBadge = (type) => {
        const labels = {
            'new_notice': { label: 'New Notice', color: 'bg-primary/20 text-primary border-primary/30' },
            'field_changed': { label: 'Fields Changed', color: 'bg-secondary text-secondary-foreground border-border' },
            'deadline_changed': { label: 'Deadline Changed', color: 'bg-primary/15 text-card-foreground border-border' },
            'corrected': { label: 'Correction', color: 'bg-secondary text-secondary-foreground border-border' },
            'award_published': { label: 'Award Published', color: 'bg-primary/20 text-primary border-primary/30' },
            'unknown': { label: 'Unknown', color: 'bg-secondary text-secondary-foreground border-border' }
        };
        return labels[type] || labels.unknown;
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
                        {enrichment?.confidence_score != null && (
                            <Badge variant="primary">
                                AI confidence {Math.round(Number(enrichment.confidence_score) * 100)}%
                            </Badge>
                        )}
                    </div>
                    <h1 className="text-2xl font-bold text-card-foreground">{tender.title}</h1>
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
                        disabled={enriching || enrichment}
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
                            disabled={integrationLoading === 'calendar'}
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
                        disabled={integrationLoading === 'slack'}
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
                            <p className="text-xs text-muted-foreground uppercase tracking-wide">Tender UID</p>
                            <Input value={tender.tender_uid || ''} readOnly className="mt-1 font-mono text-xs" />
                        </div>
                        
                        <div>
                            <p className="text-xs text-muted-foreground uppercase tracking-wide">Source Notice ID</p>
                            <Input value={tender.source_notice_id || ''} readOnly className="mt-1 font-mono text-xs" />
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
                            <p className="text-xs text-muted-foreground uppercase tracking-wide">Fingerprint</p>
                            <Input value={tender.fingerprint || ''} readOnly className="mt-1 font-mono text-xs" />
                        </div>
                    </CardContent>
                </Card>
            </div>
            
            {/* Version History */}
            <Card>
                <CardHeader className="pb-3">
                    <CardTitle className="text-lg font-semibold flex items-center gap-2">
                        <History className="h-5 w-5" />
                        Version History
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    {versions.length === 0 ? (
                        <p className="text-muted-foreground text-center py-6">No version history available</p>
                    ) : (
                        <div className="space-y-4">
                            {versions.map((version) => {
                                const changeInfo = getChangeTypeBadge(version.change_type);
                                
                                return (
                                    <div 
                                        key={version.id} 
                                        className="relative pl-6 pb-4 border-l-2 border-border last:border-l-transparent last:pb-0"
                                    >
                                        <div className="absolute left-0 top-0 transform -translate-x-1/2 w-3 h-3 rounded-full bg-background border-2 border-primary" />
                                        
                                        <div className="flex flex-col sm:flex-row sm:items-center gap-2 mb-2">
                                            <Badge className={`${changeInfo.color} w-fit`}>
                                                {changeInfo.label}
                                            </Badge>
                                            <span className="text-sm text-muted-foreground">
                                                Version {version.version_number}
                                            </span>
                                            <span className="text-sm text-muted-foreground">
                                                {version.change_date 
                                                    ? format(new Date(version.change_date), 'MMM d, yyyy HH:mm')
                                                    : ''
                                                }
                                            </span>
                                        </div>
                                        
                                        {version.change_type !== 'new_notice' && version.old_value && version.new_value && (
                                            <div className="mt-2 p-3 bg-muted/40 border border-border rounded-lg text-xs">
                                                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                                                    <div>
                                                        <p className="text-muted-foreground mb-1 font-medium">Before</p>
                                                        <pre className="text-card-foreground whitespace-pre-wrap overflow-x-auto">
                                                            {version.old_value}
                                                        </pre>
                                                    </div>
                                                    <div>
                                                        <p className="text-muted-foreground mb-1 font-medium">After</p>
                                                        <pre className="text-card-foreground whitespace-pre-wrap overflow-x-auto">
                                                            {version.new_value}
                                                        </pre>
                                                    </div>
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </CardContent>
            </Card>
        </div>
    );
}
