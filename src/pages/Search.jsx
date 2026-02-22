import React, { useState, useEffect, useMemo } from 'react';
import { civant } from '@/api/civantClient';
import { createPageUrl } from '../utils';
import { useLocation } from 'react-router-dom';
import { useTenant } from '@/lib/tenant';
import { useAuth } from '@/lib/auth';
import { 
    Search as SearchIcon, 
    Filter,
    X,
    Loader2
} from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { format, subDays, isAfter, addDays } from 'date-fns';
import CpvCodePicker from '@/components/CpvCodePicker';

function parseCpvFilterCodes(value) {
    const seen = new Set();
    const output = [];
    const matches = String(value || '').match(/\d{2,8}/g) || [];
    for (const raw of matches) {
        const code = raw.slice(0, 8);
        if (!code || seen.has(code)) continue;
        seen.add(code);
        output.push(code);
    }
    return output;
}

function parseTenderCpvCodes(value) {
    if (Array.isArray(value)) {
        return parseTenderCpvCodes(value.join(','));
    }

    const seen = new Set();
    const output = [];
    const parts = String(value || '')
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);
    for (const part of parts) {
        const code = part.replace(/\D/g, '').slice(0, 8);
        if (code.length < 2 || seen.has(code)) continue;
        seen.add(code);
        output.push(code);
    }
    return output;
}

function parseTenderSources(tender) {
    const seen = new Set();
    const output = [];

    const add = (value) => {
        const normalized = String(value || '').trim().toUpperCase();
        if (!normalized || seen.has(normalized)) return;
        seen.add(normalized);
        output.push(normalized);
    };

    if (Array.isArray(tender?.verification_sources)) {
        tender.verification_sources.forEach(add);
    } else if (typeof tender?.verification_sources === 'string') {
        tender.verification_sources
            .split(',')
            .map((item) => item.replace(/[{}"]/g, '').trim())
            .filter(Boolean)
            .forEach(add);
    }

    add(tender?.source);
    return output;
}

const CLOSED_STATUS_CODES = new Set(['RES', 'ADJ', 'CAN', 'DES']);

function parseTenderDeadlineDate(value) {
    const raw = String(value || '').trim();
    if (!raw) return null;

    // Date-only deadlines remain valid until the end of the day.
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
        const parsed = new Date(`${raw}T23:59:59.999Z`);
        return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    const parsed = new Date(raw);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function isClosedTenderByStatus(tender) {
    const statusCode = String(tender?.status_code || '').trim().toUpperCase();
    const status = String(tender?.status || '').trim().toLowerCase();
    const noticeType = String(tender?.notice_type || '').trim().toLowerCase();
    if (tender?.is_open === false) return true;
    if (noticeType === 'award') return true;
    if (['closed', 'cancelled', 'canceled', 'awarded', 'completed', 'unsuccessful'].includes(status)) return true;
    return CLOSED_STATUS_CODES.has(statusCode);
}

function formatDateSafe(value, pattern = 'MMM d, yyyy') {
    if (!value) return '-';
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return '-';
    return format(parsed, pattern);
}

const DEFAULT_FILTERS = Object.freeze({
    keyword: '',
    country: 'all',
    source: 'all',
    buyerSearch: '',
    cpvSearchCodes: [],
    deadlineWithin: 'all',
    industry: 'all',
    institutionType: 'all',
    lastTendered: 'all',
    openOnly: false,
    sortBy: 'relevance'
});

const DEFAULT_LOAD_LIMIT = 150;
const APPLY_LOAD_LIMIT = 250;

const SPAIN_OPEN_PRESET = Object.freeze({
    ...DEFAULT_FILTERS,
    country: 'ES',
    source: 'PLACSP_ES',
    deadlineWithin: '90',
    lastTendered: '90'
});

function normalizeFilterSnapshot(snapshot) {
    const safe = snapshot || DEFAULT_FILTERS;
    return {
        keyword: String(safe.keyword || ''),
        country: String(safe.country || 'all'),
        source: String(safe.source || 'all'),
        buyerSearch: String(safe.buyerSearch || ''),
        cpvSearchCodes: Array.isArray(safe.cpvSearchCodes) ? [...safe.cpvSearchCodes] : [],
        deadlineWithin: String(safe.deadlineWithin || 'all'),
        industry: String(safe.industry || 'all'),
        institutionType: String(safe.institutionType || 'all'),
        lastTendered: String(safe.lastTendered || 'all'),
        openOnly: Boolean(safe.openOnly),
        sortBy: String(safe.sortBy || 'relevance')
    };
}

function areFilterSnapshotsEqual(a, b) {
    const left = normalizeFilterSnapshot(a);
    const right = normalizeFilterSnapshot(b);
    return (
        left.keyword === right.keyword &&
        left.country === right.country &&
        left.source === right.source &&
        left.buyerSearch === right.buyerSearch &&
        left.deadlineWithin === right.deadlineWithin &&
        left.industry === right.industry &&
        left.institutionType === right.institutionType &&
        left.lastTendered === right.lastTendered &&
        left.openOnly === right.openOnly &&
        left.sortBy === right.sortBy &&
        [...left.cpvSearchCodes].sort().join(',') === [...right.cpvSearchCodes].sort().join(',')
    );
}

export default function Search() {
    const location = useLocation();
    const [tenders, setTenders] = useState([]);
    const [filteredTenders, setFilteredTenders] = useState([]);
    const [loading, setLoading] = useState(false);
    const [showFilters, setShowFilters] = useState(false);
    const [searchMeta, setSearchMeta] = useState(null);
    const { activeTenantId, isLoadingTenants } = useTenant();
    const { roles } = useAuth();
    
    // Filters
    const [keyword, setKeyword] = useState('');
    const [country, setCountry] = useState('all');
    const [source, setSource] = useState('all');
    const [buyerSearch, setBuyerSearch] = useState('');
    const [cpvSearchCodes, setCpvSearchCodes] = useState([]);
    const [deadlineWithin, setDeadlineWithin] = useState('all');
    const [industry, setIndustry] = useState('all');
    const [institutionType, setInstitutionType] = useState('all');
    const [lastTendered, setLastTendered] = useState('all');
    const [openOnly, setOpenOnly] = useState(false);
    const [sortBy, setSortBy] = useState('relevance');
    const [appliedFilters, setAppliedFilters] = useState(() => normalizeFilterSnapshot(DEFAULT_FILTERS));
    const [lastSearchAt, setLastSearchAt] = useState(null);
    
    useEffect(() => {
        if (isLoadingTenants) return;
        if (!activeTenantId) return;
        setLoading(true);
        void loadTenders(appliedFilters, DEFAULT_LOAD_LIMIT);
    }, [activeTenantId, isLoadingTenants]);

    useEffect(() => {
        const params = new URLSearchParams(location.search);
        const hasQueryParams = String(location.search || '').length > 1;
        const allowedCountries = new Set(['all', 'FR', 'IE', 'ES']);
        const allowedSources = new Set(['all', 'BOAMP_FR', 'TED', 'ETENDERS_IE', 'PLACSP_ES']);
        const allowedDeadlineWithin = new Set(['all', '7', '14', '30', '60', '90', '180', '365']);
        const allowedIndustry = new Set(['all', 'construction', 'it', 'health', 'transport', 'consulting', 'food']);
        const allowedInstitutionType = new Set(['all', 'ministry', 'local', 'health', 'education', 'transport']);
        const allowedLastTendered = new Set(['all', '1', '7', '30', '90', '180', '365']);
        const allowedSortBy = new Set(['relevance', 'published_desc', 'deadline_asc']);

        const readFilter = (key, fallback = 'all') => params.get(key) || fallback;

        const nextFilters = hasQueryParams ? {
            keyword: params.get('keyword') || '',
            country: (() => {
                const nextCountry = readFilter('country');
                return allowedCountries.has(nextCountry) ? nextCountry : 'all';
            })(),
            source: (() => {
                const nextSource = readFilter('source');
                return allowedSources.has(nextSource) ? nextSource : 'all';
            })(),
            buyerSearch: params.get('buyer') || '',
            cpvSearchCodes: parseCpvFilterCodes(params.get('cpv') || ''),
            deadlineWithin: (() => {
                const nextDeadlineWithin = readFilter('deadlineWithin');
                return allowedDeadlineWithin.has(nextDeadlineWithin) ? nextDeadlineWithin : 'all';
            })(),
            industry: (() => {
                const nextIndustry = readFilter('industry');
                return allowedIndustry.has(nextIndustry) ? nextIndustry : 'all';
            })(),
            institutionType: (() => {
                const nextInstitutionType = readFilter('institutionType');
                return allowedInstitutionType.has(nextInstitutionType) ? nextInstitutionType : 'all';
            })(),
            lastTendered: (() => {
                const nextLastTendered = readFilter('lastTendered');
                return allowedLastTendered.has(nextLastTendered) ? nextLastTendered : 'all';
            })(),
            openOnly: (() => {
                const raw = String(params.get('openOnly') || '').trim().toLowerCase();
                return raw === '1' || raw === 'true' || raw === 'yes';
            })(),
            sortBy: (() => {
                const nextSortBy = readFilter('sortBy', 'relevance');
                return allowedSortBy.has(nextSortBy) ? nextSortBy : 'relevance';
            })()
        } : normalizeFilterSnapshot(SPAIN_OPEN_PRESET);

        setKeyword(nextFilters.keyword);
        setCountry(nextFilters.country);
        setSource(nextFilters.source);
        setBuyerSearch(nextFilters.buyerSearch);
        setCpvSearchCodes(nextFilters.cpvSearchCodes);
        setDeadlineWithin(nextFilters.deadlineWithin);
        setIndustry(nextFilters.industry);
        setInstitutionType(nextFilters.institutionType);
        setLastTendered(nextFilters.lastTendered);
        setOpenOnly(nextFilters.openOnly);
        setSortBy(nextFilters.sortBy);
        setAppliedFilters(nextFilters);
    }, [location.search]);
    
    useEffect(() => {
        applyFilters(appliedFilters);
    }, [tenders, appliedFilters]);

    const getTenderPublicationDate = (tender) =>
        tender.publication_date || tender.published_at || tender.first_seen_at || tender.last_seen_at || tender.updated_at;
    const getTenderFirstSeen = (tender) =>
        tender.first_seen_at || tender.published_at || tender.publication_date || tender.last_seen_at || tender.updated_at;

    const loadTendersFallback = async (filters) => {
        const query = {};

        if (filters.lastTendered !== 'all') {
            const days = Number.parseInt(filters.lastTendered, 10);
            if (!Number.isNaN(days) && days > 0) {
                query.last_seen_at = { $gte: subDays(new Date(), days).toISOString() };
            }
        }

        const pageSize = 500;
        const maxRows = filters.lastTendered !== 'all' ? 6000 : 2500;
        const cutoffDate = (() => {
            const days = Number.parseInt(filters.lastTendered, 10);
            if (Number.isNaN(days) || days <= 0) return null;
            return subDays(new Date(), days);
        })();

        const rows = [];
        let skip = 0;

        while (rows.length < maxRows) {
            const batch = Object.keys(query).length > 0
                ? await civant.entities.notices_search_current.filter(query, '-last_seen_at', pageSize, skip)
                : await civant.entities.notices_search_current.list('-last_seen_at', pageSize, skip);

            if (!Array.isArray(batch) || batch.length === 0) break;

            rows.push(...batch);
            skip += batch.length;

            if (batch.length < pageSize) break;

            if (cutoffDate) {
                const oldestInBatch = batch.reduce((oldest, tender) => {
                    const dateValue = getTenderPublicationDate(tender);
                    if (!dateValue) return oldest;
                    const date = new Date(dateValue);
                    if (Number.isNaN(date.getTime())) return oldest;
                    if (!oldest) return date;
                    return date < oldest ? date : oldest;
                }, null);

                if (oldestInBatch && oldestInBatch < cutoffDate) break;
            }
        }

        return rows.slice(0, maxRows);
    };
    
    const loadTenders = async (snapshot = appliedFilters, resultLimit = DEFAULT_LOAD_LIMIT) => {
        const filters = normalizeFilterSnapshot(snapshot);

        try {
            const startedAtMs = Date.now();
            const response = await civant.functions.invoke('searchTenders', {
                ...filters,
                limit: resultLimit
            });

            const data = Array.isArray(response?.items)
                ? response.items
                : Array.isArray(response)
                    ? response
                    : [];

            setTenders(data);
            const apiMeta = response && typeof response.meta === 'object' ? response.meta : {};
            setSearchMeta({
                ...apiMeta,
                elapsed_ms: Number(apiMeta.elapsed_ms) > 0 ? Number(apiMeta.elapsed_ms) : Date.now() - startedAtMs
            });
        } catch (error) {
            console.warn('searchTenders function unavailable, using fallback search path:', error);
            try {
                const startedAtMs = Date.now();
                const fallbackData = await loadTendersFallback(filters);
                setTenders(fallbackData);
                setSearchMeta({
                    search_engine: 'entities_fallback_client',
                    returned_rows: Array.isArray(fallbackData) ? fallbackData.length : 0,
                    scanned_rows: Array.isArray(fallbackData) ? fallbackData.length : 0,
                    elapsed_ms: Date.now() - startedAtMs,
                    fallback_reason: error?.message || 'searchTenders unavailable'
                });
            } catch (fallbackError) {
                console.error('Error loading tenders:', fallbackError);
                setTenders([]);
                setSearchMeta({
                    search_engine: 'entities_fallback_client',
                    returned_rows: 0,
                    elapsed_ms: 0,
                    fallback_error: fallbackError?.message || 'fallback failed'
                });
            }
        } finally {
            setLoading(false);
        }
    };
    
    const applyFilters = (snapshot) => {
        const filters = normalizeFilterSnapshot(snapshot);
        let filtered = [...tenders];
        
        // Keyword search in title
        if (filters.keyword) {
            const kw = filters.keyword.toLowerCase();
            const hasKeywordMatch = (value) => String(value || '').toLowerCase().includes(kw);
            filtered = filtered.filter(t => 
                hasKeywordMatch(t.title) ||
                hasKeywordMatch(t.buyer_name) ||
                hasKeywordMatch(t.description)
            );
        }
        
        // Country filter
        if (filters.country !== 'all') {
            filtered = filtered.filter(t => String(t.country || '').toUpperCase() === filters.country);
        }
        
        // Source filter
        if (filters.source !== 'all') {
            const wantedSource = String(filters.source || '').trim().toUpperCase();
            filtered = filtered.filter(t => parseTenderSources(t).includes(wantedSource));
        }
        
        // Buyer search
        if (filters.buyerSearch) {
            filtered = filtered.filter(t => 
                t.buyer_name?.toLowerCase().includes(filters.buyerSearch.toLowerCase())
            );
        }
        
        // CPV search
        if (filters.cpvSearchCodes.length) {
            filtered = filtered.filter((t) => {
                const tenderCodes = parseTenderCpvCodes(t.cpv_codes);
                if (!tenderCodes.length) return false;
                return filters.cpvSearchCodes.some((wanted) =>
                    tenderCodes.some((code) => code.startsWith(wanted))
                );
            });
        }
        
        // Deadline filter
        if (filters.deadlineWithin !== 'all') {
            const days = parseInt(filters.deadlineWithin);
            const now = new Date();
            const futureDate = addDays(now, days);
            
            filtered = filtered.filter(t => {
                if (isClosedTenderByStatus(t)) return false;
                const deadline = parseTenderDeadlineDate(t.deadline_date);
                if (!deadline) return false;
                return deadline >= now && deadline <= futureDate;
            });
        }
        
        // Industry filter (based on CPV codes)
        if (filters.industry !== 'all') {
            filtered = filtered.filter(t => {
                if (!t.cpv_codes) return false;
                const cpv = t.cpv_codes.toLowerCase();
                // Map industries to CPV code prefixes
                if (filters.industry === 'construction') return cpv.includes('45');
                if (filters.industry === 'it') return cpv.includes('72') || cpv.includes('48');
                if (filters.industry === 'health') return cpv.includes('33') || cpv.includes('85');
                if (filters.industry === 'transport') return cpv.includes('60') || cpv.includes('34');
                if (filters.industry === 'consulting') return cpv.includes('79') || cpv.includes('71');
                if (filters.industry === 'food') return cpv.includes('15') || cpv.includes('55');
                return false;
            });
        }
        
        // Institution type filter
        if (filters.institutionType !== 'all') {
            filtered = filtered.filter(t => {
                if (!t.buyer_name) return false;
                const buyer = t.buyer_name.toLowerCase();
                if (filters.institutionType === 'ministry') return buyer.includes('ministry') || buyer.includes('ministère') || buyer.includes('minister');
                if (filters.institutionType === 'local') return buyer.includes('council') || buyer.includes('city') || buyer.includes('county') || buyer.includes('commune') || buyer.includes('ville');
                if (filters.institutionType === 'health') return buyer.includes('health') || buyer.includes('hospital') || buyer.includes('santé') || buyer.includes('hôpital');
                if (filters.institutionType === 'education') return buyer.includes('university') || buyer.includes('college') || buyer.includes('school') || buyer.includes('université') || buyer.includes('école');
                if (filters.institutionType === 'transport') return buyer.includes('transport') || buyer.includes('railway') || buyer.includes('road');
                return false;
            });
        }
        
        // Last tendered filter (publication date)
        if (filters.lastTendered !== 'all') {
            const days = parseInt(filters.lastTendered);
            const now = new Date();
            const cutoffDate = subDays(now, days);
            
            filtered = filtered.filter(t => {
                // For the "last 24h" quick view, match the dashboard "new tenders"
                // metric by using first-seen time.
                const dateValue = days === 1
                    ? getTenderFirstSeen(t)
                    : getTenderPublicationDate(t);
                if (!dateValue) return false;
                return new Date(dateValue) >= cutoffDate;
            });
        }

        if (filters.openOnly) {
            const now = new Date();
            filtered = filtered.filter((t) => {
                if (t?.is_open === true || String(t?.is_open || '').toLowerCase() === 'true') return true;
                if (isClosedTenderByStatus(t)) return false;
                const deadline = parseTenderDeadlineDate(t.deadline_date);
                return !deadline || deadline >= now;
            });
        }

        const ranked = [...filtered];
        ranked.sort((a, b) => {
            if (filters.sortBy === 'published_desc') {
                return new Date(getTenderPublicationDate(b) || 0).getTime() - new Date(getTenderPublicationDate(a) || 0).getTime();
            }
            if (filters.sortBy === 'deadline_asc') {
                const left = parseTenderDeadlineDate(a.deadline_date);
                const right = parseTenderDeadlineDate(b.deadline_date);
                if (!left && !right) return 0;
                if (!left) return 1;
                if (!right) return -1;
                return left.getTime() - right.getTime();
            }

            const leftScore = Number(a.relevance_score || 0);
            const rightScore = Number(b.relevance_score || 0);
            if (rightScore !== leftScore) return rightScore - leftScore;

            const leftTime = new Date(getTenderPublicationDate(a) || 0).getTime();
            const rightTime = new Date(getTenderPublicationDate(b) || 0).getTime();
            return rightTime - leftTime;
        });
        
        setFilteredTenders(ranked);
    };
    
    const clearFilters = () => {
        setKeyword(DEFAULT_FILTERS.keyword);
        setCountry(DEFAULT_FILTERS.country);
        setSource(DEFAULT_FILTERS.source);
        setBuyerSearch(DEFAULT_FILTERS.buyerSearch);
        setCpvSearchCodes(DEFAULT_FILTERS.cpvSearchCodes);
        setDeadlineWithin(DEFAULT_FILTERS.deadlineWithin);
        setIndustry(DEFAULT_FILTERS.industry);
        setInstitutionType(DEFAULT_FILTERS.institutionType);
        setLastTendered(DEFAULT_FILTERS.lastTendered);
        setOpenOnly(DEFAULT_FILTERS.openOnly);
        setSortBy(DEFAULT_FILTERS.sortBy);
        setAppliedFilters(normalizeFilterSnapshot(DEFAULT_FILTERS));
    };
    
    const currentFilters = useMemo(() => ({
        keyword,
        country,
        source,
        buyerSearch,
        cpvSearchCodes,
        deadlineWithin,
        industry,
        institutionType,
        lastTendered,
        openOnly,
        sortBy
    }), [keyword, country, source, buyerSearch, cpvSearchCodes, deadlineWithin, industry, institutionType, lastTendered, openOnly, sortBy]);

    const hasPendingFilterChanges = !areFilterSnapshotsEqual(currentFilters, appliedFilters);
    const hasActiveFilters = !areFilterSnapshotsEqual(appliedFilters, DEFAULT_FILTERS);
    const canViewSearchDiagnostics = Array.isArray(roles) && (roles.includes('admin') || roles.includes('creator'));
    const tedOnlyResults = filteredTenders.length > 0 && filteredTenders.every((tender) => tender.coverage_status === 'ted_only');

    const applySearch = () => {
        const nextFilters = normalizeFilterSnapshot(currentFilters);
        setLoading(true);
        setAppliedFilters(nextFilters);
        setLastSearchAt(new Date());
        void loadTenders(nextFilters, APPLY_LOAD_LIMIT);
    };
    
    const getSourceBadge = (source) => {
        const colors = {
            'BOAMP_FR': 'bg-blue-500/15 text-blue-200 border-blue-400/40',
            'TED': 'bg-violet-500/15 text-violet-200 border-violet-400/40',
            'ETENDERS_IE': 'bg-emerald-500/15 text-emerald-200 border-emerald-400/40',
            'PLACSP_ES': 'bg-amber-500/15 text-amber-200 border-amber-400/40'
        };
        return colors[source] || 'bg-slate-900/60 text-slate-300 border-slate-700';
    };

    const getCoverageBadge = (coverageStatus) => {
        if (coverageStatus === 'linked') return 'bg-civant-teal/15 text-civant-teal border-civant-teal/40';
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
    
    return (
        <div className="space-y-6">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-bold text-slate-100">Search Tenders</h1>
                <p className="text-slate-400 mt-1">Find procurement opportunities matching your criteria</p>
            </div>
            
            {/* Search Bar */}
            <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                <CardContent className="p-4">
                    <div className="flex flex-col sm:flex-row gap-3">
                        <div className="relative flex-1">
                            <SearchIcon className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
                            <Input
                                placeholder="Search by keyword or buyer..."
                                value={keyword}
                                onChange={(e) => setKeyword(e.target.value)}
                                onKeyDown={(e) => {
                                    if (e.key === 'Enter') {
                                        e.preventDefault();
                                        applySearch();
                                    }
                                }}
                                className="pl-10 bg-slate-900/60 border-0"
                            />
                        </div>
                        <Button
                            type="button"
                            onClick={applySearch}
                            className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90"
                        >
                            <SearchIcon className="h-4 w-4 mr-2" />
                            Search
                        </Button>
                        <Button
                            type="button"
                            variant="outline"
                            onClick={() => setShowFilters(!showFilters)}
                            className={showFilters ? 'bg-civant-teal/20 border-civant-teal/40 text-civant-teal' : ''}
                        >
                            <Filter className="h-4 w-4 mr-2" />
                            Filters
                        </Button>
                    </div>
                    {hasPendingFilterChanges ? (
                        <p className="mt-2 text-xs text-amber-300">You changed filters. Click Search to refresh results.</p>
                    ) : null}
                    
                    {/* Expanded Filters */}
                    {showFilters && (
                        <div className="mt-4 pt-4 border-t border-slate-800">
                            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                                <div>
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">Country</label>
                                    <Select value={country} onValueChange={setCountry}>
                                        <SelectTrigger className="bg-slate-900/60 border-0">
                                            <SelectValue placeholder="All countries" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">All Countries</SelectItem>
                                            <SelectItem value="FR">🇫🇷 France</SelectItem>
                                            <SelectItem value="IE">🇮🇪 Ireland</SelectItem>
                                            <SelectItem value="ES">🇪🇸 Spain</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                                
                                <div>
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">Source</label>
                                    <Select value={source} onValueChange={setSource}>
                                        <SelectTrigger className="bg-slate-900/60 border-0">
                                            <SelectValue placeholder="All sources" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">All Sources</SelectItem>
                                            <SelectItem value="BOAMP_FR">BOAMP FR</SelectItem>
                                            <SelectItem value="TED">TED EU</SelectItem>
                                            <SelectItem value="ETENDERS_IE">eTenders IE</SelectItem>
                                            <SelectItem value="PLACSP_ES">PLACSP ES</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                                
                                <div>
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">Buyer contains</label>
                                    <Input
                                        placeholder="e.g. Ministry"
                                        value={buyerSearch}
                                        onChange={(e) => setBuyerSearch(e.target.value)}
                                        className="bg-slate-900/60 border-0"
                                    />
                                </div>
                                
                                <div className="sm:col-span-2 lg:col-span-2">
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">CPV code</label>
                                    <CpvCodePicker
                                        value={cpvSearchCodes}
                                        onChange={setCpvSearchCodes}
                                        placeholder="Search CPV by code or keyword"
                                        maxSelections={8}
                                        className="text-sm"
                                    />
                                </div>
                                
                                <div>
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">Deadline within</label>
                                    <Select value={deadlineWithin} onValueChange={setDeadlineWithin}>
                                        <SelectTrigger className="bg-slate-900/60 border-0">
                                            <SelectValue placeholder="Any time" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">Any time</SelectItem>
                                            <SelectItem value="7">7 days</SelectItem>
                                            <SelectItem value="14">14 days</SelectItem>
                                            <SelectItem value="30">30 days</SelectItem>
                                            <SelectItem value="60">60 days</SelectItem>
                                            <SelectItem value="90">3 months</SelectItem>
                                            <SelectItem value="180">6 months</SelectItem>
                                            <SelectItem value="365">1 year</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>

                                <div>
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">Industry</label>
                                    <Select value={industry} onValueChange={setIndustry}>
                                        <SelectTrigger className="bg-slate-900/60 border-0">
                                            <SelectValue placeholder="All industries" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">All Industries</SelectItem>
                                            <SelectItem value="construction">Construction</SelectItem>
                                            <SelectItem value="it">IT & Software</SelectItem>
                                            <SelectItem value="health">Healthcare</SelectItem>
                                            <SelectItem value="transport">Transport</SelectItem>
                                            <SelectItem value="consulting">Consulting</SelectItem>
                                            <SelectItem value="food">Food & Catering</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>

                                <div>
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">Institution Type</label>
                                    <Select value={institutionType} onValueChange={setInstitutionType}>
                                        <SelectTrigger className="bg-slate-900/60 border-0">
                                            <SelectValue placeholder="All types" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">All Types</SelectItem>
                                            <SelectItem value="ministry">Ministry/Govt</SelectItem>
                                            <SelectItem value="local">Local Authority</SelectItem>
                                            <SelectItem value="health">Health Service</SelectItem>
                                            <SelectItem value="education">Education</SelectItem>
                                            <SelectItem value="transport">Transport</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>

                                <div>
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">Published within</label>
                                    <Select value={lastTendered} onValueChange={setLastTendered}>
                                        <SelectTrigger className="bg-slate-900/60 border-0">
                                            <SelectValue placeholder="Any time" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">Any time</SelectItem>
                                            <SelectItem value="1">Last 24 hours</SelectItem>
                                            <SelectItem value="7">Last 7 days</SelectItem>
                                            <SelectItem value="30">Last 30 days</SelectItem>
                                            <SelectItem value="90">Last 3 months</SelectItem>
                                            <SelectItem value="180">Last 6 months</SelectItem>
                                            <SelectItem value="365">Last year</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>

                                <div>
                                    <label className="text-xs font-medium text-slate-400 mb-1.5 block">Sort</label>
                                    <Select value={sortBy} onValueChange={setSortBy}>
                                        <SelectTrigger className="bg-slate-900/60 border-0">
                                            <SelectValue />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="relevance">Relevance</SelectItem>
                                            <SelectItem value="published_desc">Published (newest)</SelectItem>
                                            <SelectItem value="deadline_asc">Deadline (soonest)</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>

                                <div className="flex items-center justify-between rounded-md bg-slate-900/60 px-3 py-2">
                                    <div>
                                        <p className="text-xs font-medium text-slate-400">Open only</p>
                                        <p className="text-[11px] text-slate-500">Hide closed/expired tenders</p>
                                    </div>
                                    <Switch checked={openOnly} onCheckedChange={setOpenOnly} />
                                </div>
                                </div>
                            
                            {(hasActiveFilters || hasPendingFilterChanges) && (
                                <div className="mt-4 flex flex-wrap items-center justify-end gap-2">
                                    <Button
                                        type="button"
                                        variant="outline"
                                        size="sm"
                                        onClick={applySearch}
                                        className="border-civant-teal/40 text-civant-teal hover:bg-civant-teal/10"
                                    >
                                        Search
                                    </Button>
                                    <Button type="button" variant="ghost" size="sm" onClick={clearFilters}>
                                        <X className="h-4 w-4 mr-1" />
                                        Clear all filters
                                    </Button>
                                </div>
                            )}
                        </div>
                    )}
                </CardContent>
            </Card>
            
            {/* Results count */}
            <div className="flex items-center justify-between">
                <p className="text-sm text-slate-400">
                    Showing <span className="font-medium text-slate-100">{filteredTenders.length}</span> tenders
                </p>
                <div className="flex items-center gap-3">
                    {loading ? (
                        <p className="text-xs text-civant-teal flex items-center gap-1.5">
                            <Loader2 className="h-3.5 w-3.5 animate-spin" />
                            Refreshing...
                        </p>
                    ) : null}
                    {lastSearchAt ? (
                        <p className="text-xs text-slate-500">
                            Search run at {format(lastSearchAt, 'HH:mm:ss')}
                        </p>
                    ) : null}
                </div>
            </div>

            {tedOnlyResults ? (
                <Card className="border border-violet-400/30 bg-violet-950/20">
                    <CardContent className="p-3 text-sm text-violet-100">
                        Showing TED-only results. These opportunities are sourced from TED and may not yet have a linked national notice.
                    </CardContent>
                </Card>
            ) : null}

            {canViewSearchDiagnostics && searchMeta ? (
                <Card className="border border-civant-border bg-civant-navy/45 shadow-none">
                    <CardContent className="p-3">
                        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 text-xs">
                            <div>
                                <p className="text-slate-500">Engine</p>
                                <p className="text-slate-200 font-medium">{String(searchMeta.search_engine || 'unknown')}</p>
                            </div>
                            <div>
                                <p className="text-slate-500">Returned</p>
                                <p className="text-slate-200 font-medium">{Number(searchMeta.returned_rows ?? filteredTenders.length ?? 0)}</p>
                            </div>
                            <div>
                                <p className="text-slate-500">Scanned</p>
                                <p className="text-slate-200 font-medium">{Number(searchMeta.scanned_rows || 0)}</p>
                            </div>
                            <div>
                                <p className="text-slate-500">Candidates</p>
                                <p className="text-slate-200 font-medium">{Number(searchMeta.candidate_rows || 0)}</p>
                            </div>
                            <div>
                                <p className="text-slate-500">Latency</p>
                                <p className="text-slate-200 font-medium">{Number(searchMeta.elapsed_ms || 0)} ms</p>
                            </div>
                            <div>
                                <p className="text-slate-500">Zero-result log</p>
                                <p className="text-slate-200 font-medium">
                                    {searchMeta.zero_result_logged === true ? 'logged' : Number(searchMeta.returned_rows ?? 0) === 0 ? 'pending/failed' : 'n/a'}
                                </p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            ) : null}
            
            {/* Results Table */}
            <Card className="border border-civant-border bg-civant-navy/55 shadow-none overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full">
                        <thead>
                            <tr className="bg-slate-900/60 border-b border-slate-800">
                                <th className="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">
                                    Tender
                                </th>
                                <th className="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3 hidden md:table-cell">
                                    Buyer
                                </th>
                                <th className="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3 hidden lg:table-cell">
                                    Publication
                                </th>
                                <th className="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3">
                                    Deadline
                                </th>
                                <th className="text-left text-xs font-semibold text-slate-400 uppercase tracking-wider px-4 py-3 hidden sm:table-cell">
                                    Source
                                </th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800">
                            {loading && filteredTenders.length === 0 ? (
                                <tr>
                                    <td colSpan={5} className="px-4 py-12 text-center text-slate-300">
                                        <Loader2 className="h-8 w-8 mx-auto mb-2 animate-spin text-civant-teal" />
                                        <p>Loading tenders...</p>
                                    </td>
                                </tr>
                            ) : filteredTenders.length === 0 ? (
                                <tr>
                                    <td colSpan={5} className="px-4 py-12 text-center text-slate-400">
                                        <SearchIcon className="h-8 w-8 mx-auto mb-2 text-slate-300" />
                                        <p>No tenders found matching your criteria</p>
                                        {appliedFilters.openOnly ? (
                                            <p className="text-xs mt-2">
                                                Try turning off <span className="font-medium">Open only</span> to include closed historical tenders.
                                            </p>
                                        ) : null}
                                        {(hasActiveFilters || hasPendingFilterChanges) ? (
                                            <p className="text-xs mt-2">
                                                Adjust filters or use Clear all filters to broaden results.
                                            </p>
                                        ) : null}
                                    </td>
                                </tr>
                            ) : (
                                filteredTenders.map(tender => {
                                    const firstSeenAt = getTenderFirstSeen(tender);
                                    const isNew = firstSeenAt && isAfter(new Date(firstSeenAt), subDays(new Date(), 1));
                                    const isUpdated = tender.version_count > 1;
                                    
                                    return (
                                        <tr 
                                            key={tender.id} 
                                            className="hover:bg-slate-900/60 transition-colors cursor-pointer"
                                            onClick={() => window.location.href = createPageUrl(`TenderDetail?id=${tender.id}`)}
                                        >
                                            <td className="px-4 py-4">
                                                <div className="flex items-start gap-3">
                                                    <span className="text-lg">{getCountryFlag(tender.country)}</span>
                                                    <div className="min-w-0">
                                                        <div className="flex items-center gap-2 flex-wrap">
                                                            <p className="font-medium text-slate-100 line-clamp-2">
                                                                {tender.title}
                                                            </p>
                                                        </div>
                                                        <div className="flex items-center gap-2 mt-1">
                                                            {isNew && (
                                                                <Badge className="bg-emerald-500/15 text-emerald-200 border-emerald-400/40 text-xs">
                                                                    New
                                                                </Badge>
                                                            )}
                                                            {isUpdated && (
                                                                <Badge className="bg-amber-500/15 text-amber-200 border-amber-400/40 text-xs">
                                                                    Updated
                                                                </Badge>
                                                            )}
                                                        </div>
                                                    </div>
                                                </div>
                                            </td>
                                            <td className="px-4 py-4 hidden md:table-cell">
                                                <p className="text-sm text-slate-300 line-clamp-2">
                                                    {tender.buyer_name || '-'}
                                                </p>
                                            </td>
                                            <td className="px-4 py-4 hidden lg:table-cell">
                                                <p className="text-sm text-slate-300">
                                                    {formatDateSafe(getTenderPublicationDate(tender))}
                                                </p>
                                            </td>
                                            <td className="px-4 py-4">
                                                <p className="text-sm text-slate-300">
                                                    {formatDateSafe(tender.deadline_date)}
                                                </p>
                                            </td>
                                            <td className="px-4 py-4 hidden sm:table-cell">
                                                <div className="flex flex-wrap gap-1.5">
                                                    <Badge className={`${getSourceBadge(tender.source)} border text-xs`}>
                                                        {tender.source}
                                                    </Badge>
                                                    {tender.coverage_status ? (
                                                        <Badge className={`${getCoverageBadge(tender.coverage_status)} border text-xs`}>
                                                            {tender.coverage_status.replace('_', ' ')}
                                                        </Badge>
                                                    ) : null}
                                                    {tender.verification_level ? (
                                                        <Badge className={`${getVerificationBadge(tender.verification_level)} border text-xs`}>
                                                            {tender.verification_level.replace('_', ' ')}
                                                        </Badge>
                                                    ) : null}
                                                </div>
                                            </td>
                                        </tr>
                                    );
                                })
                            )}
                        </tbody>
                    </table>
                </div>
            </Card>
        </div>
    );
}
