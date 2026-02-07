import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { createPageUrl } from '../utils';
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
import { format, subDays, isAfter, addDays } from 'date-fns';

export default function Search() {
    const [tenders, setTenders] = useState([]);
    const [filteredTenders, setFilteredTenders] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showFilters, setShowFilters] = useState(false);
    
    // Filters
    const [keyword, setKeyword] = useState('');
    const [country, setCountry] = useState('all');
    const [source, setSource] = useState('all');
    const [buyerSearch, setBuyerSearch] = useState('');
    const [cpvSearch, setCpvSearch] = useState('');
    const [deadlineWithin, setDeadlineWithin] = useState('all');
    const [industry, setIndustry] = useState('all');
    const [institutionType, setInstitutionType] = useState('all');
    const [lastTendered, setLastTendered] = useState('all');
    
    useEffect(() => {
        loadTenders();
    }, []);
    
    useEffect(() => {
        applyFilters();
    }, [tenders, keyword, country, source, buyerSearch, cpvSearch, deadlineWithin, industry, institutionType, lastTendered]);
    
    const loadTenders = async () => {
        try {
            const data = await civant.entities.TendersCurrent.list('-publication_date', 500);
            setTenders(data);
        } catch (error) {
            console.error('Error loading tenders:', error);
        } finally {
            setLoading(false);
        }
    };
    
    const applyFilters = () => {
        let filtered = [...tenders];
        
        // Keyword search in title
        if (keyword) {
            const kw = keyword.toLowerCase();
            filtered = filtered.filter(t => 
                t.title?.toLowerCase().includes(kw) ||
                t.buyer_name?.toLowerCase().includes(kw)
            );
        }
        
        // Country filter
        if (country !== 'all') {
            filtered = filtered.filter(t => t.country === country);
        }
        
        // Source filter
        if (source !== 'all') {
            filtered = filtered.filter(t => t.source === source);
        }
        
        // Buyer search
        if (buyerSearch) {
            filtered = filtered.filter(t => 
                t.buyer_name?.toLowerCase().includes(buyerSearch.toLowerCase())
            );
        }
        
        // CPV search
        if (cpvSearch) {
            filtered = filtered.filter(t => 
                t.cpv_codes?.includes(cpvSearch)
            );
        }
        
        // Deadline filter
        if (deadlineWithin !== 'all') {
            const days = parseInt(deadlineWithin);
            const now = new Date();
            const futureDate = addDays(now, days);
            
            filtered = filtered.filter(t => {
                if (!t.deadline_date) return false;
                const deadline = new Date(t.deadline_date);
                return deadline >= now && deadline <= futureDate;
            });
        }
        
        // Industry filter (based on CPV codes)
        if (industry !== 'all') {
            filtered = filtered.filter(t => {
                if (!t.cpv_codes) return false;
                const cpv = t.cpv_codes.toLowerCase();
                // Map industries to CPV code prefixes
                if (industry === 'construction') return cpv.includes('45');
                if (industry === 'it') return cpv.includes('72') || cpv.includes('48');
                if (industry === 'health') return cpv.includes('33') || cpv.includes('85');
                if (industry === 'transport') return cpv.includes('60') || cpv.includes('34');
                if (industry === 'consulting') return cpv.includes('79') || cpv.includes('71');
                if (industry === 'food') return cpv.includes('15') || cpv.includes('55');
                return false;
            });
        }
        
        // Institution type filter
        if (institutionType !== 'all') {
            filtered = filtered.filter(t => {
                if (!t.buyer_name) return false;
                const buyer = t.buyer_name.toLowerCase();
                if (institutionType === 'ministry') return buyer.includes('ministry') || buyer.includes('ministère') || buyer.includes('minister');
                if (institutionType === 'local') return buyer.includes('council') || buyer.includes('city') || buyer.includes('county') || buyer.includes('commune') || buyer.includes('ville');
                if (institutionType === 'health') return buyer.includes('health') || buyer.includes('hospital') || buyer.includes('santé') || buyer.includes('hôpital');
                if (institutionType === 'education') return buyer.includes('university') || buyer.includes('college') || buyer.includes('school') || buyer.includes('université') || buyer.includes('école');
                if (institutionType === 'transport') return buyer.includes('transport') || buyer.includes('railway') || buyer.includes('road');
                return false;
            });
        }
        
        // Last tendered filter (publication date)
        if (lastTendered !== 'all') {
            const days = parseInt(lastTendered);
            const now = new Date();
            const cutoffDate = subDays(now, days);
            
            filtered = filtered.filter(t => {
                if (!t.publication_date) return false;
                const pubDate = new Date(t.publication_date);
                return pubDate >= cutoffDate;
            });
        }
        
        setFilteredTenders(filtered);
    };
    
    const clearFilters = () => {
        setKeyword('');
        setCountry('all');
        setSource('all');
        setBuyerSearch('');
        setCpvSearch('');
        setDeadlineWithin('all');
        setIndustry('all');
        setInstitutionType('all');
        setLastTendered('all');
    };
    
    const hasActiveFilters = keyword || country !== 'all' || source !== 'all' || 
        buyerSearch || cpvSearch || deadlineWithin !== 'all' || industry !== 'all' || 
        institutionType !== 'all' || lastTendered !== 'all';
    
    const getSourceBadge = (source) => {
        const colors = {
            'BOAMP_FR': 'bg-blue-50 text-blue-700 border-blue-200',
            'TED': 'bg-purple-50 text-purple-700 border-purple-200',
            'ETENDERS_IE': 'bg-emerald-50 text-emerald-700 border-emerald-200'
        };
        return colors[source] || 'bg-slate-50 text-slate-700 border-slate-200';
    };
    
    const getCountryFlag = (country) => {
        return country === 'FR' ? '🇫🇷' : country === 'IE' ? '🇮🇪' : '🌍';
    };
    
    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-indigo-600" />
            </div>
        );
    }
    
    return (
        <div className="space-y-6">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-bold text-slate-900">Search Tenders</h1>
                <p className="text-slate-500 mt-1">Find procurement opportunities matching your criteria</p>
            </div>
            
            {/* Search Bar */}
            <Card className="border-0 shadow-sm">
                <CardContent className="p-4">
                    <div className="flex flex-col sm:flex-row gap-3">
                        <div className="relative flex-1">
                            <SearchIcon className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
                            <Input
                                placeholder="Search by keyword or buyer..."
                                value={keyword}
                                onChange={(e) => setKeyword(e.target.value)}
                                className="pl-10 bg-slate-50 border-0"
                            />
                        </div>
                        <Button
                            variant="outline"
                            onClick={() => setShowFilters(!showFilters)}
                            className={showFilters ? 'bg-indigo-50 border-indigo-200 text-indigo-700' : ''}
                        >
                            <Filter className="h-4 w-4 mr-2" />
                            Filters
                            {hasActiveFilters && (
                                <span className="ml-2 w-5 h-5 rounded-full bg-indigo-600 text-white text-xs flex items-center justify-center">
                                    !
                                </span>
                            )}
                        </Button>
                    </div>
                    
                    {/* Expanded Filters */}
                    {showFilters && (
                        <div className="mt-4 pt-4 border-t border-slate-100">
                            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                                <div>
                                    <label className="text-xs font-medium text-slate-500 mb-1.5 block">Country</label>
                                    <Select value={country} onValueChange={setCountry}>
                                        <SelectTrigger className="bg-slate-50 border-0">
                                            <SelectValue placeholder="All countries" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">All Countries</SelectItem>
                                            <SelectItem value="FR">🇫🇷 France</SelectItem>
                                            <SelectItem value="IE">🇮🇪 Ireland</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                                
                                <div>
                                    <label className="text-xs font-medium text-slate-500 mb-1.5 block">Source</label>
                                    <Select value={source} onValueChange={setSource}>
                                        <SelectTrigger className="bg-slate-50 border-0">
                                            <SelectValue placeholder="All sources" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">All Sources</SelectItem>
                                            <SelectItem value="BOAMP_FR">BOAMP FR</SelectItem>
                                            <SelectItem value="TED">TED EU</SelectItem>
                                            <SelectItem value="ETENDERS_IE">eTenders IE</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                                
                                <div>
                                    <label className="text-xs font-medium text-slate-500 mb-1.5 block">Buyer contains</label>
                                    <Input
                                        placeholder="e.g. Ministry"
                                        value={buyerSearch}
                                        onChange={(e) => setBuyerSearch(e.target.value)}
                                        className="bg-slate-50 border-0"
                                    />
                                </div>
                                
                                <div>
                                    <label className="text-xs font-medium text-slate-500 mb-1.5 block">CPV code</label>
                                    <Input
                                        placeholder="e.g. 45000000"
                                        value={cpvSearch}
                                        onChange={(e) => setCpvSearch(e.target.value)}
                                        className="bg-slate-50 border-0"
                                    />
                                </div>
                                
                                <div>
                                    <label className="text-xs font-medium text-slate-500 mb-1.5 block">Deadline within</label>
                                    <Select value={deadlineWithin} onValueChange={setDeadlineWithin}>
                                        <SelectTrigger className="bg-slate-50 border-0">
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
                                    <label className="text-xs font-medium text-slate-500 mb-1.5 block">Industry</label>
                                    <Select value={industry} onValueChange={setIndustry}>
                                        <SelectTrigger className="bg-slate-50 border-0">
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
                                    <label className="text-xs font-medium text-slate-500 mb-1.5 block">Institution Type</label>
                                    <Select value={institutionType} onValueChange={setInstitutionType}>
                                        <SelectTrigger className="bg-slate-50 border-0">
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
                                    <label className="text-xs font-medium text-slate-500 mb-1.5 block">Published within</label>
                                    <Select value={lastTendered} onValueChange={setLastTendered}>
                                        <SelectTrigger className="bg-slate-50 border-0">
                                            <SelectValue placeholder="Any time" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">Any time</SelectItem>
                                            <SelectItem value="7">Last 7 days</SelectItem>
                                            <SelectItem value="30">Last 30 days</SelectItem>
                                            <SelectItem value="90">Last 3 months</SelectItem>
                                            <SelectItem value="180">Last 6 months</SelectItem>
                                            <SelectItem value="365">Last year</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                                </div>
                            
                            {hasActiveFilters && (
                                <div className="mt-4 flex justify-end">
                                    <Button variant="ghost" size="sm" onClick={clearFilters}>
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
                <p className="text-sm text-slate-500">
                    Showing <span className="font-medium text-slate-900">{filteredTenders.length}</span> tenders
                </p>
            </div>
            
            {/* Results Table */}
            <Card className="border-0 shadow-sm overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full">
                        <thead>
                            <tr className="bg-slate-50 border-b border-slate-100">
                                <th className="text-left text-xs font-semibold text-slate-500 uppercase tracking-wider px-4 py-3">
                                    Tender
                                </th>
                                <th className="text-left text-xs font-semibold text-slate-500 uppercase tracking-wider px-4 py-3 hidden md:table-cell">
                                    Buyer
                                </th>
                                <th className="text-left text-xs font-semibold text-slate-500 uppercase tracking-wider px-4 py-3 hidden lg:table-cell">
                                    Publication
                                </th>
                                <th className="text-left text-xs font-semibold text-slate-500 uppercase tracking-wider px-4 py-3">
                                    Deadline
                                </th>
                                <th className="text-left text-xs font-semibold text-slate-500 uppercase tracking-wider px-4 py-3 hidden sm:table-cell">
                                    Source
                                </th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100">
                            {filteredTenders.length === 0 ? (
                                <tr>
                                    <td colSpan={5} className="px-4 py-12 text-center text-slate-500">
                                        <SearchIcon className="h-8 w-8 mx-auto mb-2 text-slate-300" />
                                        <p>No tenders found matching your criteria</p>
                                    </td>
                                </tr>
                            ) : (
                                filteredTenders.map(tender => {
                                    const isNew = tender.first_seen_at && 
                                        isAfter(new Date(tender.first_seen_at), subDays(new Date(), 1));
                                    const isUpdated = tender.version_count > 1;
                                    
                                    return (
                                        <tr 
                                            key={tender.id} 
                                            className="hover:bg-slate-50 transition-colors cursor-pointer"
                                            onClick={() => window.location.href = createPageUrl(`TenderDetail?id=${tender.id}`)}
                                        >
                                            <td className="px-4 py-4">
                                                <div className="flex items-start gap-3">
                                                    <span className="text-lg">{getCountryFlag(tender.country)}</span>
                                                    <div className="min-w-0">
                                                        <div className="flex items-center gap-2 flex-wrap">
                                                            <p className="font-medium text-slate-900 line-clamp-2">
                                                                {tender.title}
                                                            </p>
                                                        </div>
                                                        <div className="flex items-center gap-2 mt-1">
                                                            {isNew && (
                                                                <Badge className="bg-emerald-50 text-emerald-700 border-emerald-200 text-xs">
                                                                    New
                                                                </Badge>
                                                            )}
                                                            {isUpdated && (
                                                                <Badge className="bg-amber-50 text-amber-700 border-amber-200 text-xs">
                                                                    Updated
                                                                </Badge>
                                                            )}
                                                        </div>
                                                    </div>
                                                </div>
                                            </td>
                                            <td className="px-4 py-4 hidden md:table-cell">
                                                <p className="text-sm text-slate-600 line-clamp-2">
                                                    {tender.buyer_name || '-'}
                                                </p>
                                            </td>
                                            <td className="px-4 py-4 hidden lg:table-cell">
                                                <p className="text-sm text-slate-600">
                                                    {tender.publication_date 
                                                        ? format(new Date(tender.publication_date), 'MMM d, yyyy')
                                                        : '-'
                                                    }
                                                </p>
                                            </td>
                                            <td className="px-4 py-4">
                                                <p className="text-sm text-slate-600">
                                                    {tender.deadline_date 
                                                        ? format(new Date(tender.deadline_date), 'MMM d, yyyy')
                                                        : '-'
                                                    }
                                                </p>
                                            </td>
                                            <td className="px-4 py-4 hidden sm:table-cell">
                                                <Badge className={`${getSourceBadge(tender.source)} border text-xs`}>
                                                    {tender.source}
                                                </Badge>
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