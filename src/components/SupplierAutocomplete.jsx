import React, { useState, useEffect, useRef, useCallback } from 'react';
import { supabase } from '@/lib/supabaseClient';

export default function SupplierAutocomplete({ value, onChange, placeholder }) {
    const [query, setQuery] = useState(value || '');
    const [suggestions, setSuggestions] = useState([]);
    const [showDrop, setShowDrop] = useState(false);
    const [loading, setLoading] = useState(false);
    const timer = useRef(null);
    const wrap = useRef(null);

    useEffect(() => { setQuery(value || ''); }, [value]);

    useEffect(() => {
        const handler = (e) => {
            if (wrap.current && !wrap.current.contains(e.target)) setShowDrop(false);
        };
        document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, []);

    const doSearch = useCallback(async (term) => {
        if (term.length < 2) { setSuggestions([]); return; }
        setLoading(true);
        try {
            const { data, error } = await (/** @type {any} */ (supabase)).rpc('search_suppliers', { p_term: term, p_limit: 8 });
            if (!error && data) {
                setSuggestions(data);
                setShowDrop(true);
            }
        } catch (e) {
            console.error('Autocomplete error:', e);
        } finally {
            setLoading(false);
        }
    }, []);

    const onType = (e) => {
        const v = e.target.value;
        setQuery(v);
        onChange(v);
        if (timer.current) clearTimeout(timer.current);
        timer.current = setTimeout(() => doSearch(v), 300);
    };

    const pick = (s) => {
        setQuery(s.name);
        onChange(s.name);
        setShowDrop(false);
    };

    const flags = { IE: '🇮🇪', FR: '🇫🇷', ES: '🇪🇸' };

    return (
        <div ref={wrap} className="relative">
            <input
                type="text"
                value={query}
                onChange={onType}
                onFocus={() => suggestions.length > 0 && setShowDrop(true)}
                placeholder={placeholder}
                className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                required
            />
            {loading && (
                <div className="absolute right-3 top-1/2 -translate-y-1/2">
                    <div className="h-3.5 w-3.5 border-2 border-civant-teal/40 border-t-civant-teal rounded-full animate-spin" />
                </div>
            )}
            {showDrop && suggestions.length > 0 && (
                <div className="absolute z-50 mt-1 w-full rounded-xl border border-white/[0.08] bg-slate-900/95 backdrop-blur-md shadow-xl overflow-hidden">
                    {suggestions.map((s, i) => (
                        <button
                            key={i}
                            type="button"
                            className="w-full text-left px-3 py-2.5 hover:bg-white/[0.05] transition-colors flex items-center justify-between gap-2 border-b border-white/[0.04] last:border-0"
                            onClick={() => pick(s)}
                        >
                            <div className="min-w-0">
                                <p className="text-sm text-slate-200 truncate">{s.name}</p>
                                <p className="text-[11px] text-slate-500">
                                    {s.awards} contract{s.awards !== 1 ? 's' : ''}
                                </p>
                            </div>
                            <div className="flex items-center gap-1 shrink-0">
                                {(s.countries || []).map((c, j) => (
                                    <span key={j} className="text-xs">{flags[c] || c}</span>
                                ))}
                            </div>
                        </button>
                    ))}
                </div>
            )}
        </div>
    );
}
