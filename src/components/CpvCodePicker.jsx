import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Search, X } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import { getCpvEntryByCode, normalizeCpvCodeList, searchCpvCatalog } from '@/lib/cpv-catalog';

function parsePastedCodes(text) {
  const matches = String(text || '').match(/\d{8}/g);
  if (!matches) return [];
  return normalizeCpvCodeList(matches);
}

export default function CpvCodePicker({
  value = [],
  onChange,
  placeholder = 'Search CPV by code or keyword',
  language = 'en',
  maxSelections = 20,
  className
}) {
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);

  const selectedCodes = useMemo(() => normalizeCpvCodeList(value), [value]);
  const selectedSet = useMemo(() => new Set(selectedCodes), [selectedCodes]);

  const selectedEntries = useMemo(
    () =>
      selectedCodes.map((code) => {
        const cpv = getCpvEntryByCode(code, language);
        return {
          code,
          label: cpv?.label || 'CPV description unavailable'
        };
      }),
    [language, selectedCodes]
  );

  const suggestions = useMemo(
    () =>
      searchCpvCatalog(query, {
        language,
        limit: 18,
        excludeCodes: selectedCodes
      }),
    [query, language, selectedCodes]
  );

  useEffect(() => {
    const onClickOutside = (event) => {
      if (!rootRef.current?.contains(event.target)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', onClickOutside);
    return () => document.removeEventListener('mousedown', onClickOutside);
  }, []);

  const commit = (nextCodes) => {
    onChange?.(normalizeCpvCodeList(nextCodes));
  };

  const addCode = (code) => {
    if (!code || selectedSet.has(code) || selectedCodes.length >= maxSelections) return;
    commit([...selectedCodes, code]);
    setQuery('');
    setOpen(true);
  };

  const removeCode = (code) => {
    commit(selectedCodes.filter((item) => item !== code));
  };

  const handleKeyDown = (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      if (suggestions.length > 0) {
        addCode(suggestions[0].code);
        return;
      }
      const exact = getCpvEntryByCode(query, language);
      if (exact) addCode(exact.code);
    }

    if (event.key === 'Backspace' && !query && selectedCodes.length) {
      removeCode(selectedCodes[selectedCodes.length - 1]);
    }
  };

  const handlePaste = (event) => {
    const pasted = event.clipboardData?.getData('text') || '';
    const pastedCodes = parsePastedCodes(pasted);
    if (!pastedCodes.length) return;
    event.preventDefault();

    const next = [...selectedCodes];
    for (const code of pastedCodes) {
      if (next.includes(code)) continue;
      if (next.length >= maxSelections) break;
      next.push(code);
    }
    commit(next);
    setQuery('');
    setOpen(true);
  };

  return (
    <div ref={rootRef} className={cn('space-y-2', className)}>
      <div className="rounded-xl border border-border bg-slate-900/40">
        {selectedEntries.length > 0 ? (
          <div className="px-3 pt-3 pb-1 flex flex-wrap gap-2">
            {selectedEntries.map((entry) => (
              <Badge key={entry.code} variant="outline" className="gap-1 py-1">
                <span className="font-mono text-[11px]">{entry.code}</span>
                <span className="text-xs">{entry.label}</span>
                <button
                  type="button"
                  aria-label={`Remove ${entry.code}`}
                  className="ml-1 rounded hover:text-primary"
                  onClick={() => removeCode(entry.code)}
                >
                  <X className="h-3 w-3" />
                </button>
              </Badge>
            ))}
          </div>
        ) : null}

        <div className="relative p-2">
          <Search className="pointer-events-none absolute left-5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            value={query}
            onChange={(event) => {
              setQuery(event.target.value);
              setOpen(true);
            }}
            onFocus={() => setOpen(true)}
            onKeyDown={handleKeyDown}
            onPaste={handlePaste}
            placeholder={placeholder}
            className="pl-10 bg-transparent border-0 focus-visible:ring-1"
          />
        </div>

        {open ? (
          <div className="border-t border-border max-h-64 overflow-y-auto">
            {suggestions.length ? (
              suggestions.map((item) => (
                <button
                  key={item.code}
                  type="button"
                  onClick={() => addCode(item.code)}
                  className="w-full px-3 py-2 text-left hover:bg-civant-teal/10 focus:bg-civant-teal/10 transition flex items-start justify-between gap-3"
                >
                  <div>
                    <p className="font-mono text-xs text-civant-teal">{item.code}</p>
                    <p className="text-sm text-card-foreground">{item.label}</p>
                  </div>
                  <span className="text-xs text-muted-foreground">Add</span>
                </button>
              ))
            ) : (
              <div className="px-3 py-3 text-xs text-muted-foreground">
                No CPV match found. Try code prefixes (e.g. 72) or keywords (e.g. software, construction).
              </div>
            )}
          </div>
        ) : null}
      </div>

      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>Tip: type keywords or paste CPV codes directly.</span>
        <span>
          {selectedCodes.length}/{maxSelections} selected
        </span>
      </div>

      {selectedCodes.length >= maxSelections ? (
        <p className="text-xs text-amber-300">You reached the maximum of {maxSelections} CPV codes.</p>
      ) : null}
    </div>
  );
}
