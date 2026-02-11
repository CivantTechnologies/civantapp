import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Search, X } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

function normalizeArray(value) {
  if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean);
  if (!value) return [];
  return String(value)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

export default function OptionMultiSelector({
  value = [],
  options = [],
  onChange,
  placeholder = 'Search options',
  helperText = '',
  maxSelections = 20,
  className
}) {
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);

  const selected = useMemo(() => normalizeArray(value), [value]);
  const selectedSet = useMemo(() => new Set(selected), [selected]);

  const available = useMemo(() => {
    const q = query.trim().toLowerCase();
    return options
      .filter((item) => !selectedSet.has(item))
      .filter((item) => !q || item.toLowerCase().includes(q))
      .slice(0, 24);
  }, [options, query, selectedSet]);

  useEffect(() => {
    const onClickOutside = (event) => {
      if (!rootRef.current?.contains(event.target)) {
        setOpen(false);
      }
    };

    document.addEventListener('mousedown', onClickOutside);
    return () => document.removeEventListener('mousedown', onClickOutside);
  }, []);

  const commit = (nextValues) => {
    onChange?.(normalizeArray(nextValues));
  };

  const addValue = (next) => {
    if (!next || selectedSet.has(next) || selected.length >= maxSelections) return;
    commit([...selected, next]);
    setQuery('');
    setOpen(true);
  };

  const removeValue = (next) => {
    commit(selected.filter((item) => item !== next));
  };

  const onKeyDown = (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      if (available.length > 0) {
        addValue(available[0]);
      }
    }
    if (event.key === 'Backspace' && !query && selected.length > 0) {
      removeValue(selected[selected.length - 1]);
    }
  };

  return (
    <div ref={rootRef} className={cn('space-y-2', className)}>
      <div className="rounded-xl border border-border bg-slate-900/40">
        {selected.length > 0 ? (
          <div className="px-3 pt-3 pb-1 flex flex-wrap gap-2">
            {selected.map((item) => (
              <Badge key={item} variant="outline" className="gap-1 py-1">
                <span className="text-xs">{item}</span>
                <button
                  type="button"
                  aria-label={`Remove ${item}`}
                  className="ml-1 rounded hover:text-primary"
                  onClick={() => removeValue(item)}
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
            onKeyDown={onKeyDown}
            placeholder={placeholder}
            className="pl-10 bg-transparent border-0 focus-visible:ring-1"
          />
        </div>

        {open ? (
          <div className="border-t border-border max-h-64 overflow-y-auto">
            {available.length ? (
              available.map((item) => (
                <button
                  key={item}
                  type="button"
                  onClick={() => addValue(item)}
                  className="w-full px-3 py-2 text-left hover:bg-civant-teal/10 focus:bg-civant-teal/10 transition flex items-start justify-between gap-3"
                >
                  <span className="text-sm text-card-foreground">{item}</span>
                  <span className="text-xs text-muted-foreground">Add</span>
                </button>
              ))
            ) : (
              <div className="px-3 py-3 text-xs text-muted-foreground">
                No matching options available.
              </div>
            )}
          </div>
        ) : null}
      </div>

      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>{helperText}</span>
        <span>
          {selected.length}/{maxSelections} selected
        </span>
      </div>
    </div>
  );
}
