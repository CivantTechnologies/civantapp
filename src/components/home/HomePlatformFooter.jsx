import React from 'react';
import { Link } from 'react-router-dom';

function formatRefreshLabel(value) {
  if (!value) return 'Unknown';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return 'Unknown';

  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit'
  }).format(parsed);
}

export default function HomePlatformFooter({
  version = 'v0.9.3',
  lastDataRefresh = null,
  supportTo,
  legalTo
}) {
  return (
    <footer className="mt-6 border-t border-white/[0.04] bg-white/[0.01] px-4 py-3">
      <div className="mx-auto flex w-full flex-wrap items-center justify-center gap-x-5 gap-y-1 text-center text-[11px] text-muted-foreground/80">
        <span>{version}</span>
        <span>Last data refresh: {formatRefreshLabel(lastDataRefresh)}</span>
        <Link to={supportTo} className="transition-colors hover:text-slate-200">Support</Link>
        <Link to={legalTo} className="transition-colors hover:text-slate-200">Privacy / Legal</Link>
      </div>
    </footer>
  );
}
