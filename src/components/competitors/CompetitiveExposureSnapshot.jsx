import React from 'react';

function SnapshotMetric({ label, value, hint }) {
  return (
    <div className="space-y-1.5 rounded-xl bg-white/[0.01] px-4 py-3">
      <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">{label}</p>
      <p className="text-2xl font-semibold tracking-tight text-card-foreground tabular-nums">{value}</p>
      {hint ? <p className="text-xs text-muted-foreground">{hint}</p> : null}
    </div>
  );
}

export default function CompetitiveExposureSnapshot({ metrics = [], loading = false }) {
  return (
    <section className="space-y-3">
      <div className="space-y-1">
        <h2 className="text-base font-semibold text-card-foreground">Competitive Exposure Snapshot</h2>
      </div>

      {loading ? (
        <div className="py-4 text-sm text-muted-foreground">Compiling exposure signals...</div>
      ) : (
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
          {metrics.map((metric) => (
            <SnapshotMetric
              key={metric.label}
              label={metric.label}
              value={metric.value}
              hint={metric.hint}
            />
          ))}
        </div>
      )}
    </section>
  );
}

