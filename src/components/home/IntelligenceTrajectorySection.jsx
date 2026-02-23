import React, { useMemo } from 'react';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';

const RANGE_OPTIONS = [
  { key: '6m', label: '6M' },
  { key: '12m', label: '12M' }
];

function TrajectoryTooltip({ active = false, payload = [], label = '' } = {}) {
  if (!active || !Array.isArray(payload) || payload.length === 0) return null;

  const trackedScope = Number(payload.find((item) => item.dataKey === 'trackedScope')?.value || 0);
  const totalMarket = Number(payload.find((item) => item.dataKey === 'totalMarket')?.value || 0);

  return (
    <div className="min-w-[11rem] rounded-lg border border-white/[0.1] bg-background/95 px-3 py-2 text-xs shadow-xl">
      <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">{label}</p>
      <div className="mt-1.5 space-y-1.5">
        <div className="flex items-center justify-between gap-4">
          <span className="text-slate-300">Tracked Scope</span>
          <span className="font-medium tabular-nums text-slate-100">{trackedScope.toLocaleString()}</span>
        </div>
        <div className="flex items-center justify-between gap-4">
          <span className="text-slate-400">Total Market</span>
          <span className="font-medium tabular-nums text-slate-200">{totalMarket.toLocaleString()}</span>
        </div>
      </div>
    </div>
  );
}

export default function IntelligenceTrajectorySection({
  series12m = [],
  range = '12m',
  onRangeChange,
  indicators = null
}) {
  const chartData = useMemo(() => (
    range === '6m' ? series12m.slice(-6) : series12m
  ), [range, series12m]);

  const hasHistory = chartData.some((point) =>
    Number(point?.trackedScope || 0) > 0 || Number(point?.totalMarket || 0) > 0
  );

  const missedRenewalCycles12m = Number(indicators?.missedRenewalCycles12m || 0);
  const newBuyersDetected90d = Number(indicators?.newBuyersDetected90d || 0);
  const incumbentDominanceShift12m = Number(indicators?.incumbentDominanceShift12m || 0);
  const dominanceShiftLabel = `${incumbentDominanceShift12m >= 0 ? '+' : ''}${incumbentDominanceShift12m.toFixed(1)}pp`;

  return (
    <section className="space-y-5 pt-2">
      <div className="space-y-1">
        <p className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">Intelligence Trajectory</p>
        <p className="text-sm text-muted-foreground">
          How your tracked scope is trending relative to the market.
        </p>
      </div>

      <div className="grid grid-cols-1 gap-8 lg:grid-cols-[minmax(0,1.65fr)_minmax(0,1fr)] lg:items-start">
        <div className="space-y-4 rounded-2xl bg-white/[0.015] p-4">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="space-y-2">
              <h3 className="text-base font-medium text-card-foreground">Relevant Opportunities (12 months)</h3>
              <div className="flex items-center gap-5 text-xs text-muted-foreground">
                <span className="inline-flex items-center gap-2">
                  <span className="h-px w-4 bg-cyan-300/85" />
                  Tracked Scope
                </span>
                <span className="inline-flex items-center gap-2">
                  <span className="h-px w-4 bg-slate-300/70" />
                  Total Market
                </span>
              </div>
            </div>

            <div className="inline-flex rounded-lg bg-white/[0.03] p-1">
              {RANGE_OPTIONS.map((option) => {
                const isActive = range === option.key;
                return (
                  <button
                    key={option.key}
                    type="button"
                    onClick={() => onRangeChange?.(option.key)}
                    className={`rounded-md px-2.5 py-1 text-[11px] font-medium tracking-[0.04em] transition-colors ${
                      isActive
                        ? 'bg-white/[0.08] text-slate-100'
                        : 'text-slate-400 hover:text-slate-200'
                    }`}
                  >
                    {option.label}
                  </button>
                );
              })}
            </div>
          </div>

          {hasHistory ? (
            <div className="h-[280px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData} margin={{ top: 8, right: 8, left: -14, bottom: 2 }}>
                  <CartesianGrid vertical={false} stroke="rgba(148,163,184,0.16)" strokeDasharray="3 3" />
                  <XAxis
                    dataKey="monthLabel"
                    tickLine={false}
                    axisLine={false}
                    tick={{ fill: 'rgba(148,163,184,0.75)', fontSize: 11 }}
                  />
                  <YAxis
                    tickLine={false}
                    axisLine={false}
                    allowDecimals={false}
                    width={32}
                    tick={{ fill: 'rgba(148,163,184,0.75)', fontSize: 11 }}
                  />
                  <Tooltip cursor={{ stroke: 'rgba(148,163,184,0.25)', strokeWidth: 1 }} content={<TrajectoryTooltip />} />
                  <Line
                    type="monotone"
                    dataKey="trackedScope"
                    stroke="#67e8f9"
                    strokeWidth={1.8}
                    dot={false}
                    activeDot={{ r: 3, fill: '#67e8f9', strokeWidth: 0 }}
                  />
                  <Line
                    type="monotone"
                    dataKey="totalMarket"
                    stroke="#cbd5e1"
                    strokeWidth={1.6}
                    dot={false}
                    activeDot={{ r: 3, fill: '#cbd5e1', strokeWidth: 0 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="flex h-[280px] items-center justify-center text-center text-sm text-muted-foreground">
              Not enough historical data yet. We&apos;re building your trend line as new signals arrive.
            </div>
          )}
        </div>

        <div className="space-y-7 pt-1">
          <div className="space-y-1">
            <p className="text-4xl font-semibold tracking-tight text-card-foreground tabular-nums">
              {missedRenewalCycles12m.toLocaleString()}
            </p>
            <p className="text-sm font-medium text-slate-300">Missed Renewal Cycles (12M)</p>
            <p className="text-xs text-muted-foreground">Overdue high-confidence renewal windows in tracked scope.</p>
          </div>

          <div className="space-y-1">
            <p className="text-4xl font-semibold tracking-tight text-card-foreground tabular-nums">
              {newBuyersDetected90d.toLocaleString()}
            </p>
            <p className="text-sm font-medium text-slate-300">New Buyers Detected (90d)</p>
            <p className="text-xs text-muted-foreground">Net-new buyer organizations versus the prior window.</p>
          </div>

          <div className="space-y-1">
            <p className="text-4xl font-semibold tracking-tight text-card-foreground tabular-nums">
              {dominanceShiftLabel}
            </p>
            <p className="text-sm font-medium text-slate-300">Incumbent Dominance Shift (12M)</p>
            <p className="text-xs text-muted-foreground">Top-buyer concentration change versus the previous 6 months.</p>
          </div>
        </div>
      </div>
    </section>
  );
}
