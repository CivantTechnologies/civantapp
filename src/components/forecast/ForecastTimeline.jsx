import React, { useMemo } from 'react';
import { addMonths, format, startOfMonth } from 'date-fns';
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';

function getPredictionDate(row) {
  return row?.predicted_window_start || row?.predicted_tender_date || row?.contract_end_date || null;
}

function getConfidencePercent(row) {
  const raw = Number(row?.probability ?? row?.confidence ?? 0);
  if (!Number.isFinite(raw)) return 0;
  if (raw <= 1) return Math.max(0, Math.min(100, Math.round(raw * 100)));
  return Math.max(0, Math.min(100, Math.round(raw)));
}

function MonthTooltip({ active = false, payload = [] } = {}) {
  if (!active || !Array.isArray(payload) || payload.length === 0) return null;
  const data = payload[0]?.payload;
  if (!data) return null;

  return (
    <div className="min-w-[10rem] rounded-lg border border-white/[0.1] bg-background/95 px-3 py-2 text-xs shadow-xl">
      <p className="text-sm font-medium text-card-foreground">{data.monthFull}</p>
      <p className="mt-1 text-muted-foreground">{data.count} forecast{data.count !== 1 ? 's' : ''}</p>
      {data.highConf > 0 ? (
        <p className="text-muted-foreground">{data.highConf} high confidence (&ge;80%)</p>
      ) : null}
      {data.avgConf > 0 ? (
        <p className="text-muted-foreground">Avg confidence: {data.avgConf}%</p>
      ) : null}
    </div>
  );
}

export default function ForecastTimeline({ rows = [] }) {
  const chartData = useMemo(() => {
    const start = startOfMonth(new Date());
    const months = Array.from({ length: 12 }, (_, index) => {
      const monthDate = addMonths(start, index);
      return {
        monthKey: format(monthDate, 'yyyy-MM'),
        monthLabel: format(monthDate, 'MMM'),
        monthFull: format(monthDate, 'MMMM yyyy'),
        count: 0,
        highConf: 0,
        totalConf: 0,
        avgConf: 0,
      };
    });
    const monthMap = new Map(months.map((m) => [m.monthKey, m]));
    const end = addMonths(start, 12);

    for (const row of rows) {
      const dateValue = getPredictionDate(row);
      if (!dateValue) continue;
      const date = new Date(dateValue);
      if (Number.isNaN(date.getTime()) || date < start || date >= end) continue;
      const key = format(startOfMonth(date), 'yyyy-MM');
      const bucket = monthMap.get(key);
      if (!bucket) continue;
      const conf = getConfidencePercent(row);
      bucket.count += 1;
      bucket.totalConf += conf;
      if (conf >= 80) bucket.highConf += 1;
    }

    for (const m of months) {
      m.avgConf = m.count > 0 ? Math.round(m.totalConf / m.count) : 0;
    }

    return months;
  }, [rows]);

  const maxCount = Math.max(...chartData.map((m) => m.count), 1);

  return (
    <section className="space-y-3 rounded-2xl bg-white/[0.015] px-4 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <h3 className="text-base font-semibold text-card-foreground">Forecast Pipeline (Next 12 Months)</h3>
        <div className="flex items-center gap-4 text-xs text-muted-foreground">
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-cyan-400" />
            Predicted opportunities per month
          </span>
        </div>
      </div>

      {chartData.every((m) => m.count === 0) ? (
        <div className="flex h-[180px] items-center justify-center text-sm text-muted-foreground">
          Not enough historical data yet. We&apos;re building your pipeline as new forecasts arrive.
        </div>
      ) : (
        <div className="h-[200px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ top: 8, right: 8, left: -12, bottom: 4 }}>
              <CartesianGrid vertical={false} stroke="rgba(148,163,184,0.12)" />
              <XAxis
                dataKey="monthLabel"
                tickLine={false}
                axisLine={false}
                tick={{ fill: 'rgba(148,163,184,0.72)', fontSize: 11 }}
              />
              <YAxis
                tickLine={false}
                axisLine={false}
                tick={{ fill: 'rgba(148,163,184,0.5)', fontSize: 10 }}
                allowDecimals={false}
                width={36}
              />
              <Tooltip cursor={{ fill: 'rgba(148,163,184,0.06)' }} content={<MonthTooltip />} />
              <Bar
                dataKey="count"
                fill="#22d3ee"
                radius={[4, 4, 0, 0]}
                fillOpacity={0.7}
                maxBarSize={40}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}
