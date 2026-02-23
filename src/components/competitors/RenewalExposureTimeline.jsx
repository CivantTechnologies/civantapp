import React from 'react';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';

function TimelineTooltip({ active = false, payload = [], label = '' } = {}) {
  if (!active || !Array.isArray(payload) || payload.length === 0) return null;

  const competitorSeries = payload.find((item) => item?.dataKey === 'competitorExposure');
  const yourSeries = payload.find((item) => item?.dataKey === 'yourExposure');

  return (
    <div className="min-w-[12rem] rounded-lg border border-white/[0.1] bg-background/95 px-3 py-2 text-xs shadow-xl">
      <p className="font-medium text-card-foreground">{label}</p>
      <p className="mt-1 text-muted-foreground">
        Competitor exposure: {Number(competitorSeries?.value || 0).toLocaleString()}
      </p>
      {yourSeries ? (
        <p className="text-muted-foreground">
          Your exposure: {Number(yourSeries?.value || 0).toLocaleString()}
        </p>
      ) : null}
    </div>
  );
}

export default function RenewalExposureTimeline({ data = [], showYourExposure = false, loading = false }) {
  const hasData = Array.isArray(data) && data.length > 0;

  return (
    <section className="space-y-3 rounded-2xl bg-white/[0.015] px-4 py-4">
      <h3 className="text-base font-semibold text-card-foreground">Renewal Exposure Timeline (12 Months)</h3>

      {loading ? (
        <div className="h-[240px] animate-pulse rounded-xl bg-white/[0.02]" />
      ) : !hasData ? (
        <div className="flex h-[220px] items-center justify-center text-sm text-muted-foreground">
          Not enough renewal history yet to plot exposure trend.
        </div>
      ) : (
        <div className="h-[240px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 4 }}>
              <CartesianGrid vertical={false} stroke="rgba(148,163,184,0.14)" />
              <XAxis
                dataKey="label"
                tickLine={false}
                axisLine={false}
                tick={{ fill: 'rgba(148,163,184,0.72)', fontSize: 11 }}
              />
              <YAxis
                tickLine={false}
                axisLine={false}
                allowDecimals={false}
                width={30}
                tick={{ fill: 'rgba(148,163,184,0.72)', fontSize: 11 }}
              />
              <Tooltip content={<TimelineTooltip />} />
              <Line
                type="monotone"
                dataKey="competitorExposure"
                stroke="rgba(45,212,191,0.95)"
                strokeWidth={1.6}
                dot={false}
                activeDot={{ r: 3 }}
              />
              {showYourExposure ? (
                <Line
                  type="monotone"
                  dataKey="yourExposure"
                  stroke="rgba(148,163,184,0.85)"
                  strokeWidth={1.3}
                  strokeDasharray="3 3"
                  dot={false}
                  activeDot={{ r: 2.5 }}
                />
              ) : null}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}

