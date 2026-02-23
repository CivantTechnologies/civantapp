import React, { useMemo } from 'react';
import { addMonths, format, startOfMonth } from 'date-fns';
import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
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

function formatWindow(row) {
  const startValue = row?.predicted_window_start || row?.predicted_tender_date;
  const endValue = row?.predicted_window_end || row?.predicted_tender_date;
  if (!startValue && !endValue) return 'Window pending';
  const start = startValue ? new Date(startValue) : null;
  const end = endValue ? new Date(endValue) : null;
  const validStart = start && !Number.isNaN(start.getTime()) ? start : null;
  const validEnd = end && !Number.isNaN(end.getTime()) ? end : null;
  if (validStart && validEnd && format(validStart, 'MMM yyyy') !== format(validEnd, 'MMM yyyy')) {
    return `${format(validStart, 'MMM yyyy')} – ${format(validEnd, 'MMM yyyy')}`;
  }
  const single = validStart || validEnd;
  return single ? format(single, 'MMM yyyy') : 'Window pending';
}

function cycleReference(row) {
  if (Number.isFinite(Number(row?.avg_cycle_days)) && Number(row?.avg_cycle_days) > 0) {
    return `Avg cycle ${Math.round(Number(row.avg_cycle_days))} days`;
  }
  const drivers = Array.isArray(row?.top_drivers) ? row.top_drivers : [];
  if (drivers.length > 0) {
    const first = drivers[0];
    const label = typeof first === 'string'
      ? first
      : first?.label || first?.narrative || '';
    if (label) return label;
  }
  const signalType = String(row?.signal_type || '').trim();
  if (signalType) return signalType.replaceAll('_', ' ');
  return 'Pattern baseline';
}

function TimelineTooltip({ active = false, payload = [] } = {}) {
  if (!active || !Array.isArray(payload) || payload.length === 0) return null;

  const point = payload[0]?.payload;
  if (!point) return null;

  return (
    <div className="min-w-[14rem] rounded-lg border border-white/[0.1] bg-background/95 px-3 py-2 text-xs shadow-xl">
      <p className="truncate text-sm font-medium text-card-foreground">{point.buyerName}</p>
      <p className="mt-1 text-muted-foreground">Expected window: {point.expectedWindow}</p>
      <p className="text-muted-foreground">Confidence: {point.confidence}%</p>
      <p className="mt-1 truncate text-muted-foreground">Past cycle: {point.cycleRef}</p>
    </div>
  );
}

function Marker(props) {
  const { cx, cy, payload } = props || {};
  if (!Number.isFinite(cx) || !Number.isFinite(cy)) return null;
  const radius = Number(payload?.radius || 3.5);
  const opacity = Number(payload?.opacity || 0.45);
  return <circle cx={cx} cy={cy} r={radius} fill="#67e8f9" opacity={opacity} />;
}

export default function ForecastTimeline({ rows = [] }) {
  const timeline = useMemo(() => {
    const start = startOfMonth(new Date());
    const months = Array.from({ length: 12 }, (_, index) => {
      const monthDate = addMonths(start, index);
      return {
        monthIndex: index,
        monthKey: format(monthDate, 'yyyy-MM'),
        monthLabel: format(monthDate, 'MMM')
      };
    });
    const monthMap = new Map(months.map((month) => [month.monthKey, month.monthIndex]));
    const end = addMonths(start, 12);

    const points = rows
      .map((row, index) => {
        const dateValue = getPredictionDate(row);
        if (!dateValue) return null;
        const date = new Date(dateValue);
        if (Number.isNaN(date.getTime()) || date < start || date >= end) return null;

        const key = format(startOfMonth(date), 'yyyy-MM');
        const monthIndex = monthMap.get(key);
        if (monthIndex === undefined) return null;

        const confidence = getConfidencePercent(row);
        return {
          id: row?.id || row?.prediction_id || `${key}-${index}`,
          monthIndex,
          lane: (index % 4) + 1,
          confidence,
          buyerName: row?.buyer_name || row?.buyer_display_name || row?.buyer_id || 'Unknown buyer',
          expectedWindow: formatWindow(row),
          cycleRef: cycleReference(row),
          radius: 2.8 + (confidence / 100) * 2.4,
          opacity: 0.3 + (confidence / 100) * 0.45
        };
      })
      .filter(Boolean);

    return { months, points };
  }, [rows]);

  return (
    <section className="space-y-3 rounded-2xl bg-white/[0.015] px-4 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <h3 className="text-base font-semibold text-card-foreground">Renewal Timeline (Next 12 Months)</h3>
        <div className="flex items-center gap-4 text-xs text-muted-foreground">
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-cyan-300/80" />
            Higher confidence
          </span>
          <span className="text-muted-foreground/80">Marker size and opacity indicate confidence.</span>
        </div>
      </div>

      {timeline.points.length === 0 ? (
        <div className="flex h-[220px] items-center justify-center text-sm text-muted-foreground">
          Not enough historical data yet. We&apos;re building your timeline as new forecasts arrive.
        </div>
      ) : (
        <div className="h-[240px]">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 16, right: 12, left: 0, bottom: 12 }}>
              <CartesianGrid vertical={false} stroke="rgba(148,163,184,0.15)" />
              <XAxis
                type="number"
                dataKey="monthIndex"
                ticks={timeline.months.map((month) => month.monthIndex)}
                tickFormatter={(index) => timeline.months[index]?.monthLabel || ''}
                domain={[0, 11]}
                tickLine={false}
                axisLine={false}
                tick={{ fill: 'rgba(148,163,184,0.72)', fontSize: 11 }}
              />
              <YAxis type="number" dataKey="lane" domain={[0.5, 4.5]} hide />
              <Tooltip cursor={false} content={<TimelineTooltip />} />
              <Scatter data={timeline.points} shape={<Marker />} />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}
