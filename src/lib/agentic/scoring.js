function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function mean(values) {
  if (!values.length) return 0;
  return values.reduce((sum, n) => sum + n, 0) / values.length;
}

export function scoreDataConfidence(input) {
  const completenessScore = clamp(input.completeness, 0, 1) * 14;
  const recencyScore = clamp(1 - input.recencyDays / 180, 0, 1) * 10;
  const historyScore = clamp(input.historyLengthWeeks / 52, 0, 1) * 8;
  const dedupeScore = clamp(input.dedupeQuality, 0, 1) * 8;
  return clamp(Math.round((completenessScore + recencyScore + historyScore + dedupeScore) * 100) / 100, 0, 40);
}

export function scoreSignalConfidence(input) {
  if (!input.signals.length) {
    return 0;
  }
  const strength = mean(input.signals.map((s) => clamp(s.strength, 0, 1)));
  const sourceQuality = mean(input.signals.map((s) => clamp(s.source_quality, 0, 1)));
  const agreement = clamp(input.agreement, 0, 1);
  const total = (strength * 0.45 + sourceQuality * 0.35 + agreement * 0.2) * 30;
  return clamp(Math.round(total * 100) / 100, 0, 30);
}

export function scoreModelConfidence(input) {
  const calibration = clamp(input.calibration, 0, 1);
  const varianceQuality = 1 - clamp(input.variance, 0, 1);
  const stability = clamp(input.stability, 0, 1);
  const total = (calibration * 0.5 + varianceQuality * 0.25 + stability * 0.25) * 30;
  return clamp(Math.round(total * 100) / 100, 0, 30);
}

export function computeConfidence(input) {
  const data_confidence = scoreDataConfidence(input.data);
  const signal_confidence = scoreSignalConfidence(input.signals);
  const model_confidence = scoreModelConfidence(input.model);
  const overall_confidence = clamp(
    Math.round((data_confidence + signal_confidence + model_confidence) * 100) / 100,
    0,
    100
  );

  return {
    data_confidence,
    signal_confidence,
    model_confidence,
    overall_confidence,
    drivers: input.drivers || []
  };
}
