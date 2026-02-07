import test from 'node:test';
import assert from 'node:assert/strict';
import { computeConfidence, scoreDataConfidence, scoreModelConfidence, scoreSignalConfidence } from '../src/lib/agentic/scoring.js';

test('component scores stay within required bounds', () => {
  const data = scoreDataConfidence({ completeness: 1, recencyDays: 0, historyLengthWeeks: 52, dedupeQuality: 1 });
  const signal = scoreSignalConfidence({ signals: [{ strength: 1, source_quality: 1 }], agreement: 1 });
  const model = scoreModelConfidence({ calibration: 1, variance: 0, stability: 1 });

  assert.equal(data <= 40 && data >= 0, true);
  assert.equal(signal <= 30 && signal >= 0, true);
  assert.equal(model <= 30 && model >= 0, true);
});

test('overall confidence equals additive breakdown', () => {
  const breakdown = computeConfidence({
    data: { completeness: 0.9, recencyDays: 10, historyLengthWeeks: 40, dedupeQuality: 0.95 },
    signals: {
      signals: [
        { strength: 0.9, source_quality: 0.8 },
        { strength: 0.8, source_quality: 0.9 }
      ],
      agreement: 0.75
    },
    model: { calibration: 0.82, variance: 0.18, stability: 0.76 },
    drivers: ['test']
  });

  const sum = breakdown.data_confidence + breakdown.signal_confidence + breakdown.model_confidence;
  assert.equal(breakdown.overall_confidence, Math.round(sum * 100) / 100);
  assert.equal(breakdown.drivers.length, 1);
});
