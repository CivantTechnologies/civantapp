#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_MANAGER_STATUS_FILE = '/tmp/placsp-es-block-manager-status.json';
const DEFAULT_MANAGER_CONTROL_FILE = '/tmp/placsp-es-block-manager-control.json';
const DEFAULT_MANAGER_PID_FILE = '/tmp/placsp-es-block-manager.pid';
const DEFAULT_MANAGER_LOG_FILE = '/tmp/placsp-es-block-manager.log';
const DEFAULT_REPORT_LOG_FILE = '/tmp/placsp-es-block-reports.log';
const DEFAULT_BLOCK_FILE_PREFIX = '/tmp/placsp-es-backfill-block';
const DEFAULT_DOWNLOAD_DIR = '/Users/davidmanrique/Downloads/placsp_zips';

const DEFAULT_API_BASE = 'https://civantapp.vercel.app';
const DEFAULT_APP_ID = 'civantapp';
const DEFAULT_TENANT_ID = 'civant_default';
const DEFAULT_BATCH_SIZE = 120;
const DEFAULT_REPORT_INTERVAL_MS = 5 * 60 * 1000;
const DEFAULT_WARNING_THRESHOLD_MS = 3 * 60 * 1000;
const DEFAULT_STALL_THRESHOLD_MS = 10 * 60 * 1000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_POLL_MS = 5000;

const IMPORTER_SCRIPT = path.join(__dirname, 'import-placsp-es.mjs');
const PROJECT_ROOT = path.resolve(__dirname, '..');

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) {
      args._.push(token);
      continue;
    }
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = 'true';
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function clean(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  if (!text) return null;
  return text;
}

function parseBoolean(value, fallback = false) {
  const text = clean(value);
  if (!text) return fallback;
  if (['1', 'true', 'yes', 'y', 'on'].includes(text.toLowerCase())) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(text.toLowerCase())) return false;
  return fallback;
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

function readJson(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function writeJson(filePath, data) {
  if (!filePath) return;
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, 'utf8');
}

function unlinkIfExists(filePath) {
  if (!filePath) return;
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  } catch {
    // best effort
  }
}

function appendLine(filePath, line) {
  fs.appendFileSync(filePath, `${line}\n`, 'utf8');
}

function nowIso() {
  return new Date().toISOString();
}

function isProcessAlive(pid) {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildDefaultBlocks(currentYear = new Date().getUTCFullYear()) {
  const anchors = [2012, 2017, 2021, 2024];
  const blocks = [];

  for (let i = 0; i < anchors.length; i += 1) {
    const fromYear = anchors[i];
    if (fromYear > currentYear) continue;

    const nextAnchor = anchors[i + 1];
    const toYear = nextAnchor ? Math.min(currentYear, nextAnchor - 1) : currentYear;
    if (toYear < fromYear) continue;

    blocks.push({
      key: `${fromYear}-${toYear}`,
      fromYear,
      toYear,
      includeMonthlyCurrentYear: i === anchors.length - 1
    });
  }

  return blocks;
}

function parseBlockSpec(spec) {
  const text = clean(spec);
  if (!text) return buildDefaultBlocks();

  const blocks = [];
  const parts = text.split(',').map((item) => item.trim()).filter(Boolean);
  for (const part of parts) {
    const [rangeRaw, monthlyRaw] = part.split(':');
    const [fromRaw, toRaw] = String(rangeRaw || '').split('-');
    const fromYear = parsePositiveInt(fromRaw, 0);
    const toYear = parsePositiveInt(toRaw, 0);
    if (!fromYear || !toYear || toYear < fromYear) {
      throw new Error(`Invalid block range: ${part}. Expected format like 2012-2016 or 2024-2026:monthly`);
    }
    blocks.push({
      key: `${fromYear}-${toYear}`,
      fromYear,
      toYear,
      includeMonthlyCurrentYear: String(monthlyRaw || '').toLowerCase() === 'monthly'
    });
  }

  if (!blocks.length) {
    throw new Error('No valid blocks resolved from --blocks argument');
  }

  return blocks;
}

function buildBlockFiles(blockPrefix, blockKey) {
  const safe = blockKey.replace(/[^0-9-]/g, '_');
  return {
    statusFile: `${blockPrefix}-${safe}-status.json`,
    checkpointFile: `${blockPrefix}-${safe}-checkpoint.json`,
    controlFile: `${blockPrefix}-${safe}-control.json`,
    pidFile: `${blockPrefix}-${safe}.pid`,
    logFile: `${blockPrefix}-${safe}.log`
  };
}

function formatStatusAgeMs(status) {
  const updatedAt = clean(status?.updated_at);
  if (!updatedAt) return null;
  const ms = Date.parse(updatedAt);
  if (!Number.isFinite(ms)) return null;
  return Date.now() - ms;
}

function extractNextStartRecord(checkpointFile, fallback) {
  const checkpoint = readJson(checkpointFile);
  const parsed = Number(checkpoint?.parsed_records || checkpoint?.processed || 0);
  if (Number.isFinite(parsed) && parsed > 0) {
    return Math.max(1, parsed + 1);
  }
  return fallback;
}

function readManagerControl(controlFile) {
  const payload = readJson(controlFile);
  if (!payload) return null;
  const action = String(payload.action || '').toLowerCase();
  if (!['pause', 'stop', 'resume'].includes(action)) return null;
  return {
    action,
    reason: clean(payload.reason) || null,
    at: clean(payload.at) || nowIso()
  };
}

function writeManagerControl(controlFile, action, reason) {
  writeJson(controlFile, {
    action,
    reason: reason || null,
    at: nowIso()
  });
}

function printHelp() {
  console.log(`PLACSP block ingestion manager

Commands:
  start      Start manager in background (detached)
  monitor    Show manager state (use --watch true for live refresh)
  pause      Request safe pause at next flush checkpoint
  stop       Request safe stop at next flush checkpoint
  restart    Stop (if needed) and start again from saved progress

Shared options:
  --api-base <url>              Default: ${DEFAULT_API_BASE}
  --app-id <id>                 Default: ${DEFAULT_APP_ID}
  --tenant-id <id>              Default: ${DEFAULT_TENANT_ID}
  --download-dir <path>         Default: ${DEFAULT_DOWNLOAD_DIR}
  --batch-size <n>              Default: ${DEFAULT_BATCH_SIZE}
  --blocks <spec>               Comma list, e.g. 2012-2016,2017-2020,2021-2023,2024-2026:monthly
  --max-retries <n>             Default: ${DEFAULT_MAX_RETRIES}
  --report-every-minutes <n>    Default: ${DEFAULT_REPORT_INTERVAL_MS / 60000}
  --warning-threshold-minutes <n> Default: ${DEFAULT_WARNING_THRESHOLD_MS / 60000}
  --stall-threshold-minutes <n> Default: ${DEFAULT_STALL_THRESHOLD_MS / 60000}

Operational files:
  --manager-status-file <path>  Default: ${DEFAULT_MANAGER_STATUS_FILE}
  --manager-control-file <path> Default: ${DEFAULT_MANAGER_CONTROL_FILE}
  --manager-pid-file <path>     Default: ${DEFAULT_MANAGER_PID_FILE}
  --manager-log-file <path>     Default: ${DEFAULT_MANAGER_LOG_FILE}
  --report-log-file <path>      Default: ${DEFAULT_REPORT_LOG_FILE}
  --block-prefix <path>         Default: ${DEFAULT_BLOCK_FILE_PREFIX}

Examples:
  node scripts/placsp-block-manager.mjs start
  node scripts/placsp-block-manager.mjs monitor --watch true
  node scripts/placsp-block-manager.mjs pause --reason "network maintenance"
  node scripts/placsp-block-manager.mjs restart
`);
}

function serializeForwardArgs(args) {
  const out = [];
  for (const [key, value] of Object.entries(args)) {
    if (key === '_') continue;
    if (value === undefined || value === null) continue;
    out.push(`--${key}`);
    if (value !== 'true') out.push(String(value));
  }
  return out;
}

function createInitialState(options, blocks) {
  return {
    phase: 'running',
    started_at: nowIso(),
    updated_at: nowIso(),
    completed_at: null,
    options: {
      api_base: options.apiBase,
      app_id: options.appId,
      tenant_id: options.tenantId,
      download_dir: options.downloadDir,
      batch_size: options.batchSize,
      max_retries: options.maxRetries,
      report_every_minutes: options.reportEveryMs / 60000,
      warning_threshold_minutes: options.warningThresholdMs / 60000,
      stall_threshold_minutes: options.stallThresholdMs / 60000
    },
    current_block_index: null,
    current_block: null,
    blocks: blocks.map((block) => ({
      ...block,
      status: 'pending',
      attempts: 0,
      started_at: null,
      finished_at: null,
      next_start_record: 1,
      last_error: null,
      last_run_id: null,
      files: buildBlockFiles(options.blockPrefix, block.key)
    })),
    last_report_at: null,
    last_error: null,
    pid: process.pid
  };
}

function mergeState(existingState, initialState) {
  if (!existingState || !Array.isArray(existingState.blocks)) return initialState;

  const byKey = new Map(existingState.blocks.map((block) => [block.key, block]));
  const mergedBlocks = initialState.blocks.map((block) => {
    const prev = byKey.get(block.key);
    if (!prev) return block;
    return {
      ...block,
      status: prev.status || block.status,
      attempts: Number(prev.attempts || 0),
      started_at: prev.started_at || null,
      finished_at: prev.finished_at || null,
      next_start_record: Math.max(1, Number(prev.next_start_record || 1)),
      last_error: prev.last_error || null,
      last_run_id: prev.last_run_id || null
    };
  });

  return {
    ...initialState,
    started_at: existingState.started_at || initialState.started_at,
    last_report_at: existingState.last_report_at || null,
    blocks: mergedBlocks
  };
}

function persistState(state, managerStatusFile) {
  state.updated_at = nowIso();
  writeJson(managerStatusFile, state);
}

function reportLine(prefix, payload) {
  return `[${nowIso()}] ${prefix} ${JSON.stringify(payload)}`;
}

async function runBlockAttempt({
  block,
  blockIndex,
  attempt,
  startRecord,
  options,
  state,
  managerStatusFile,
  managerControlFile,
  reportLogFile
}) {
  const files = block.files;
  unlinkIfExists(files.controlFile);

  const logFd = fs.openSync(files.logFile, 'a');

  const args = [
    IMPORTER_SCRIPT,
    '--mode', 'backfill',
    '--api-base', options.apiBase,
    '--app-id', options.appId,
    '--tenant-id', options.tenantId,
    '--with-tenant-header', 'true',
    '--historical-from-year', String(block.fromYear),
    '--historical-to-year', String(block.toYear),
    '--include-monthly-current-year', block.includeMonthlyCurrentYear ? 'true' : 'false',
    '--download-dir', options.downloadDir,
    '--batch-size', String(options.batchSize),
    '--start-record', String(startRecord),
    '--status-file', files.statusFile,
    '--checkpoint-file', files.checkpointFile,
    '--control-file', files.controlFile,
    '--pid-file', files.pidFile
  ];

  const child = spawn(process.execPath, args, {
    cwd: PROJECT_ROOT,
    stdio: ['ignore', logFd, logFd]
  });

  const childPid = child.pid || null;
  let sentAction = null;
  let lastReportAt = 0;
  let previousProcessed = null;
  let previousProcessedAt = null;
  let lastStatusUpdatedAt = null;
  let lastWarningMarker = null;
  let forcedStopByStall = false;

  state.current_block_index = blockIndex;
  state.current_block = {
    key: block.key,
    from_year: block.fromYear,
    to_year: block.toYear,
    include_monthly_current_year: block.includeMonthlyCurrentYear,
    attempt,
    start_record: startRecord,
    pid: childPid,
    log_file: files.logFile,
    status_file: files.statusFile,
    checkpoint_file: files.checkpointFile
  };
  persistState(state, managerStatusFile);

  appendLine(reportLogFile, reportLine('block-start', {
    key: block.key,
    attempt,
    start_record: startRecord,
    pid: childPid
  }));

  while (child.exitCode === null && child.signalCode === null) {
    await sleep(DEFAULT_POLL_MS);

    const control = readManagerControl(managerControlFile);
    if (control && ['pause', 'stop'].includes(control.action) && sentAction !== control.action) {
      writeJson(files.controlFile, {
        action: control.action,
        reason: control.reason || `manager ${control.action}`
      });
      sentAction = control.action;
      appendLine(reportLogFile, reportLine('block-control-forwarded', {
        key: block.key,
        attempt,
        action: control.action,
        reason: control.reason || null
      }));
    }

    const now = Date.now();
    const status = readJson(files.statusFile) || {};
    const processed = Number(status.processed || 0);
    const updatedAt = clean(status.updated_at);
    const statusAgeMs = formatStatusAgeMs(status);
    const statusMarker = updatedAt || '__no_status_updated_at__';

    if (updatedAt && updatedAt !== lastStatusUpdatedAt) {
      lastStatusUpdatedAt = updatedAt;
      lastWarningMarker = null;
    }

    if (
      statusAgeMs !== null &&
      statusAgeMs >= options.warningThresholdMs &&
      lastWarningMarker !== statusMarker
    ) {
      appendLine(reportLogFile, reportLine('stall-warning', {
        key: block.key,
        attempt,
        status_age_seconds: Math.round(statusAgeMs / 1000),
        warning_threshold_seconds: Math.round(options.warningThresholdMs / 1000),
        status_updated_at: updatedAt
      }));
      lastWarningMarker = statusMarker;
    }

    if (statusAgeMs !== null && statusAgeMs >= options.stallThresholdMs && !forcedStopByStall) {
      writeJson(files.controlFile, {
        action: 'stop',
        reason: `stall detected: status stale for ${Math.round(statusAgeMs / 60000)} minutes`
      });
      forcedStopByStall = true;
      appendLine(reportLogFile, reportLine('stall-stop-triggered', {
        key: block.key,
        attempt,
        status_age_minutes: Math.round(statusAgeMs / 60000),
        stall_threshold_minutes: Math.round(options.stallThresholdMs / 60000)
      }));
    }

    if (now - lastReportAt >= options.reportEveryMs) {
      let rowsPerMinute = null;
      if (Number.isFinite(processed) && Number.isFinite(previousProcessed) && previousProcessedAt) {
        const deltaRows = processed - previousProcessed;
        const deltaMinutes = (now - previousProcessedAt) / 60000;
        if (deltaMinutes > 0) rowsPerMinute = Math.round((deltaRows / deltaMinutes) * 100) / 100;
      }
      previousProcessed = processed;
      previousProcessedAt = now;

      appendLine(reportLogFile, reportLine('health', {
        key: block.key,
        attempt,
        phase: status.phase || 'unknown',
        parsed_records: status.parsed_records || 0,
        processed,
        cursor: status.cursor || null,
        rows_per_minute: rowsPerMinute,
        status_updated_at: updatedAt,
        status_age_seconds: statusAgeMs === null ? null : Math.round(statusAgeMs / 1000),
        failures: {
          raw_failed: status.raw_failed || 0,
          canonical_failed: status.canonical_failed || 0,
          current_failed: status.current_failed || 0
        }
      }));

      state.last_report_at = nowIso();
      persistState(state, managerStatusFile);
      lastReportAt = now;
    }
  }

  fs.closeSync(logFd);

  const status = readJson(files.statusFile) || {};
  const checkpoint = readJson(files.checkpointFile) || {};
  const nextStartRecord = extractNextStartRecord(files.checkpointFile, startRecord);

  const exitCode = child.exitCode;
  const signalCode = child.signalCode;
  const phase = clean(status.phase) || (exitCode === 0 ? 'completed' : 'failed');

  appendLine(reportLogFile, reportLine('block-exit', {
    key: block.key,
    attempt,
    phase,
    exit_code: exitCode,
    signal: signalCode,
    next_start_record: nextStartRecord,
    parsed_records: status.parsed_records || checkpoint.parsed_records || 0,
    processed: status.processed || checkpoint.processed || 0
  }));

  if (phase === 'paused') {
    return { outcome: 'paused', nextStartRecord, phase, status, checkpoint };
  }
  if (phase === 'stopped') {
    return { outcome: 'stopped', nextStartRecord, phase, status, checkpoint };
  }
  if (phase === 'completed' || phase === 'completed_with_errors') {
    return { outcome: phase, nextStartRecord, phase, status, checkpoint };
  }

  return { outcome: 'failed', nextStartRecord, phase, status, checkpoint, exitCode, signalCode };
}

async function runManager(options) {
  if (!fs.existsSync(IMPORTER_SCRIPT)) {
    throw new Error(`Importer not found: ${IMPORTER_SCRIPT}`);
  }

  if (options.fresh) {
    unlinkIfExists(options.managerStatusFile);
    unlinkIfExists(options.managerControlFile);
    unlinkIfExists(options.reportLogFile);
  }

  const blocks = parseBlockSpec(options.blocks);
  const initialState = createInitialState(options, blocks);
  const existingState = options.resume ? readJson(options.managerStatusFile) : null;
  const state = options.resume ? mergeState(existingState, initialState) : initialState;

  state.phase = 'running';
  state.pid = process.pid;
  state.last_error = null;
  state.completed_at = null;

  writeJson(options.managerPidFile, {
    pid: process.pid,
    started_at: nowIso(),
    manager_status_file: options.managerStatusFile,
    manager_control_file: options.managerControlFile,
    report_log_file: options.reportLogFile,
    manager_log_file: options.managerLogFile
  });

  persistState(state, options.managerStatusFile);
  appendLine(options.reportLogFile, reportLine('manager-start', {
    pid: process.pid,
    policy: {
      report_every_minutes: options.reportEveryMs / 60000,
      warning_threshold_minutes: options.warningThresholdMs / 60000,
      stall_threshold_minutes: options.stallThresholdMs / 60000
    },
    blocks: state.blocks.map((block) => ({
      key: block.key,
      from_year: block.fromYear,
      to_year: block.toYear,
      include_monthly_current_year: block.includeMonthlyCurrentYear
    }))
  }));

  try {
    for (let i = 0; i < state.blocks.length; i += 1) {
      const block = state.blocks[i];
      if (['completed', 'completed_with_errors'].includes(block.status)) continue;

      block.status = 'running';
      block.started_at = block.started_at || nowIso();
      block.last_error = null;
      persistState(state, options.managerStatusFile);

      let startRecord = Math.max(1, Number(block.next_start_record || 1));
      startRecord = extractNextStartRecord(block.files.checkpointFile, startRecord);

      let finalResult = null;

      for (let attempt = 1; attempt <= options.maxRetries; attempt += 1) {
        block.attempts = Number(block.attempts || 0) + 1;
        persistState(state, options.managerStatusFile);

        const result = await runBlockAttempt({
          block,
          blockIndex: i,
          attempt,
          startRecord,
          options,
          state,
          managerStatusFile: options.managerStatusFile,
          managerControlFile: options.managerControlFile,
          reportLogFile: options.reportLogFile
        });

        block.next_start_record = result.nextStartRecord;
        block.last_run_id = clean(result.status?.run_id) || block.last_run_id;

        if (result.outcome === 'completed' || result.outcome === 'completed_with_errors') {
          block.status = result.outcome;
          block.finished_at = nowIso();
          block.last_error = null;
          finalResult = result;
          persistState(state, options.managerStatusFile);
          break;
        }

        if (result.outcome === 'paused') {
          block.status = 'paused';
          block.last_error = null;
          state.phase = 'paused';
          state.current_block = {
            ...(state.current_block || {}),
            pause_checkpoint_record: result.nextStartRecord
          };
          persistState(state, options.managerStatusFile);
          appendLine(options.reportLogFile, reportLine('manager-paused', {
            key: block.key,
            next_start_record: result.nextStartRecord
          }));
          unlinkIfExists(options.managerControlFile);
          return;
        }

        if (result.outcome === 'stopped') {
          block.status = 'stopped';
          block.last_error = null;
          state.phase = 'stopped';
          state.completed_at = nowIso();
          persistState(state, options.managerStatusFile);
          appendLine(options.reportLogFile, reportLine('manager-stopped', {
            key: block.key,
            next_start_record: result.nextStartRecord
          }));
          unlinkIfExists(options.managerControlFile);
          return;
        }

        block.status = 'retrying';
        block.last_error = clean(result.status?.error) || result.phase || 'block failed';
        persistState(state, options.managerStatusFile);

        if (attempt < options.maxRetries) {
          const waitMs = Math.min(60000, attempt * 10000);
          appendLine(options.reportLogFile, reportLine('block-retry', {
            key: block.key,
            attempt,
            wait_ms: waitMs,
            next_start_record: result.nextStartRecord,
            reason: block.last_error
          }));
          await sleep(waitMs);
          startRecord = Math.max(1, result.nextStartRecord);
          continue;
        }

        block.status = 'failed';
        block.finished_at = nowIso();
        state.phase = 'failed';
        state.last_error = `Block ${block.key} failed after ${options.maxRetries} attempts`;
        state.completed_at = nowIso();
        persistState(state, options.managerStatusFile);
        appendLine(options.reportLogFile, reportLine('manager-failed', {
          key: block.key,
          attempts: options.maxRetries,
          error: block.last_error
        }));
        return;
      }

      if (!finalResult && block.status === 'retrying') {
        block.status = 'failed';
        block.finished_at = nowIso();
        state.phase = 'failed';
        state.last_error = `Block ${block.key} ended in retry state`;
        state.completed_at = nowIso();
        persistState(state, options.managerStatusFile);
        return;
      }
    }

    state.phase = 'completed';
    state.completed_at = nowIso();
    state.current_block_index = null;
    state.current_block = null;
    persistState(state, options.managerStatusFile);
    appendLine(options.reportLogFile, reportLine('manager-completed', {
      completed_at: state.completed_at
    }));
  } finally {
    unlinkIfExists(options.managerControlFile);
    unlinkIfExists(options.managerPidFile);
  }
}

function getManagerPid(managerPidFile) {
  const payload = readJson(managerPidFile);
  if (!payload) return null;
  const pid = Number(payload.pid || 0);
  if (!Number.isFinite(pid) || pid <= 0) return null;
  return pid;
}

function managerIsRunning(managerPidFile) {
  const pid = getManagerPid(managerPidFile);
  if (!pid) return false;
  return isProcessAlive(pid);
}

function startDetached(args, options) {
  if (managerIsRunning(options.managerPidFile)) {
    const pid = getManagerPid(options.managerPidFile);
    console.log(`PLACSP block manager already running (pid ${pid})`);
    return;
  }

  const forward = serializeForwardArgs(args).filter((token) => token !== '--fresh');
  const outFd = fs.openSync(options.managerLogFile, 'a');
  const child = spawn(process.execPath, [__filename, 'run', ...forward], {
    detached: true,
    cwd: PROJECT_ROOT,
    stdio: ['ignore', outFd, outFd]
  });
  child.unref();

  console.log(`PLACSP block manager started (launcher pid ${child.pid || 'unknown'})`);
  console.log(`Manager log: ${options.managerLogFile}`);
  console.log(`Manager status: ${options.managerStatusFile}`);
  console.log(`Report log: ${options.reportLogFile}`);
}

async function stopAndWait(options, reason) {
  if (!managerIsRunning(options.managerPidFile)) {
    console.log('PLACSP block manager is not running.');
    return;
  }

  writeManagerControl(options.managerControlFile, 'stop', reason || 'restart requested');

  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    if (!managerIsRunning(options.managerPidFile)) {
      unlinkIfExists(options.managerControlFile);
      console.log('PLACSP block manager stopped.');
      return;
    }
    await sleep(2000);
  }

  const pid = getManagerPid(options.managerPidFile);
  throw new Error(`Timed out waiting for manager to stop (pid ${pid || 'unknown'})`);
}

function printMonitorSnapshot(options) {
  const state = readJson(options.managerStatusFile);
  const pid = getManagerPid(options.managerPidFile);
  const running = pid ? isProcessAlive(pid) : false;

  if (!state) {
    console.log('No manager state found yet.');
    console.log(`Expected state file: ${options.managerStatusFile}`);
    return;
  }

  const current = state.current_block;
  const block = Number.isFinite(Number(state.current_block_index))
    ? state.blocks?.[Number(state.current_block_index)]
    : null;

  let blockStatus = null;
  if (current?.status_file) {
    blockStatus = readJson(current.status_file);
  }

  const summary = {
    phase: state.phase,
    running,
    pid,
    started_at: state.started_at,
    updated_at: state.updated_at,
    completed_at: state.completed_at,
    current_block: current?.key || null,
    current_block_attempt: current?.attempt || null,
    current_block_start_record: current?.start_record || null,
    current_block_processed: blockStatus?.processed ?? null,
    current_block_parsed_records: blockStatus?.parsed_records ?? null,
    current_block_phase: blockStatus?.phase || null,
    last_report_at: state.last_report_at || null,
    last_error: state.last_error || null,
    blocks: Array.isArray(state.blocks)
      ? state.blocks.map((item) => ({
          key: item.key,
          status: item.status,
          attempts: item.attempts,
          next_start_record: item.next_start_record,
          finished_at: item.finished_at
        }))
      : []
  };

  console.log(JSON.stringify(summary, null, 2));
}

async function monitorLoop(options, watchSeconds) {
  while (true) {
    printMonitorSnapshot(options);
    await sleep(watchSeconds * 1000);
    console.log('');
  }
}

function parseOptions(args) {
  const reportEveryMinutes = Math.max(
    1,
    parsePositiveInt(args['report-every-minutes'], DEFAULT_REPORT_INTERVAL_MS / 60000)
  );
  const warningThresholdMinutes = Math.max(
    1,
    parsePositiveInt(args['warning-threshold-minutes'], DEFAULT_WARNING_THRESHOLD_MS / 60000)
  );
  const parsedStallThresholdMinutes = parsePositiveInt(
    args['stall-threshold-minutes'],
    DEFAULT_STALL_THRESHOLD_MS / 60000
  );
  const stallThresholdMinutes = Math.max(warningThresholdMinutes + 1, parsedStallThresholdMinutes);

  return {
    apiBase: clean(args['api-base']) || DEFAULT_API_BASE,
    appId: clean(args['app-id']) || DEFAULT_APP_ID,
    tenantId: clean(args['tenant-id']) || DEFAULT_TENANT_ID,
    downloadDir: clean(args['download-dir']) || DEFAULT_DOWNLOAD_DIR,
    batchSize: Math.max(20, parsePositiveInt(args['batch-size'], DEFAULT_BATCH_SIZE)),
    blocks: clean(args.blocks) || null,
    maxRetries: parsePositiveInt(args['max-retries'], DEFAULT_MAX_RETRIES),
    reportEveryMs: reportEveryMinutes * 60000,
    warningThresholdMs: warningThresholdMinutes * 60000,
    stallThresholdMs: stallThresholdMinutes * 60000,
    managerStatusFile: clean(args['manager-status-file']) || DEFAULT_MANAGER_STATUS_FILE,
    managerControlFile: clean(args['manager-control-file']) || DEFAULT_MANAGER_CONTROL_FILE,
    managerPidFile: clean(args['manager-pid-file']) || DEFAULT_MANAGER_PID_FILE,
    managerLogFile: clean(args['manager-log-file']) || DEFAULT_MANAGER_LOG_FILE,
    reportLogFile: clean(args['report-log-file']) || DEFAULT_REPORT_LOG_FILE,
    blockPrefix: clean(args['block-prefix']) || DEFAULT_BLOCK_FILE_PREFIX,
    resume: parseBoolean(args.resume, true),
    fresh: parseBoolean(args.fresh, false)
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const command = clean(args._[0]) || 'monitor';

  if (parseBoolean(args.help, false) || parseBoolean(args.h, false)) {
    printHelp();
    return;
  }

  const options = parseOptions(args);

  if (command === 'start') {
    startDetached(args, options);
    return;
  }

  if (command === 'run') {
    await runManager(options);
    return;
  }

  if (command === 'monitor') {
    const watch = parseBoolean(args.watch, false);
    const watchSeconds = Math.max(2, parsePositiveInt(args['watch-seconds'], 15));
    if (!watch) {
      printMonitorSnapshot(options);
      return;
    }
    await monitorLoop(options, watchSeconds);
    return;
  }

  if (command === 'pause') {
    writeManagerControl(options.managerControlFile, 'pause', clean(args.reason) || 'manual pause');
    console.log(`Pause requested via ${options.managerControlFile}`);
    return;
  }

  if (command === 'stop') {
    writeManagerControl(options.managerControlFile, 'stop', clean(args.reason) || 'manual stop');
    console.log(`Stop requested via ${options.managerControlFile}`);
    return;
  }

  if (command === 'restart') {
    await stopAndWait(options, clean(args.reason) || 'manual restart');
    startDetached(args, options);
    return;
  }

  throw new Error(`Unsupported command: ${command}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
