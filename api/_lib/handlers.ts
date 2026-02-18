import {
  getCurrentUser,
  getTenantFromHeader,
  hasRole,
  isPrivileged,
  requireCreator,
  requirePrivileged,
  requireTenantAccess,
  type CurrentUser
} from './auth.js';
import { createHash } from 'node:crypto';
import { readJsonBody, type RequestLike } from './http.js';
import { getServerSupabase } from './supabase.js';
import type { ApiDatabase, Json } from './db.types.js';

const CONNECTOR_MAP: Record<string, { key: string; displayName: string }> = {
  BOAMP_FR: { key: 'BOAMP_FR', displayName: 'BOAMP France' },
  TED: { key: 'TED', displayName: 'TED (EU)' },
  TED_IE: { key: 'TED_IE', displayName: 'TED Ireland' },
  TED_FR: { key: 'TED_FR', displayName: 'TED France' },
  ETENDERS_IE: { key: 'ETENDERS_IE', displayName: 'eTenders Ireland' },
  PLACSP_ES: { key: 'PLACSP_ES', displayName: 'PLACSP Spain' },
  PLACSP_ES_INCREMENTAL: { key: 'PLACSP_ES', displayName: 'PLACSP Spain' }
};

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;

type SupportAccessGrantRow = ApiDatabase['public']['Tables']['support_access_grants']['Row'];
type SupportAccessAuditRow = ApiDatabase['public']['Tables']['support_access_audit']['Row'];
type TenantRow = ApiDatabase['public']['Tables']['tenants']['Row'];
type UserRow = ApiDatabase['public']['Tables']['users']['Row'];
type UserRoleRow = ApiDatabase['public']['Tables']['user_roles']['Row'];
type ConnectorConfigRow = ApiDatabase['public']['Tables']['ConnectorConfig']['Row'];
type ConnectorRunRow = ApiDatabase['public']['Tables']['ConnectorRuns']['Row'];

function badRequest(message: string) {
  return Object.assign(new Error(message), { status: 400 });
}

function conflict(message: string) {
  return Object.assign(new Error(message), { status: 409 });
}

function toDisplay(connectorKey: string) {
  if (CONNECTOR_MAP[connectorKey]) return CONNECTOR_MAP[connectorKey];

  const upperKey = connectorKey.toUpperCase();
  const keyPrefix = upperKey.split(':')[0];
  if (CONNECTOR_MAP[keyPrefix]) {
    return { key: connectorKey, displayName: CONNECTOR_MAP[keyPrefix].displayName };
  }

  if (keyPrefix.endsWith('_INCREMENTAL')) {
    const baseKey = keyPrefix.replace(/_INCREMENTAL$/, '');
    if (CONNECTOR_MAP[baseKey]) {
      return { key: connectorKey, displayName: CONNECTOR_MAP[baseKey].displayName };
    }
  }

  return { key: connectorKey, displayName: connectorKey };
}

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
}

function isExpired(expiresAt: unknown) {
  if (!expiresAt) return false;
  const timestamp = new Date(String(expiresAt)).getTime();
  if (Number.isNaN(timestamp)) return false;
  return timestamp <= Date.now();
}

function sanitizeDuration(value: unknown) {
  const n = Number(value || 60);
  if (Number.isNaN(n) || n <= 0) return 60;
  return Math.min(Math.max(Math.round(n), 5), 24 * 60);
}

function normalizeLimit(value: unknown, fallback = 50, max = 200) {
  const n = Number(value || fallback);
  if (Number.isNaN(n) || n <= 0) return fallback;
  return Math.min(Math.round(n), max);
}

function slugifyTenantId(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_')
    .slice(0, 40);
}

async function writeSupportAudit(params: {
  tenantId: string;
  actor: Pick<CurrentUser, 'userId' | 'email'>;
  action: string;
  reason?: string | null;
  metadata?: Json | null;
}) {
  const { tenantId, actor, action, reason, metadata } = params;
  const supabase = getServerSupabase();

  const { error } = await supabase
    .from('support_access_audit')
    .insert({
      id: makeId('sa_audit'),
      tenant_id: tenantId,
      actor_user_id: actor.userId || 'unknown',
      actor_email: actor.email || null,
      action,
      reason: reason || null,
      metadata_json: metadata ?? null,
      created_at: new Date().toISOString()
    });

  if (error) {
    throw Object.assign(new Error(error.message), { status: 500 });
  }
}

async function getLatestSupportGrant(tenantId: string) {
  const supabase = getServerSupabase();
  const { data, error } = await supabase
    .from('support_access_grants')
    .select('*')
    .eq('tenant_id', tenantId)
    .order('created_at', { ascending: false })
    .limit(1);

  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  return Array.isArray(data) && data.length ? (data[0] as SupportAccessGrantRow) : null;
}

async function getActiveSupportGrant(tenantId: string) {
  const supabase = getServerSupabase();
  const { data, error } = await supabase
    .from('support_access_grants')
    .select('*')
    .eq('tenant_id', tenantId)
    .eq('enabled', true)
    .order('created_at', { ascending: false })
    .limit(50);

  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  const grantRows = (Array.isArray(data) ? data : []) as SupportAccessGrantRow[];
  const active = grantRows.find((row: SupportAccessGrantRow) => !row.revoked_at && !isExpired(row.expires_at));
  return active || null;
}

async function computeSupportStatus(tenantId: string, actor?: Pick<CurrentUser, 'userId' | 'email'>) {
  const supabase = getServerSupabase();
  const latest = await getLatestSupportGrant(tenantId);

  if (!latest) {
    return {
      enabled: false,
      expiresAt: null,
      enabledBy: null,
      reason: null,
      revokedAt: null
    };
  }

  const expired = Boolean(latest.enabled) && !latest.revoked_at && isExpired(latest.expires_at);
  if (expired) {
    const { error: updateError } = await supabase
      .from('support_access_grants')
      .update({ enabled: false })
      .eq('id', String(latest.id || ''))
      .eq('tenant_id', tenantId);

    if (updateError) throw Object.assign(new Error(updateError.message), { status: 500 });

    if (actor) {
      await writeSupportAudit({
        tenantId,
        actor,
        action: 'EXPIRED',
        reason: 'Support access grant expired automatically',
        metadata: { grantId: latest.id || null, expiresAt: latest.expires_at || null }
      });
    }

    return {
      enabled: false,
      expiresAt: latest.expires_at || null,
      enabledBy: latest.enabled_by_user_id || null,
      reason: latest.reason || null,
      revokedAt: latest.revoked_at || null
    };
  }

  return {
    enabled: Boolean(latest.enabled) && !latest.revoked_at && !isExpired(latest.expires_at),
    expiresAt: latest.expires_at || null,
    enabledBy: latest.enabled_by_user_id || null,
    reason: latest.reason || null,
    revokedAt: latest.revoked_at || null
  };
}

async function requireTenantScopedPrivilegedUser(req: RequestLike) {
  const user = await getCurrentUser(req);
  const tenantId = getTenantFromHeader(req);
  requireTenantAccess(user, tenantId);
  requirePrivileged(user);
  return { user, tenantId };
}

async function requireTenantScopedUser(req: RequestLike) {
  const user = await getCurrentUser(req);
  const tenantId = getTenantFromHeader(req);
  requireTenantAccess(user, tenantId);
  return { user, tenantId };
}

async function requireTenantScopedAdmin(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedPrivilegedUser(req);
  if (!hasRole(user, 'admin')) {
    throw Object.assign(new Error('Admin role required'), { status: 403 });
  }
  return { user, tenantId };
}

function toNumber(value: unknown) {
  const n = Number(value || 0);
  if (Number.isNaN(n) || !Number.isFinite(n)) return 0;
  return Math.max(0, Math.round(n));
}

function normalizeMetrics(value: unknown) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {} as Record<string, unknown>;
  }
  return value as Record<string, unknown>;
}

function parseSearchCpvCodes(value: unknown) {
  const seen = new Set<string>();
  const output: string[] = [];
  const matches = String(value || '').match(/\d{2,8}/g) || [];
  for (const raw of matches) {
    const code = raw.slice(0, 8);
    if (!code || seen.has(code)) continue;
    seen.add(code);
    output.push(code);
  }
  return output;
}

function parseTenderCpvCodes(value: unknown) {
  if (Array.isArray(value)) {
    return parseSearchCpvCodes(value.join(','));
  }

  const seen = new Set<string>();
  const output: string[] = [];
  const parts = String(value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);

  for (const part of parts) {
    const code = part.replace(/\D/g, '').slice(0, 8);
    if (code.length < 2 || seen.has(code)) continue;
    seen.add(code);
    output.push(code);
  }

  return output;
}

function parseTenderSources(value: unknown, fallback?: unknown) {
  const seen = new Set<string>();
  const output: string[] = [];

  const addSource = (raw: unknown) => {
    const source = String(raw || '').trim().toUpperCase();
    if (!source || seen.has(source)) return;
    seen.add(source);
    output.push(source);
  };

  if (Array.isArray(value)) {
    for (const item of value) addSource(item);
  } else if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
          for (const item of parsed) addSource(item);
        } else {
          addSource(trimmed);
        }
      } catch {
        trimmed
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean)
          .forEach((item) => addSource(item));
      }
    } else {
      trimmed
        .split(',')
        .map((item) => item.replace(/[{}"]/g, '').trim())
        .filter(Boolean)
        .forEach((item) => addSource(item));
    }
  }

  addSource(fallback);
  return output;
}

const CLOSED_STATUS_CODES = new Set([
  'RES', // resolved/result
  'ADJ', // adjudicated
  'CAN', // cancelled
  'DES'  // deserted/void
]);

function parseTenderDeadlineDate(value: unknown) {
  const raw = String(value || '').trim();
  if (!raw) return null;

  // Date-only deadlines should remain valid until the end of that date.
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const parsed = new Date(`${raw}T23:59:59.999Z`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function isClosedTenderByStatus(tender: Record<string, unknown>) {
  const statusCode = String(tender.status_code || '').trim().toUpperCase();
  const noticeType = String(tender.notice_type || '').trim().toLowerCase();
  if (noticeType === 'award') return true;
  return CLOSED_STATUS_CODES.has(statusCode);
}

function normalizeSearchFilters(raw: Record<string, unknown>) {
  return {
    keyword: String(raw.keyword || '').trim(),
    country: String(raw.country || 'all').trim() || 'all',
    source: String(raw.source || 'all').trim() || 'all',
    buyerSearch: String(raw.buyerSearch || '').trim(),
    cpvSearchCodes: Array.isArray(raw.cpvSearchCodes)
      ? raw.cpvSearchCodes.map((v) => String(v || '').trim()).filter(Boolean).slice(0, 8)
      : parseSearchCpvCodes(raw.cpvSearchCodes || ''),
    deadlineWithin: String(raw.deadlineWithin || 'all').trim() || 'all',
    industry: String(raw.industry || 'all').trim() || 'all',
    institutionType: String(raw.institutionType || 'all').trim() || 'all',
    lastTendered: String(raw.lastTendered || 'all').trim() || 'all'
  };
}

type SearchFilters = ReturnType<typeof normalizeSearchFilters>;

function parseSearchWindowDays(value: string) {
  const days = Number.parseInt(value, 10);
  if (Number.isNaN(days) || days <= 0) return null;
  return days;
}

function hashTelemetryValue(value: string) {
  return createHash('sha256').update(value).digest('hex');
}

function sanitizeFiltersForTelemetry(tenantId: string, filters: SearchFilters) {
  const normalizedKeyword = String(filters.keyword || '').trim().toLowerCase();
  const normalizedBuyer = String(filters.buyerSearch || '').trim().toLowerCase();
  const cpvCodes = Array.isArray(filters.cpvSearchCodes) ? filters.cpvSearchCodes.slice(0, 8) : [];

  return {
    country: filters.country,
    source: filters.source,
    deadline_within: filters.deadlineWithin,
    industry: filters.industry,
    institution_type: filters.institutionType,
    last_tendered: filters.lastTendered,
    cpv_search_codes: cpvCodes,
    cpv_count: cpvCodes.length,
    has_keyword: Boolean(normalizedKeyword),
    has_buyer_search: Boolean(normalizedBuyer),
    keyword_hash: normalizedKeyword ? hashTelemetryValue(`${tenantId}:${normalizedKeyword}`) : null,
    buyer_search_hash: normalizedBuyer ? hashTelemetryValue(`${tenantId}:${normalizedBuyer}`) : null
  };
}

async function recordZeroResultSearchTelemetry(params: {
  supabase: any;
  tenantId: string;
  userId: string;
  filters: SearchFilters;
  meta: Record<string, unknown>;
}) {
  const { supabase, tenantId, userId, filters, meta } = params;
  const filtersTelemetry = sanitizeFiltersForTelemetry(tenantId, filters);
  const queryHash = hashTelemetryValue(JSON.stringify(filtersTelemetry));

  const payload = {
    tenant_id: tenantId,
    user_id: userId || null,
    search_engine: String(meta.search_engine || 'unknown'),
    latency_ms: toNumber(meta.elapsed_ms),
    scanned_rows: toNumber(meta.scanned_rows),
    candidate_rows: toNumber(meta.candidate_rows),
    returned_rows: 0,
    query_hash: queryHash,
    filters_json: filtersTelemetry,
    metadata_json: {
      limit: toNumber(meta.limit),
      scan_limit: toNumber(meta.scan_limit),
      hit_scan_limit: Boolean(meta.hit_scan_limit),
      rpc_error: String(meta.rpc_error || '').slice(0, 300)
    }
  };

  const { error } = await supabase
    .from('search_zero_results_log')
    .insert(payload);

  if (error) {
    console.warn('[search-zero-results-log] insert failed', error.message);
    return false;
  }

  return true;
}

function normalizeTenderForSearch(row: unknown) {
  if (!row || typeof row !== 'object') return {};
  const base = row as Record<string, unknown>;
  const nestedData = base.data;
  const data =
    nestedData && typeof nestedData === 'object' && !Array.isArray(nestedData)
      ? (nestedData as Record<string, unknown>)
      : {};

  const merged: Record<string, unknown> = { ...data, ...base };
  const tenderId = String(base.tender_id || base.canonical_id || data.tender_id || data.canonical_id || data.id || '').trim();

  if (tenderId) {
    if (!merged.id) merged.id = tenderId;
    if (!merged.tender_uid) merged.tender_uid = tenderId;
    if (!merged.canonical_id) merged.canonical_id = tenderId;
  }

  if (!merged.title && data.tender_name) {
    merged.title = data.tender_name;
  }

  if (!merged.description) {
    const normalizedJson =
      base.normalized_json && typeof base.normalized_json === 'object' && !Array.isArray(base.normalized_json)
        ? (base.normalized_json as Record<string, unknown>)
        : {};
    merged.description =
      data.description ||
      data.summary ||
      data.short_description ||
      normalizedJson.description ||
      normalizedJson.summary ||
      normalizedJson.short_description ||
      null;
  }

  if (!merged.buyer_name && data.contracting_authority) {
    merged.buyer_name = data.contracting_authority;
  }
  if (!merged.buyer_name && base.buyer_name_raw) {
    merged.buyer_name = base.buyer_name_raw;
  }

  if (!merged.country) {
    merged.country = data.country || data.country_code || data.country_iso || base.country || null;
  }

  const cpvCodes = merged.cpv_codes;
  if (Array.isArray(cpvCodes)) {
    merged.cpv_codes = cpvCodes.join(',');
  } else if (cpvCodes === null || cpvCodes === undefined) {
    merged.cpv_codes = '';
  }

  if (!merged.publication_date && typeof base.published_at === 'string') {
    merged.publication_date = base.published_at;
  }
  if (!merged.publication_date && typeof base.publication_date === 'string') {
    merged.publication_date = base.publication_date;
  }

  if (!merged.first_seen_at) {
    merged.first_seen_at = data.first_seen_at || base.published_at || base.updated_at || null;
  }

  if (!merged.deadline_date) {
    merged.deadline_date = data.deadline_date || data.event_deadline_date || data.submission_deadline || base.deadline_date || null;
  }

  if (merged.is_open === undefined || merged.is_open === null) {
    const status = String(merged.status || merged.status_code || '').trim().toLowerCase();
    const deadline = parseTenderDeadlineDate(merged.deadline_date);
    const isClosed = ['closed', 'cancelled', 'canceled', 'awarded', 'completed', 'unsuccessful'].includes(status);
    merged.is_open = !isClosed && (!deadline || deadline.getTime() >= Date.now());
  }

  if (!merged.url && data.source_url) {
    merged.url = data.source_url;
  }
  if (!merged.url && base.source_url) {
    merged.url = base.source_url;
  }

  if (!merged.source && base.source) {
    merged.source = base.source;
  }

  const sourceSummary = parseTenderSources(
    merged.verification_sources || data.verification_sources || base.verification_sources,
    merged.source || base.source
  );
  if (sourceSummary.length > 0) {
    merged.verification_sources = sourceSummary;
  }

  return merged;
}

function getTenderPublicationDate(tender: Record<string, unknown>) {
  return String(tender.publication_date || tender.published_at || tender.first_seen_at || tender.updated_at || '');
}

function getTenderFirstSeenDate(tender: Record<string, unknown>) {
  return String(tender.first_seen_at || tender.published_at || tender.publication_date || tender.updated_at || '');
}

function buildTenderRelevanceScore(tender: Record<string, unknown>, filters: SearchFilters) {
  const now = Date.now();
  const title = String(tender.title || '').toLowerCase();
  const description = String(tender.description || '').toLowerCase();
  const buyerName = String(tender.buyer_name || '').toLowerCase();
  const cpvCodes = parseTenderCpvCodes(tender.cpv_codes);
  const source = String(tender.source || '').trim().toUpperCase();
  const country = String(tender.country || '').trim().toUpperCase();

  let score = 0;

  if (filters.keyword) {
    const kw = filters.keyword.toLowerCase();
    if (title === kw) {
      score += 60;
    } else if (title.startsWith(kw)) {
      score += 46;
    } else if (title.includes(kw)) {
      score += 34;
    }

    if (buyerName.includes(kw)) {
      score += 20;
    }
    if (description.includes(kw)) {
      score += 12;
    }
  }

  if (filters.buyerSearch && buyerName.includes(filters.buyerSearch.toLowerCase())) {
    score += 15;
  }

  if (filters.cpvSearchCodes.length > 0 && cpvCodes.length > 0) {
    const prefixDepth = filters.cpvSearchCodes.reduce((best, wanted) => {
      const depth = cpvCodes.reduce((localBest, code) => {
        if (!code.startsWith(wanted)) return localBest;
        return Math.max(localBest, wanted.length);
      }, 0);
      return Math.max(best, depth);
    }, 0);

    if (prefixDepth > 0) {
      score += 14 + Math.min(prefixDepth, 8);
    }
  }

  const deadline = parseTenderDeadlineDate(tender.deadline_date);
  if (deadline && deadline.getTime() >= now) {
    const daysUntilDeadline = Math.floor((deadline.getTime() - now) / (24 * 60 * 60 * 1000));
    score += Math.max(0, 20 - Math.min(daysUntilDeadline, 20));
  } else if (deadline && deadline.getTime() < now) {
    score -= 8;
  }

  const publicationValue = getTenderPublicationDate(tender);
  if (publicationValue) {
    const publication = new Date(publicationValue);
    if (!Number.isNaN(publication.getTime())) {
      const daysOld = Math.max(0, Math.floor((now - publication.getTime()) / (24 * 60 * 60 * 1000)));
      score += Math.max(0, 15 - Math.floor(daysOld / 7));
    }
  }

  if (filters.source !== 'all' && source === String(filters.source).toUpperCase()) score += 6;
  if (filters.country !== 'all' && country === String(filters.country).toUpperCase()) score += 5;

  // Spain + PLACSP defaults get a small recency tie-break to keep local open opportunities on top.
  if (source === 'PLACSP_ES' && country === 'ES') score += 3;

  return score;
}

function sortTendersByRelevance(tenders: Record<string, unknown>[], filters: SearchFilters) {
  const scored = tenders.map((tender, index) => ({
    tender,
    index,
    score: buildTenderRelevanceScore(tender, filters),
    publication: new Date(getTenderPublicationDate(tender)).getTime() || 0
  }));

  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (b.publication !== a.publication) return b.publication - a.publication;
    return a.index - b.index;
  });

  return scored.map((entry) => entry.tender);
}

function isTenderMatch(tender: Record<string, unknown>, filters: SearchFilters) {
  const country = String(tender.country || '').trim().toUpperCase();
  const sources = parseTenderSources(tender.verification_sources, tender.source);
  const title = String(tender.title || '').toLowerCase();
  const description = String(tender.description || '').toLowerCase();
  const buyerName = String(tender.buyer_name || '').toLowerCase();

  if (filters.keyword) {
    const kw = filters.keyword.toLowerCase();
    if (!title.includes(kw) && !buyerName.includes(kw) && !description.includes(kw)) return false;
  }

  if (filters.country !== 'all' && country !== String(filters.country).toUpperCase()) {
    return false;
  }

  if (filters.source !== 'all') {
    const wanted = String(filters.source || '').trim().toUpperCase();
    if (!sources.includes(wanted)) return false;
  }

  if (filters.source !== 'all' && !sources.length) {
    return false;
  }

  if (filters.buyerSearch && !buyerName.includes(filters.buyerSearch.toLowerCase())) {
    return false;
  }

  if (filters.cpvSearchCodes.length > 0) {
    const tenderCodes = parseTenderCpvCodes(tender.cpv_codes);
    if (!tenderCodes.length) return false;
    const hasMatch = filters.cpvSearchCodes.some((wanted) =>
      tenderCodes.some((code) => code.startsWith(wanted))
    );
    if (!hasMatch) return false;
  }

  if (filters.deadlineWithin !== 'all') {
    const days = Number.parseInt(filters.deadlineWithin, 10);
    if (!Number.isNaN(days) && days > 0) {
      const now = new Date();
      const futureDate = new Date(now.getTime() + days * 24 * 60 * 60 * 1000);

      if (isClosedTenderByStatus(tender)) return false;

      const deadline = parseTenderDeadlineDate(tender.deadline_date);
      if (!deadline) return false;
      if (deadline < now || deadline > futureDate) return false;
    }
  }

  if (filters.industry !== 'all') {
    const cpv = String(tender.cpv_codes || '').toLowerCase();
    if (!cpv) return false;
    if (filters.industry === 'construction' && !cpv.includes('45')) return false;
    if (filters.industry === 'it' && !cpv.includes('72') && !cpv.includes('48')) return false;
    if (filters.industry === 'health' && !cpv.includes('33') && !cpv.includes('85')) return false;
    if (filters.industry === 'transport' && !cpv.includes('60') && !cpv.includes('34')) return false;
    if (filters.industry === 'consulting' && !cpv.includes('79') && !cpv.includes('71')) return false;
    if (filters.industry === 'food' && !cpv.includes('15') && !cpv.includes('55')) return false;
  }

  if (filters.institutionType !== 'all') {
    if (!buyerName) return false;
    if (filters.institutionType === 'ministry' && !buyerName.includes('ministry') && !buyerName.includes('ministère') && !buyerName.includes('minister')) return false;
    if (filters.institutionType === 'local' && !buyerName.includes('council') && !buyerName.includes('city') && !buyerName.includes('county') && !buyerName.includes('commune') && !buyerName.includes('ville')) return false;
    if (filters.institutionType === 'health' && !buyerName.includes('health') && !buyerName.includes('hospital') && !buyerName.includes('santé') && !buyerName.includes('hôpital')) return false;
    if (filters.institutionType === 'education' && !buyerName.includes('university') && !buyerName.includes('college') && !buyerName.includes('school') && !buyerName.includes('université') && !buyerName.includes('école')) return false;
    if (filters.institutionType === 'transport' && !buyerName.includes('transport') && !buyerName.includes('railway') && !buyerName.includes('road')) return false;
  }

  if (filters.lastTendered !== 'all') {
    const days = Number.parseInt(filters.lastTendered, 10);
    if (!Number.isNaN(days) && days > 0) {
      const cutoffDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
      const dateValue = days === 1
        ? getTenderFirstSeenDate(tender)
        : getTenderPublicationDate(tender);
      if (!dateValue) return false;
      const date = new Date(dateValue);
      if (Number.isNaN(date.getTime()) || date < cutoffDate) return false;
    }
  }

  return true;
}

export async function searchTenders(req: RequestLike) {
  const startedAtMs = Date.now();
  const { user, tenantId } = await requireTenantScopedUser(req);
  const supabase = getServerSupabase() as any;
  const body = readJsonBody<Record<string, unknown>>(req);
  const filters = normalizeSearchFilters(body);
  const limit = normalizeLimit(body.limit, 200, 500);
  const pageSize = 500;
  const scanLimit = filters.lastTendered !== 'all' ? 20000 : 8000;

  const rpcArgs = {
    p_tenant_id: tenantId,
    p_limit: limit,
    p_keyword: filters.keyword || null,
    p_country: filters.country === 'all' ? null : filters.country,
    p_source: filters.source === 'all' ? null : filters.source,
    p_buyer_search: filters.buyerSearch || null,
    p_cpv_search_codes: filters.cpvSearchCodes.length > 0 ? filters.cpvSearchCodes : null,
    p_deadline_within: parseSearchWindowDays(filters.deadlineWithin),
    p_industry: filters.industry === 'all' ? null : filters.industry,
    p_institution_type: filters.institutionType === 'all' ? null : filters.institutionType,
    p_last_tendered: parseSearchWindowDays(filters.lastTendered)
  };

  const rpcResult = await supabase.rpc('search_tenders_ranked', rpcArgs);
  if (!rpcResult.error) {
    const rpcRows = Array.isArray(rpcResult.data) ? rpcResult.data : [];
    const items = rpcRows.map((row) => normalizeTenderForSearch(row as Record<string, unknown>));
    const meta: Record<string, unknown> = {
      tenant_id: tenantId,
      returned_rows: items.length,
      limit,
      search_engine: 'rpc_ranked',
      elapsed_ms: Date.now() - startedAtMs
    };
    if (items.length === 0) {
      meta.zero_result_logged = await recordZeroResultSearchTelemetry({
        supabase,
        tenantId,
        userId: user.userId,
        filters,
        meta
      });
    }
    return {
      success: true,
      items,
      meta
    };
  }

  const rpcErrorMessage = String(rpcResult.error?.message || '');

  const days = parseSearchWindowDays(filters.lastTendered);
  const publishedCutoff = days
    ? new Date(Date.now() - days * 24 * 60 * 60 * 1000)
    : null;

  const results: Record<string, unknown>[] = [];
  const candidateLimit = Math.min(scanLimit, Math.max(limit * 8, limit));
  let skip = 0;
  let scannedRows = 0;

  while (scannedRows < scanLimit && results.length < candidateLimit) {
    let qb = supabase
      .from('notices_search_current')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('last_seen_at', { ascending: false })
      .range(skip, skip + pageSize - 1);

    if (filters.source !== 'all') {
      const sourceValue = String(filters.source || '').trim().toUpperCase();
      qb = qb.or(`source.eq.${sourceValue},verification_sources.cs.{${sourceValue}}`);
    }

    if (publishedCutoff) {
      qb = qb.gte('last_seen_at', publishedCutoff.toISOString());
    }

    const { data, error } = await qb;
    if (error) throw Object.assign(new Error(error.message), { status: 500 });

    const rows = Array.isArray(data) ? data : [];
    if (!rows.length) break;

    scannedRows += rows.length;
    skip += rows.length;

    for (const row of rows) {
      const tender = normalizeTenderForSearch(row);
      if (!isTenderMatch(tender, filters)) continue;
      results.push(tender);
      if (results.length >= limit) break;
    }

    if (rows.length < pageSize) break;
  }

  const rankedResults = sortTendersByRelevance(results, filters).slice(0, limit);
  const meta: Record<string, unknown> = {
    tenant_id: tenantId,
    scanned_rows: scannedRows,
    candidate_rows: results.length,
    returned_rows: rankedResults.length,
    limit,
    scan_limit: scanLimit,
    hit_scan_limit: scannedRows >= scanLimit && results.length < candidateLimit,
    search_engine: 'scan_ranked_fallback',
    rpc_error: rpcResult.error.message,
    elapsed_ms: Date.now() - startedAtMs
  };
  if (rankedResults.length === 0) {
    meta.zero_result_logged = await recordZeroResultSearchTelemetry({
      supabase,
      tenantId,
      userId: user.userId,
      filters,
      meta
    });
  }

  return {
    success: true,
    items: rankedResults,
    meta
  };
}

export async function getMyProfile(req: RequestLike) {
  const user = await getCurrentUser(req);
  return {
    email: user.email,
    tenant_id: user.tenantId,
    roles: user.roles
  };
}

export async function getDashboardStats(req: RequestLike) {
  const { tenantId } = await requireTenantScopedUser(req);
  const supabase = getServerSupabase() as any;

  const { data, error } = await supabase.rpc('get_dashboard_stats', {
    p_tenant_id: tenantId
  });

  if (error) throw Object.assign(new Error(error.message), { status: 500 });

  const row = Array.isArray(data) && data.length ? data[0] : null;
  let openTendersNow = 0;
  const openNowResult = await supabase.rpc('count_open_tenders_now', {
    p_tenant_id: tenantId
  });
  if (openNowResult.error) {
    throw Object.assign(new Error(openNowResult.error.message), { status: 500 });
  }
  openTendersNow = toNumber(openNowResult.data);

  const baseStats = row || {
    tenant_id: tenantId,
    total_tenders: 0,
    new_tenders_24h: 0,
    deadlines_in_7_days: 0,
    alerts_triggered_24h: 0
  };

  return {
    success: true,
    stats: {
      ...baseStats,
      open_tenders_now: openTendersNow
    }
  };
}

export async function getCurrentUserPayload(req: RequestLike) {
  const user = await getCurrentUser(req);
  return {
    userId: user.userId,
    email: user.email,
    role: user.role,
    roles: user.roles,
    tenantId: user.tenantId
  };
}

export async function listTenants(req: RequestLike) {
  const user = await getCurrentUser(req);
  const supabase = getServerSupabase();

  if (hasRole(user, 'creator')) {
    const { data, error } = await supabase
      .from('tenants')
      .select('id,name')
      .order('created_at', { ascending: true });

    if (error) throw Object.assign(new Error(error.message), { status: 500 });

    return {
      tenants: (Array.isArray(data) ? data : [])
        .map((row) => ({
          id: String(row.id || ''),
          name: String(row.name || row.id || '')
        }))
        .filter((row) => row.id)
    };
  }

  const { data, error } = await supabase
    .from('tenants')
    .select('id,name')
    .eq('id', user.tenantId)
    .order('created_at', { ascending: true });

  if (error) throw Object.assign(new Error(error.message), { status: 500 });

  return {
    tenants: (Array.isArray(data) ? data : [])
      .map((row) => ({
        id: String(row.id || ''),
        name: String(row.name || row.id || '')
      }))
      .filter((row) => row.id)
  };
}

export async function createTenant(req: RequestLike) {
  const user = await getCurrentUser(req);
  requireCreator(user);

  const body = readJsonBody<{ id?: string; name?: string }>(req);
  const name = String(body.name || '').trim();
  if (!name) throw badRequest('name is required');

  const requestedId = String(body.id || '').trim().toLowerCase();
  const tenantId = requestedId ? slugifyTenantId(requestedId) : slugifyTenantId(name);

  if (!TENANT_ID_PATTERN.test(tenantId)) {
    throw badRequest('invalid tenant id');
  }

  const supabase = getServerSupabase();
  const { error } = await supabase
    .from('tenants')
    .insert({ id: tenantId, name })
    .select('id,name')
    .single();

  if (error) {
    if (error.code === '23505') throw conflict('Tenant already exists');
    throw Object.assign(new Error(error.message), { status: 500 });
  }

  return {
    tenant: {
      id: tenantId,
      name
    }
  };
}

export async function getTenant(req: RequestLike) {
  const user = await getCurrentUser(req);
  const tenantId = getTenantFromHeader(req);
  requireTenantAccess(user, tenantId);

  const supabase = getServerSupabase();
  const { data, error } = await supabase
    .from('tenants')
    .select('id,name,regions,plan,plan_limits,limits,created_at')
    .eq('id', tenantId)
    .limit(1);

  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  const tenant = Array.isArray(data) && data.length ? (data[0] as TenantRow) : null;

  return {
    tenantId,
    name: tenant?.name || null,
    regions: Array.isArray(tenant?.regions) ? tenant.regions : [],
    planTier: tenant?.plan || null,
    planLimits: tenant?.plan_limits || tenant?.limits || null,
    createdAt: tenant?.created_at || null
  };
}

export async function listTenantUsers(req: RequestLike) {
  const { tenantId } = await requireTenantScopedPrivilegedUser(req);
  const supabase = getServerSupabase();

  const { data: users, error: usersError } = await supabase
    .from('users')
    .select('id,email,role,tenant_id,created_at')
    .eq('tenant_id', tenantId)
    .order('created_at', { ascending: false })
    .limit(200);

  if (usersError) throw Object.assign(new Error(usersError.message), { status: 500 });

  const userRows = (Array.isArray(users) ? users : []) as UserRow[];
  const userIds = userRows.map((row) => String(row.id || '')).filter(Boolean);

  let rolesByUserId = new Map<string, string[]>();
  if (userIds.length) {
    const { data: roleRows, error: rolesError } = await supabase
      .from('user_roles')
      .select('user_id,role')
      .in('user_id', userIds);

    if (rolesError) throw Object.assign(new Error(rolesError.message), { status: 500 });

    rolesByUserId = ((Array.isArray(roleRows) ? roleRows : []) as UserRoleRow[]).reduce((map, row: UserRoleRow) => {
      const userId = String(row.user_id || '');
      if (!userId) return map;
      const role = String(row.role || '').trim().toLowerCase();
      if (!role) return map;
      const next = map.get(userId) || [];
      if (!next.includes(role)) next.push(role);
      map.set(userId, next);
      return map;
    }, new Map<string, string[]>());
  }

  return userRows.map((tenantUser) => {
    const userId = String(tenantUser.id || '');
    const roles = rolesByUserId.get(userId) || [];
    const fallback = String(tenantUser.role || '').trim().toLowerCase();
    const effectiveRoles = roles.length ? roles : (fallback ? [fallback] : ['user']);

    return {
      userId,
      email: String(tenantUser.email || ''),
      roles: effectiveRoles
    };
  });
}

export async function getConnectorStatus(req: RequestLike) {
  const { tenantId } = await requireTenantScopedPrivilegedUser(req);
  const supabase = getServerSupabase();

  const [configResult, runsResult] = await Promise.all([
    supabase
      .from('ConnectorConfig')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('updated_at', { ascending: false })
      .limit(200),
    supabase
      .from('ConnectorRuns')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('started_at', { ascending: false })
      .limit(300)
  ]);

  if (configResult.error) throw Object.assign(new Error(configResult.error.message), { status: 500 });
  if (runsResult.error) throw Object.assign(new Error(runsResult.error.message), { status: 500 });

  const configs = (Array.isArray(configResult.data) ? configResult.data : []) as ConnectorConfigRow[];
  const runs = (Array.isArray(runsResult.data) ? runsResult.data : []) as ConnectorRunRow[];

  const keys = new Set<string>();

  for (const cfg of configs) {
    const key = String(cfg.connector_key || cfg.connector_id || cfg.source || '').trim();
    if (key) keys.add(key);
  }

  for (const run of runs) {
    const key = String(run.connector_key || run.source || '').trim();
    if (key) keys.add(key);
  }

  return Array.from(keys)
    .map((key) => {
      const display = toDisplay(key);
      const cfg = configs.find(
        (row: ConnectorConfigRow) => String(row.connector_key || row.connector_id || row.source || '').trim() === key
      );
      const keyRuns = runs.filter((row: ConnectorRunRow) => String(row.connector_key || row.source || '').trim() === key);
      const latestRun = keyRuns[0];
      const latestSuccess = keyRuns.find((row: ConnectorRunRow) => String(row.status || '').toLowerCase() === 'success');

      const enabled = cfg?.enabled !== false;
      const status = !enabled
        ? 'disabled'
        : String(latestRun?.status || cfg?.status || 'no_data').toLowerCase();

      const metadata = (latestRun?.metadata && typeof latestRun.metadata === 'object' && !Array.isArray(latestRun.metadata))
        ? (latestRun.metadata as Record<string, unknown>)
        : null;

      return {
        key: display.key,
        displayName: display.displayName,
        status,
        lastRunAt: latestRun?.started_at || cfg?.updated_at || null,
        lastSuccessAt: latestSuccess?.finished_at || latestSuccess?.started_at || null,
        lastError: latestRun?.error_summary || String(metadata?.error || '') || null,
        schedule: cfg?.schedule || cfg?.fetch_interval || null
      };
    })
    .sort((a, b) => a.key.localeCompare(b.key));
}

export async function getCapabilities(req: RequestLike) {
  const user = await getCurrentUser(req);
  const tenantId = getTenantFromHeader(req);
  requireTenantAccess(user, tenantId);

  return {
    isAdmin: isPrivileged(user),
    tenantId
  };
}

export async function enableSupportAccess(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedPrivilegedUser(req);
  const supabase = getServerSupabase();
  const body = readJsonBody<{ durationMinutes?: number; duration_minutes?: number; reason?: string }>(req);

  const reason = String(body.reason || '').trim();
  if (!reason) throw badRequest('reason is required');

  const durationMinutes = sanitizeDuration(body.durationMinutes || body.duration_minutes);
  const expiresAt = new Date(Date.now() + durationMinutes * 60 * 1000).toISOString();

  const active = await getActiveSupportGrant(tenantId);

  if (active) {
    const { error: updateError } = await supabase
      .from('support_access_grants')
      .update({
        enabled: true,
        expires_at: expiresAt,
        enabled_by_user_id: user.userId || null,
        reason,
        revoked_at: null,
        revoked_by_user_id: null,
        revoke_reason: null
      })
      .eq('id', String(active.id || ''))
      .eq('tenant_id', tenantId);

    if (updateError) throw Object.assign(new Error(updateError.message), { status: 500 });
  } else {
    const { error: insertError } = await supabase
      .from('support_access_grants')
      .insert({
        id: makeId('sa_grant'),
        tenant_id: tenantId,
        enabled: true,
        expires_at: expiresAt,
        enabled_by_user_id: user.userId || null,
        reason,
        created_at: new Date().toISOString(),
        revoked_at: null,
        revoked_by_user_id: null,
        revoke_reason: null
      });

    if (insertError) throw Object.assign(new Error(insertError.message), { status: 500 });
  }

  await writeSupportAudit({
    tenantId,
    actor: { userId: user.userId, email: user.email },
    action: 'ENABLED',
    reason,
    metadata: { durationMinutes, expiresAt }
  });

  return computeSupportStatus(tenantId, { userId: user.userId, email: user.email });
}

export async function revokeSupportAccess(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedPrivilegedUser(req);
  const supabase = getServerSupabase();
  const body = readJsonBody<{ reason?: string }>(req);

  const reason = String(body.reason || '').trim();
  if (!reason) throw badRequest('reason is required');

  const active = await getActiveSupportGrant(tenantId);
  if (active) {
    const { error: updateError } = await supabase
      .from('support_access_grants')
      .update({
        enabled: false,
        revoked_at: new Date().toISOString(),
        revoked_by_user_id: user.userId || null,
        revoke_reason: reason
      })
      .eq('id', String(active.id || ''))
      .eq('tenant_id', tenantId);

    if (updateError) throw Object.assign(new Error(updateError.message), { status: 500 });
  }

  await writeSupportAudit({
    tenantId,
    actor: { userId: user.userId, email: user.email },
    action: 'REVOKED',
    reason,
    metadata: { hadActiveGrant: Boolean(active), grantId: active?.id || null }
  });

  return computeSupportStatus(tenantId, { userId: user.userId, email: user.email });
}

export async function getSupportAccessStatus(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedPrivilegedUser(req);
  return computeSupportStatus(tenantId, { userId: user.userId, email: user.email });
}

export async function listSupportAccessAudit(req: RequestLike) {
  const { tenantId } = await requireTenantScopedPrivilegedUser(req);
  const body = readJsonBody<{ limit?: number }>(req);
  const limit = normalizeLimit(body.limit, 50, 200);
  const supabase = getServerSupabase();

  const { data, error } = await supabase
    .from('support_access_audit')
    .select('*')
    .eq('tenant_id', tenantId)
    .order('created_at', { ascending: false })
    .limit(limit);

  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  return (Array.isArray(data) ? data : []) as SupportAccessAuditRow[];
}

export async function getPipelineAdmin(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedAdmin(req);
  const body = readJsonBody<{
    action?: string;
    queue_id?: string;
    decision?: string;
    review_notes?: string;
    prediction_id?: string;
  }>(req);

  const action = String(body.action || 'overview').trim().toLowerCase();
  const supabase = getServerSupabase() as any;

  if (action === 'review_decision') {
    const queueId = String(body.queue_id || '').trim();
    const decision = String(body.decision || '').trim().toLowerCase();
    if (!queueId || !['approve', 'reject'].includes(decision)) {
      throw badRequest('queue_id and decision=approve|reject are required');
    }

    const { data, error } = await supabase
      .from('reconciliation_queue')
      .update({
        status: decision === 'approve' ? 'approved' : 'rejected',
        reviewed_by: user.email || user.userId || 'admin',
        reviewed_at: new Date().toISOString(),
        review_notes: String(body.review_notes || '')
      })
      .eq('id', queueId)
      .eq('tenant_id', tenantId)
      .select('*')
      .limit(1);

    if (error) throw Object.assign(new Error(error.message), { status: 500 });
    const row = Array.isArray(data) && data.length ? data[0] : null;
    if (!row) throw Object.assign(new Error('Queue item not found'), { status: 404 });
    return { success: true, item: row };
  }

  if (action === 'prediction_detail') {
    const predictionId = String(body.prediction_id || '').trim();
    if (!predictionId) throw badRequest('prediction_id is required');

    const { data, error } = await supabase
      .from('predictions')
      .select('*')
      .eq('id', predictionId)
      .eq('tenant_id', tenantId)
      .limit(1);

    if (error) throw Object.assign(new Error(error.message), { status: 500 });
    const row = Array.isArray(data) && data.length ? data[0] : null;
    if (!row) throw Object.assign(new Error('Prediction not found'), { status: 404 });
    return { success: true, prediction: row };
  }

  const [runsResult, queueResult, predictionsResult, rawCountResult] = await Promise.all([
    supabase
      .from('ingestion_runs')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('started_at', { ascending: false })
      .limit(100),
    supabase
      .from('reconciliation_queue')
      .select('*')
      .eq('tenant_id', tenantId)
      .eq('status', 'pending')
      .order('created_at', { ascending: false })
      .limit(200),
    supabase
      .from('predictions')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('generated_at', { ascending: false })
      .limit(200),
    supabase
      .from('raw_documents')
      .select('id', { count: 'exact', head: true })
      .eq('tenant_id', tenantId)
      .eq('source', 'ETENDERS_IE')
  ]);

  if (runsResult.error) throw Object.assign(new Error(runsResult.error.message), { status: 500 });
  if (queueResult.error) throw Object.assign(new Error(queueResult.error.message), { status: 500 });
  if (predictionsResult.error) throw Object.assign(new Error(predictionsResult.error.message), { status: 500 });

  const runRows = Array.isArray(runsResult.data) ? runsResult.data : [];
  const queueRows = Array.isArray(queueResult.data) ? queueResult.data : [];
  const predictionRows = Array.isArray(predictionsResult.data) ? predictionsResult.data : [];

  const runs = runRows.map((run) => {
    const metrics = normalizeMetrics(run.metrics);
    const dedupedInFile = toNumber(metrics.deduped_in_file ?? metrics.duplicates_in_file);
    const canonicalDuplicates = toNumber(metrics.canonical_duplicates ?? metrics.duplicates);
    const currentDuplicates = toNumber(metrics.current_duplicates);
    const inferredIdRows = toNumber(metrics.inferred_id_rows);
    const rawRows = toNumber(metrics.raw_rows);
    const processedRows = toNumber(metrics.processed_rows ?? metrics.processed ?? metrics.total_rows);
    const totalDuplicates = Math.max(
      toNumber(metrics.duplicates_total),
      dedupedInFile + Math.max(canonicalDuplicates, currentDuplicates)
    );

    return {
      ...run,
      id: run.id || run.run_id,
      metrics,
      duplicate_stats: {
        deduped_in_file: dedupedInFile,
        canonical_duplicates: canonicalDuplicates,
        current_duplicates: currentDuplicates,
        inferred_id_rows: inferredIdRows,
        raw_rows: rawRows,
        processed_rows: processedRows,
        total_duplicates: totalDuplicates
      }
    };
  });

  const duplicateSummary = runs.reduce(
    (acc, run) => {
      const stats = normalizeMetrics(run.duplicate_stats);
      acc.total_duplicates += toNumber(stats.total_duplicates);
      acc.deduped_in_file += toNumber(stats.deduped_in_file);
      acc.canonical_duplicates += toNumber(stats.canonical_duplicates);
      acc.current_duplicates += toNumber(stats.current_duplicates);
      acc.inferred_id_rows += toNumber(stats.inferred_id_rows);
      acc.processed_rows += toNumber(stats.processed_rows);
      acc.raw_rows += toNumber(stats.raw_rows);
      return acc;
    },
    {
      total_duplicates: 0,
      deduped_in_file: 0,
      canonical_duplicates: 0,
      current_duplicates: 0,
      inferred_id_rows: 0,
      processed_rows: 0,
      raw_rows: 0
    }
  );

  const rawDocumentsLogged = Number(rawCountResult.count || 0);

  return {
    success: true,
    runs,
    queue: queueRows,
    predictions: predictionRows,
    duplicateSummary: {
      ...duplicateSummary,
      run_count: runs.length,
      raw_documents_logged: rawDocumentsLogged
    }
  };
}

export async function dispatchFunction(functionName: string, req: RequestLike) {
  switch (functionName) {
    case 'getMyProfile':
      return getMyProfile(req);
    case 'getDashboardStats':
      return getDashboardStats(req);
    case 'searchTenders':
      return searchTenders(req);
    case 'getCurrentUser':
      return getCurrentUserPayload(req);
    case 'listTenants':
      return listTenants(req);
    case 'createTenant':
      return createTenant(req);
    case 'getTenant':
      return getTenant(req);
    case 'listTenantUsers':
      return listTenantUsers(req);
    case 'getConnectorStatus':
      return getConnectorStatus(req);
    case 'getCapabilities':
      return getCapabilities(req);
    case 'enableSupportAccess':
      return enableSupportAccess(req);
    case 'revokeSupportAccess':
      return revokeSupportAccess(req);
    case 'getSupportAccessStatus':
      return getSupportAccessStatus(req);
    case 'listSupportAccessAudit':
      return listSupportAccessAudit(req);
    case 'getPipelineAdmin':
      return getPipelineAdmin(req);
    default:
      throw Object.assign(new Error(`Function not found: ${functionName}`), { status: 404 });
  }
}
