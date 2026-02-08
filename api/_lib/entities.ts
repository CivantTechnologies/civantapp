import { getHeader, type RequestLike } from './http.js';
import { getServerSupabase } from './supabase.js';

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;
const TENDERS_CURRENT_FIELD_MAP: Record<string, string> = {
  id: 'tender_id',
  tender_uid: 'tender_id',
  publication_date: 'published_at',
  first_seen_at: 'published_at',
  last_seen_at: 'updated_at'
};
const INGESTION_RUNS_FIELD_MAP: Record<string, string> = {
  id: 'run_id'
};

const ENTITY_TABLE_MAP: Record<string, string> = {
  User: 'users'
};

const TENANT_SCOPED_TABLES = new Set([
  'raw_documents',
  'ingestion_runs',
  'staging_records',
  'canonical_tenders',
  'entities',
  'entity_aliases',
  'reconciliation_queue',
  'tender_features_weekly',
  'market_signals',
  'predictions',
  'ConnectorConfig',
  'ConnectorRuns',
  'TendersCurrent',
  'TenderVersions',
  'support_access_grants',
  'support_access_audit',
  'users'
]);

type DynamicRequest = RequestLike & {
  query?: Record<string, unknown>;
};

function badRequest(message: string) {
  return Object.assign(new Error(message), { status: 400 });
}

function parseJson(value: unknown): Record<string, unknown> {
  if (!value) return {};
  if (typeof value === 'object') return value as Record<string, unknown>;
  if (typeof value !== 'string') return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}

function parseStringParam(value: unknown) {
  return String(value || '').trim();
}

function parseNumberParam(value: unknown, fallback: number) {
  const n = Number(value);
  if (Number.isNaN(n) || n < 0) return fallback;
  return Math.floor(n);
}

function resolveTenantFromHeader(req: RequestLike) {
  const raw = String(getHeader(req, 'x-tenant-id') || '').trim().toLowerCase();
  if (!raw) return '';
  if (!TENANT_ID_PATTERN.test(raw)) return '';
  return raw;
}

function resolveEntityName(req: DynamicRequest) {
  const entityName = parseStringParam(req.query?.entityName);
  if (!entityName) throw badRequest('Missing entity name');
  return entityName;
}

export function resolveTableName(entityName: string) {
  return ENTITY_TABLE_MAP[entityName] || entityName;
}

function mapFieldForTable(tableName: string, field: string) {
  if (tableName === 'TendersCurrent') {
    return TENDERS_CURRENT_FIELD_MAP[field] || field;
  }
  if (tableName === 'ingestion_runs') {
    return INGESTION_RUNS_FIELD_MAP[field] || field;
  }
  return field;
}

function normalizeSortForTable(tableName: string, sortValue: string) {
  if (!sortValue) return sortValue;

  return sortValue
    .split(',')
    .map((token) => {
      const raw = token.trim();
      if (!raw) return '';
      const desc = raw.startsWith('-');
      const field = desc ? raw.slice(1) : raw;
      const mappedField = mapFieldForTable(tableName, field);
      return `${desc ? '-' : ''}${mappedField}`;
    })
    .filter(Boolean)
    .join(',');
}

function normalizeTendersCurrentRow(row: unknown) {
  if (!row || typeof row !== 'object') return row;

  const base = row as Record<string, unknown>;
  const nestedData = base.data;
  const data =
    nestedData && typeof nestedData === 'object' && !Array.isArray(nestedData)
      ? (nestedData as Record<string, unknown>)
      : {};

  const merged: Record<string, unknown> = { ...data, ...base };
  const tenderId = String(base.tender_id || data.tender_id || data.id || '').trim();

  if (tenderId) {
    if (!merged.id) merged.id = tenderId;
    if (!merged.tender_uid) merged.tender_uid = tenderId;
    if (!merged.canonical_id) merged.canonical_id = tenderId;
  }

  if (!merged.title && data.tender_name) {
    merged.title = data.tender_name;
  }

  const cpvCodes = merged.cpv_codes;
  if (Array.isArray(cpvCodes)) {
    merged.cpv_codes = cpvCodes.join(',');
  } else if (cpvCodes === null || cpvCodes === undefined) {
    merged.cpv_codes = '';
  }

  if (!merged.publication_date && typeof base.published_at === 'string') {
    merged.publication_date = base.published_at.slice(0, 10);
  }

  if (!merged.first_seen_at) {
    merged.first_seen_at = data.first_seen_at || base.published_at || base.updated_at || null;
  }

  if (!merged.last_seen_at) {
    merged.last_seen_at = data.last_seen_at || base.updated_at || null;
  }

  if (!merged.buyer_name && data.contracting_authority) {
    merged.buyer_name = data.contracting_authority;
  }

  if (!merged.notice_type) {
    merged.notice_type = data.notice_type || 'tender';
  }

  if (!merged.source_notice_id && data.tender_id) {
    merged.source_notice_id = data.tender_id;
  }

  if (!merged.url && data.source_url) {
    merged.url = data.source_url;
  }

  return merged;
}

function normalizeEntityRow(tableName: string, row: unknown) {
  if (tableName === 'TendersCurrent') {
    return normalizeTendersCurrentRow(row);
  }
  if (tableName === 'ingestion_runs' && row && typeof row === 'object') {
    const base = row as Record<string, unknown>;
    if (!base.id && base.run_id) {
      return { ...base, id: base.run_id };
    }
  }
  return row;
}

function applyFilter(
  qb: any,
  field: string,
  value: unknown
) {
  if (value === null) return qb.is(field, null);
  if (Array.isArray(value)) return qb.in(field, value as string[]);

  if (typeof value === 'object' && value !== null) {
    const opValue = value as Record<string, unknown>;
    if ('$ilike' in opValue) return qb.ilike(field, String(opValue.$ilike || ''));
    if ('$like' in opValue) return qb.like(field, String(opValue.$like || ''));
    if ('$gte' in opValue) return qb.gte(field, opValue.$gte);
    if ('$lte' in opValue) return qb.lte(field, opValue.$lte);
    if ('$gt' in opValue) return qb.gt(field, opValue.$gt);
    if ('$lt' in opValue) return qb.lt(field, opValue.$lt);
    if ('$neq' in opValue) return qb.neq(field, opValue.$neq);
    if ('$in' in opValue && Array.isArray(opValue.$in)) return qb.in(field, opValue.$in as string[]);
    if ('$contains' in opValue) return qb.contains(field, opValue.$contains);
  }

  return qb.eq(field, value as string | number | boolean);
}

function applySort(qb: any, sortValue: string) {
  if (!sortValue) return qb;
  const fields = sortValue
    .split(',')
    .map((raw) => raw.trim())
    .filter(Boolean);

  let next = qb;
  for (const rawField of fields) {
    const desc = rawField.startsWith('-');
    const field = desc ? rawField.slice(1) : rawField;
    if (!field) continue;
    next = next.order(field, { ascending: !desc });
  }
  return next;
}

function applyIdFilter(qb: any, tableName: string, id: string) {
  if (tableName === 'TendersCurrent') {
    return qb.eq('tender_id', id);
  }
  if (tableName === 'ingestion_runs') {
    return qb.eq('run_id', id);
  }
  if (tableName === 'canonical_tenders') {
    return qb.eq('canonical_id', id);
  }
  return qb.eq('id', id);
}

export async function listOrFilterEntity(req: DynamicRequest) {
  const entityName = resolveEntityName(req);
  const tableName = resolveTableName(entityName);
  const queryObj = parseJson(req.query?.q);
  const sort = parseStringParam(req.query?.sort);
  const fields = parseStringParam(req.query?.fields) || '*';
  const limit = parseNumberParam(req.query?.limit, 100);
  const skip = parseNumberParam(req.query?.skip, 0);
  const tenantId = resolveTenantFromHeader(req);
  const supabase = getServerSupabase() as any;

  let qb = supabase.from(tableName as any).select(fields);

  if (tenantId && TENANT_SCOPED_TABLES.has(tableName) && !('tenant_id' in queryObj)) {
    qb = qb.eq('tenant_id', tenantId);
  }

  for (const [field, value] of Object.entries(queryObj)) {
    qb = applyFilter(qb, mapFieldForTable(tableName, field), value);
  }

  qb = applySort(qb, normalizeSortForTable(tableName, sort));

  if (limit > 0) {
    qb = qb.range(skip, skip + limit - 1);
  } else if (skip > 0) {
    qb = qb.range(skip, skip + 999);
  }

  const { data, error } = await qb;
  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  const rows = Array.isArray(data) ? data : [];
  return rows.map((row) => normalizeEntityRow(tableName, row));
}

export async function createEntity(req: DynamicRequest) {
  const entityName = resolveEntityName(req);
  const tableName = resolveTableName(entityName);
  const payload = (req.body && typeof req.body === 'object') ? req.body : {};
  const tenantId = resolveTenantFromHeader(req);
  const supabase = getServerSupabase() as any;

  const bodyWithTenant = (() => {
    if (!tenantId || !TENANT_SCOPED_TABLES.has(tableName)) {
      return payload;
    }

    if (Array.isArray(payload)) {
      return payload.map((row) => {
        if (!row || typeof row !== 'object') return row;
        if ('tenant_id' in (row as Record<string, unknown>)) return row;
        return { ...(row as Record<string, unknown>), tenant_id: tenantId };
      });
    }

    if (payload && typeof payload === 'object' && !('tenant_id' in (payload as Record<string, unknown>))) {
      return { ...(payload as Record<string, unknown>), tenant_id: tenantId };
    }

    return payload;
  })();

  const { data, error } = await supabase.from(tableName as any).insert(bodyWithTenant).select('*').limit(1);
  if (error) throw Object.assign(new Error(error.message), { status: 500 });

  if (Array.isArray(data) && data.length > 0) return normalizeEntityRow(tableName, data[0]);
  return {};
}

export async function deleteManyEntity(req: DynamicRequest) {
  const entityName = resolveEntityName(req);
  const tableName = resolveTableName(entityName);
  const supabase = getServerSupabase() as any;
  const body = (req.body && typeof req.body === 'object') ? (req.body as Record<string, unknown>) : {};
  const ids = Array.isArray(body.ids) ? body.ids : [];
  const tenantId = resolveTenantFromHeader(req);

  if (!ids.length) throw badRequest('ids array is required for deleteMany');

  let qb = tableName === 'TendersCurrent'
    ? supabase.from(tableName as any).delete().in('tender_id', ids as string[])
    : tableName === 'ingestion_runs'
      ? supabase.from(tableName as any).delete().in('run_id', ids as string[])
    : tableName === 'canonical_tenders'
      ? supabase.from(tableName as any).delete().in('canonical_id', ids as string[])
    : supabase.from(tableName as any).delete().in('id', ids as string[]);
  if (tenantId && TENANT_SCOPED_TABLES.has(tableName)) {
    qb = qb.eq('tenant_id', tenantId);
  }
  const { error } = await qb;
  if (error) throw Object.assign(new Error(error.message), { status: 500 });

  return { success: true, deleted: ids.length };
}

export async function getEntityById(req: DynamicRequest) {
  const entityName = resolveEntityName(req);
  const tableName = resolveTableName(entityName);
  const id = parseStringParam(req.query?.id);
  if (!id) throw badRequest('Missing entity id');

  const tenantId = resolveTenantFromHeader(req);
  const supabase = getServerSupabase() as any;

  let qb = supabase.from(tableName as any).select('*');
  qb = applyIdFilter(qb, tableName, id);
  if (tenantId && TENANT_SCOPED_TABLES.has(tableName)) {
    qb = qb.eq('tenant_id', tenantId);
  }

  const { data, error } = await qb.limit(1);
  if (error) throw Object.assign(new Error(error.message), { status: 500 });

  if (!Array.isArray(data) || data.length === 0) {
    throw Object.assign(new Error('Record not found'), { status: 404 });
  }
  return normalizeEntityRow(tableName, data[0]);
}

export async function updateEntityById(req: DynamicRequest) {
  const entityName = resolveEntityName(req);
  const tableName = resolveTableName(entityName);
  const id = parseStringParam(req.query?.id);
  if (!id) throw badRequest('Missing entity id');

  const payload = (req.body && typeof req.body === 'object') ? req.body : {};
  const tenantId = resolveTenantFromHeader(req);
  const supabase = getServerSupabase() as any;

  let qb = supabase.from(tableName as any).update(payload);
  qb = applyIdFilter(qb, tableName, id);
  if (tenantId && TENANT_SCOPED_TABLES.has(tableName)) {
    qb = qb.eq('tenant_id', tenantId);
  }

  const { data, error } = await qb.select('*').limit(1);
  if (error) throw Object.assign(new Error(error.message), { status: 500 });

  if (Array.isArray(data) && data.length > 0) return normalizeEntityRow(tableName, data[0]);
  return {};
}

export async function deleteEntityById(req: DynamicRequest) {
  const entityName = resolveEntityName(req);
  const tableName = resolveTableName(entityName);
  const id = parseStringParam(req.query?.id);
  if (!id) throw badRequest('Missing entity id');

  const tenantId = resolveTenantFromHeader(req);
  const supabase = getServerSupabase() as any;

  let qb = supabase.from(tableName as any).delete();
  qb = applyIdFilter(qb, tableName, id);
  if (tenantId && TENANT_SCOPED_TABLES.has(tableName)) {
    qb = qb.eq('tenant_id', tenantId);
  }

  const { error } = await qb;
  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  return { success: true };
}
