import { getHeader, type RequestLike } from './http.js';
import { getServerSupabase } from './supabase.js';

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;

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
    return qb.or(`id.eq.${id},tender_id.eq.${id}`);
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
    qb = applyFilter(qb, field, value);
  }

  qb = applySort(qb, sort);

  if (limit > 0) {
    qb = qb.range(skip, skip + limit - 1);
  } else if (skip > 0) {
    qb = qb.range(skip, skip + 999);
  }

  const { data, error } = await qb;
  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  return Array.isArray(data) ? data : [];
}

export async function createEntity(req: DynamicRequest) {
  const entityName = resolveEntityName(req);
  const tableName = resolveTableName(entityName);
  const payload = (req.body && typeof req.body === 'object') ? req.body : {};
  const tenantId = resolveTenantFromHeader(req);
  const supabase = getServerSupabase() as any;

  const bodyWithTenant =
    tenantId && TENANT_SCOPED_TABLES.has(tableName) && !(payload as Record<string, unknown>).tenant_id
      ? { ...(payload as Record<string, unknown>), tenant_id: tenantId }
      : payload;

  const { data, error } = await supabase.from(tableName as any).insert(bodyWithTenant).select('*').limit(1);
  if (error) throw Object.assign(new Error(error.message), { status: 500 });

  if (Array.isArray(data) && data.length > 0) return data[0];
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

  let qb = supabase.from(tableName as any).delete().in('id', ids as string[]);
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
  return data[0];
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

  if (Array.isArray(data) && data.length > 0) return data[0];
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
