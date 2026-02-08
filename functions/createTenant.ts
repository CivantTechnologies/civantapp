import { createClientFromRequest } from './civantSdk.ts';
import { requireCreator } from './requireAdmin.ts';

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;

function slugifyTenantId(value: string) {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_')
    .slice(0, 40);
  return normalized;
}

function badRequest(message: string) {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 400;
  return err;
}

function conflict(message: string) {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 409;
  return err;
}

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    await requireCreator({ civant, req });

    const body = await req.json().catch(() => ({}));
    const name = String(body.name || '').trim();
    if (!name) {
      throw badRequest('name is required');
    }

    const requestedId = String(body.id || '').trim().toLowerCase();
    const tenantId = requestedId ? slugifyTenantId(requestedId) : slugifyTenantId(name);
    if (!TENANT_ID_PATTERN.test(tenantId)) {
      throw badRequest('invalid tenant id');
    }

    const existing = await civant.asServiceRole.entities.tenants.filter({ id: tenantId }, 'created_at', 1);
    if (Array.isArray(existing) && existing.length > 0) {
      throw conflict('Tenant already exists');
    }

    try {
      await civant.asServiceRole.entities.tenants.create({
        id: tenantId,
        name,
        created_at: new Date().toISOString()
      });
    } catch (error) {
      const message = String((error as Error).message || '');
      if (message.toLowerCase().includes('duplicate') || message.toLowerCase().includes('already exists')) {
        throw conflict('Tenant already exists');
      }
      throw error;
    }

    return Response.json({
      tenant: {
        id: tenantId,
        name
      }
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to create tenant' }, { status });
  }
});
