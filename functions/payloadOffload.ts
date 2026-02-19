type OffloadResult = {
  offloaded: boolean;
  raw_object_key?: string;
  payload_hash_sha256?: string;
  payload_bytes?: number;
  payload_stored_at?: string;
};

type OffloadParams = {
  civant: any;
  tenantId: string;
  tableName: string;
  primaryKey: string;
  payload: unknown;
};

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function envRequired(name: string, fallback?: string) {
  const value = String(Deno.env.get(name) || Deno.env.get(fallback || '') || '').trim();
  if (!value) {
    const label = fallback ? `${name} (or ${fallback})` : name;
    const err = new Error(`Missing required environment variable: ${label}`) as Error & { payloadOffloadFatal?: boolean };
    err.payloadOffloadFatal = true;
    throw err;
  }
  return value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (isPlainObject(value)) {
    const out: Record<string, unknown> = {};
    Object.keys(value)
      .sort()
      .forEach((key) => {
        out[key] = canonicalize((value as Record<string, unknown>)[key]);
      });
    return out;
  }
  return value;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalize(value));
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const view = new Uint8Array(bytes);
  return view.buffer;
}

async function sha256Hex(bytes: Uint8Array) {
  const hash = await crypto.subtle.digest('SHA-256', toArrayBuffer(bytes));
  return Array.from(new Uint8Array(hash)).map((b) => b.toString(16).padStart(2, '0')).join('');
}

async function gzipBytes(bytes: Uint8Array): Promise<Uint8Array> {
  const stream = new CompressionStream('gzip');
  const writer = stream.writable.getWriter();
  await writer.write(new Uint8Array(bytes));
  await writer.close();
  const compressed = await new Response(stream.readable).arrayBuffer();
  return new Uint8Array(compressed);
}

function safePathSegment(input: string) {
  return input.replaceAll('/', '_');
}

async function recordFailure(params: {
  civant: any;
  tenantId: string;
  tableName: string;
  primaryKey: string;
  error: string;
  payloadHash?: string;
  objectKey?: string;
}) {
  try {
    await params.civant.asServiceRole.entities.payload_offload_failures.create({
      tenant_id: params.tenantId,
      table_name: params.tableName,
      primary_key: params.primaryKey,
      payload_hash_sha256: params.payloadHash || null,
      raw_object_key: params.objectKey || null,
      error: params.error
    });
  } catch (err: unknown) {
    console.error('Failed to record payload_offload_failures:', getErrorMessage(err));
  }
}

export async function offloadPayload(params: OffloadParams): Promise<OffloadResult> {
  const bucket = envRequired('SUPABASE_STORAGE_BUCKET');
  const supabaseUrl = envRequired('SUPABASE_URL', 'VITE_SUPABASE_URL');
  const serviceRoleKey = envRequired('SUPABASE_SERVICE_ROLE_KEY');
  const allowFallback = String(Deno.env.get('OFFLOAD_FALLBACK_ALLOW_DB_PAYLOAD') || '').toLowerCase() === 'true';

  if (params.payload === null || params.payload === undefined) {
    return { offloaded: false };
  }

  let canonicalInput: unknown = params.payload;
  if (typeof params.payload === 'string') {
    try {
      canonicalInput = JSON.parse(params.payload);
    } catch {
      canonicalInput = params.payload;
    }
  }

  const canonicalText = canonicalJson(canonicalInput);
  const encoder = new TextEncoder();
  const bytes = encoder.encode(canonicalText);
  const hash = await sha256Hex(bytes);
  const payloadBytes = bytes.byteLength;
  const objectKey = `${safePathSegment(params.tenantId)}/${safePathSegment(params.tableName)}/${safePathSegment(params.primaryKey)}/${hash}.json.gz`;
  const objectUrl = `${supabaseUrl}/storage/v1/object/${bucket}/${objectKey}`;

  try {
    const head = await fetch(objectUrl, {
      method: 'HEAD',
      headers: { Authorization: `Bearer ${serviceRoleKey}` }
    });

    if (head.status !== 200) {
      const compressed = await gzipBytes(bytes);
      const upload = await fetch(objectUrl, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${serviceRoleKey}`,
          'Content-Type': 'application/gzip',
          'x-upsert': 'true'
        },
        body: compressed
      });

      if (!upload.ok) {
        const body = await upload.text().catch(() => '');
        const message = `storage_upload_failed status=${upload.status} ${body}`;
        await recordFailure({
          civant: params.civant,
          tenantId: params.tenantId,
          tableName: params.tableName,
          primaryKey: params.primaryKey,
          error: message,
          payloadHash: hash,
          objectKey
        });
        if (!allowFallback) {
          const err = new Error(message) as Error & { payloadOffloadFatal?: boolean };
          err.payloadOffloadFatal = true;
          throw err;
        }
        return { offloaded: false };
      }
    }

    return {
      offloaded: true,
      raw_object_key: objectKey,
      payload_hash_sha256: hash,
      payload_bytes: payloadBytes,
      payload_stored_at: new Date().toISOString()
    };
  } catch (err: unknown) {
    const message = getErrorMessage(err);
    await recordFailure({
      civant: params.civant,
      tenantId: params.tenantId,
      tableName: params.tableName,
      primaryKey: params.primaryKey,
      error: message,
      payloadHash: hash,
      objectKey
    });
    if (!allowFallback) {
      (err as Error & { payloadOffloadFatal?: boolean }).payloadOffloadFatal = true;
      throw err;
    }
    return { offloaded: false };
  }
}
