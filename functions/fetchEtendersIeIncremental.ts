// @ts-nocheck
import { createClientFromRequest } from './civantSdk.ts';

// Official eTenders portal HTML listing for "Latest CfTs" (no auth)
const BASE_URL = 'https://www.etenders.gov.ie';
const FIRST_URL = `${BASE_URL}/epps/quickSearchAction.do?searchType=cftFTS&latest=true`;

function stripTags(html: string) {
  return String(html || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractFirst(re: RegExp, text: string) {
  const m = re.exec(text);
  return m ? m[1] : '';
}

function parseEtendersDate(raw: string) {
  const s = stripTags(raw);
  if (!s) return null;
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return null;
  return new Date(ms);
}

function toIsoTs(d: Date | null) {
  if (!d || !Number.isFinite(d.getTime())) return null;
  return d.toISOString();
}

function toIsoDate(d: Date | null) {
  if (!d || !Number.isFinite(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function parseLatestCftsTable(html: string) {
  const tableId = extractFirst(/\bd-(\d+)-p=/, html);
  const tbodyMatch = /<tbody[^>]*>([\s\S]*?)<\/tbody>/i.exec(html);
  const tbody = tbodyMatch ? tbodyMatch[1] : '';
  if (!tbody) return { tableId, rows: [] as any[] };

  const trMatches = tbody.split(/<tr[^>]*>/i).slice(1);
  const rows: any[] = [];

  for (const trChunk of trMatches) {
    const tr = (trChunk.split(/<\/tr>/i)[0] || '');
    const tds = tr.split(/<td[^>]*>/i).slice(1).map((c) => (c.split(/<\/td>/i)[0] || ''));
    if (tds.length < 8) continue;

    const title = stripTags(tds[1] || '');
    const resourceId = stripTags(tds[2] || '').replace(/\D/g, '');
    if (!resourceId) continue;

    const ca = stripTags(tds[3] || '');
    const publishedAt = parseEtendersDate(tds[5] || '');
    const deadlineAt = parseEtendersDate(tds[6] || '');
    const procedure = stripTags(tds[7] || '');
    const status = stripTags(tds[8] || '');

    const estValueRaw = stripTags(tds[11] || '');
    const estimatedValue = estValueRaw ? Number(estValueRaw) : null;

    const cycleRaw = stripTags(tds[12] || '');
    const cycle = cycleRaw ? Number(cycleRaw) : null;

    const url = `${BASE_URL}/epps/cft/prepareViewCfTWS.do?resourceId=${encodeURIComponent(resourceId)}`;

    rows.push({
      resourceId,
      title,
      contractingAuthority: ca,
      publishedAt,
      deadlineAt,
      procedure,
      status,
      estimatedValue: Number.isFinite(estimatedValue) ? estimatedValue : null,
      cycle: Number.isFinite(cycle) ? cycle : null,
      url
    });
  }

  return { tableId, rows };
}

async function sha256Hex(text: string) {
  const bytes = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, '0')).join('');
}

function coerceTenantId(body: any, req: Request) {
  return String(
    body?.tenant_id
      || req.headers.get('X-Tenant-Id')
      || Deno.env.get('DEFAULT_TENANT_ID')
      || 'civant_default'
  );
}

function asStartDate(raw: unknown) {
  const s = String(raw || '').trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return new Date(`${s}T00:00:00.000Z`);
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return null;
  return new Date(ms);
}

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await civant.auth.me();

    if (!user || user.role !== 'admin') {
      return Response.json({ error: 'Admin access required' }, { status: 403 });
    }

    const body = await req.json().catch(() => ({}));
    const tenantId = coerceTenantId(body, req);
    const connectorKey = `etenders_ie_incremental:${tenantId}`;

    const dryRun = Boolean(body?.dry_run);
    const limit = Math.max(1, Number(body?.limit || 200));
    const maxPages = Math.max(1, Number(body?.max_pages || 30));

    // Cursor lookup
    let cfg = null;
    try {
      const rows = await civant.asServiceRole.entities.ConnectorConfig.filter({ tenant_id: tenantId, connector_key: connectorKey });
      cfg = Array.isArray(rows) && rows.length ? rows[0] : null;
    } catch {
      cfg = null;
    }

    const cursorValue = cfg?.config?.cursor?.value || null;
    const cursorDate = asStartDate(cursorValue);
    const overrideStart = asStartDate(body?.start_date);

    const since = overrideStart
      ? overrideStart
      : cursorDate
        ? new Date(cursorDate.getTime() - 2 * 24 * 60 * 60 * 1000)
        : (() => {
          const d = new Date();
          d.setDate(d.getDate() - Math.max(1, Number(body?.days_since || 7)));
          return d;
        })();

    // Run log
    let runId = null;
    try {
      const run = await civant.asServiceRole.entities.ConnectorRuns.create({
        tenant_id: tenantId,
        connector_key: connectorKey,
        status: 'running',
        started_at: new Date().toISOString(),
        metadata: {
          start_date: since.toISOString(),
          dry_run: dryRun
        }
      });
      runId = run?.id || null;
    } catch {
      runId = null;
    }

    let tableId = '';
    let fetched = 0;
    let considered = 0;
    let insertedOrUpdated = 0;
    let versioned = 0;
    const errors: string[] = [];

    const nowMs = Date.now();
    const futureCutoffMs = nowMs + 24 * 60 * 60 * 1000;

    const upserts: any[] = [];

    for (let page = 1; page <= maxPages; page++) {
      const url = page === 1
        ? FIRST_URL
        : `${BASE_URL}/epps/quickSearchAction.do?d-${encodeURIComponent(tableId)}-p=${page}&searchType=cftFTS&latest=true`;

      const res = await fetch(url, {
        headers: {
          'Accept': 'text/html,application/xhtml+xml',
          'User-Agent': 'CivantBot/1.0 (+https://civant.ai)'
        },
        signal: AbortSignal.timeout(45000)
      });

      const html = await res.text();
      if (!res.ok) {
        errors.push(`HTTP ${res.status} for ${url}: ${html.slice(0, 120)}`);
        break;
      }

      const parsed = parseLatestCftsTable(html);
      if (!tableId) tableId = parsed.tableId;
      if (page > 1 && !tableId) {
        errors.push('Could not resolve pagination table id (d-<id>-p=).');
        break;
      }

      if (!parsed.rows.length) break;

      fetched += parsed.rows.length;

      for (const r of parsed.rows) {
        if (upserts.length >= limit) break;

        const publishedAt = r.publishedAt as Date | null;
        if (!publishedAt) continue;
        const pubMs = publishedAt.getTime();
        if (!Number.isFinite(pubMs)) continue;
        if (pubMs > futureCutoffMs) continue;
        if (pubMs < since.getTime()) continue;

        considered += 1;

        const tenderId = `ETENDERS_IE:${r.resourceId}`;
        const publishedIso = toIsoTs(publishedAt);
        if (!publishedIso) continue;

        const data = {
          source: 'ETENDERS_IE',
          country: 'IE',
          canonical_id: tenderId,
          source_notice_id: r.resourceId,
          title: r.title,
          buyer_name: r.contractingAuthority,
          contracting_authority: r.contractingAuthority,
          notice_type: 'tender',
          publication_date: toIsoDate(publishedAt),
          deadline_date: toIsoDate(r.deadlineAt || null),
          procedure: r.procedure || null,
          status: r.status || null,
          estimated_value: r.estimatedValue,
          currency: 'EUR',
          cft_active_cycle: r.cycle,
          source_url: r.url,
          first_seen_at: publishedIso,
          version_count: 1
        };

        const versionBasis = {
          title: data.title,
          buyer_name: data.buyer_name,
          publication_date: data.publication_date,
          deadline_date: data.deadline_date,
          procedure: data.procedure,
          status: data.status,
          estimated_value: data.estimated_value,
          cft_active_cycle: data.cft_active_cycle,
          source_url: data.source_url
        };

        const versionHash = await sha256Hex(JSON.stringify(versionBasis));

        upserts.push({
          current: {
            tenant_id: tenantId,
            tender_id: tenderId,
            source: 'ETENDERS_IE',
            published_at: publishedIso,
            data
          },
          version: {
            tenant_id: tenantId,
            tender_id: tenderId,
            version_hash: versionHash,
            data
          },
          published_at: publishedIso
        });
      }

      if (upserts.length >= limit) break;
    }

    const maxPublishedAt = upserts.reduce((acc: string | null, it: any) => {
      const ts = String(it.published_at || '');
      if (!ts) return acc;
      return !acc || ts > acc ? ts : acc;
    }, null);

    if (dryRun) {
      return Response.json({
        success: true,
        dry_run: true,
        tenant_id: tenantId,
        connector_key: connectorKey,
        start_date: since.toISOString(),
        fetched_count: fetched,
        considered_count: considered,
        upsert_count: upserts.length,
        max_published_at: maxPublishedAt,
        sample: upserts.slice(0, 5).map((x) => x.current),
        errors: errors.slice(0, 10)
      });
    }

    // Writes
    for (const item of upserts) {
      try {
        await civant.asServiceRole.entities.TenderVersions.create(item.version);
        versioned += 1;
      } catch {
        // Ignore duplicates / conflicts.
      }

      try {
        await civant.asServiceRole.entities.TendersCurrent.create(item.current);
        insertedOrUpdated += 1;
      } catch (e) {
        errors.push(`Upsert error: ${e?.message || String(e)}`);
      }
    }

    // Update cursor/config only on success-ish (no fatal errors).
    if (!errors.length && maxPublishedAt) {
      const nextConfig = {
        ...(cfg?.config && typeof cfg.config === 'object' ? cfg.config : {}),
        cursor: {
          type: 'published',
          value: maxPublishedAt,
          last_success_at: new Date().toISOString()
        }
      };

      try {
        if (cfg?.id) {
          await civant.asServiceRole.entities.ConnectorConfig.update(cfg.id, {
            tenant_id: tenantId,
            connector_key: connectorKey,
            enabled: true,
            config: nextConfig,
            updated_at: new Date().toISOString()
          });
        } else {
          await civant.asServiceRole.entities.ConnectorConfig.create({
            tenant_id: tenantId,
            connector_key: connectorKey,
            enabled: true,
            config: nextConfig,
            updated_at: new Date().toISOString()
          });
        }
      } catch (e) {
        errors.push(`Config update error: ${e?.message || String(e)}`);
      }
    }

    if (runId) {
      try {
        await civant.asServiceRole.entities.ConnectorRuns.update(runId, {
          status: errors.length ? 'partial' : 'success',
          finished_at: new Date().toISOString(),
          metadata: {
            start_date: since.toISOString(),
            fetched_count: fetched,
            upsert_count: upserts.length,
            written_count: insertedOrUpdated,
            versioned_count: versioned,
            max_published_at: maxPublishedAt,
            errors: errors.slice(0, 10)
          }
        });
      } catch {
        // ignore
      }
    }

    return Response.json({
      success: errors.length === 0,
      tenant_id: tenantId,
      connector_key: connectorKey,
      start_date: since.toISOString(),
      fetched_count: fetched,
      upsert_count: upserts.length,
      written_count: insertedOrUpdated,
      versioned_count: versioned,
      max_published_at: maxPublishedAt,
      errors: errors.slice(0, 10)
    }, { status: errors.length ? 207 : 200 });
  } catch (e) {
    return Response.json({ error: e?.message || String(e) }, { status: 500 });
  }
});
