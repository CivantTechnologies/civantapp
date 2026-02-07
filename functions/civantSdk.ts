type QueryParams = Record<string, string | number | boolean | undefined | null>;

function buildUrl(baseUrl: string, path: string, query?: QueryParams) {
    const url = new URL(path, baseUrl);
    if (query) {
        Object.entries(query).forEach(([key, value]) => {
            if (value === undefined || value === null) return;
            url.searchParams.set(key, String(value));
        });
    }
    return url.toString();
}

function hasFileLike(payload: unknown): boolean {
    if (!payload) return false;
    if (payload instanceof Blob) return true;
    if (Array.isArray(payload)) return payload.some(hasFileLike);
    if (typeof payload === 'object') return Object.values(payload).some(hasFileLike);
    return false;
}

function toFormData(payload: Record<string, unknown>) {
    const formData = new FormData();
    Object.entries(payload || {}).forEach(([key, value]) => {
        if (value === undefined || value === null) return;
        if (value instanceof Blob) {
            formData.append(key, value);
            return;
        }
        if (typeof value === 'object') {
            formData.append(key, JSON.stringify(value));
            return;
        }
        formData.append(key, String(value));
    });
    return formData;
}

function createHttpClient(baseUrl: string, appId: string, token?: string, extraHeaders: Record<string, string> = {}) {
    const request = async (method: string, path: string, options: {
        query?: QueryParams;
        body?: unknown;
        headers?: Record<string, string>;
    } = {}) => {
        const url = buildUrl(baseUrl, path, options.query);
        const headers = new Headers({
            Accept: 'application/json',
            'X-App-Id': appId,
            ...extraHeaders,
            ...(options.headers || {})
        });

        if (token) {
            headers.set('Authorization', token.startsWith('Bearer ') ? token : `Bearer ${token}`);
        }

        let body: BodyInit | undefined;
        if (options.body !== undefined) {
            if (options.body instanceof FormData) {
                body = options.body;
            } else if (typeof options.body === 'object') {
                headers.set('Content-Type', 'application/json');
                body = JSON.stringify(options.body);
            } else {
                body = String(options.body);
            }
        }

        const response = await fetch(url, { method, headers, body });
        const raw = await response.text();
        const data = raw ? (() => {
            try {
                return JSON.parse(raw);
            } catch {
                return raw;
            }
        })() : null;

        if (!response.ok) {
            const message = typeof data === 'object' && data && 'message' in data
                ? String((data as Record<string, unknown>).message)
                : `Request failed with status ${response.status}`;
            const error = new Error(message) as Error & { status?: number; data?: unknown };
            error.status = response.status;
            error.data = data;
            throw error;
        }

        return data;
    };

    return {
        get(path: string, query?: QueryParams) {
            return request('GET', path, { query });
        },
        post(path: string, body?: unknown) {
            return request('POST', path, { body });
        },
        put(path: string, body?: unknown) {
            return request('PUT', path, { body });
        },
        delete(path: string, body?: unknown) {
            return request('DELETE', path, { body });
        }
    };
}

function createEntityApi(http: ReturnType<typeof createHttpClient>, appId: string) {
    return new Proxy({}, {
        get(_, entity) {
            if (typeof entity !== 'string') return undefined;
            const basePath = `/apps/${appId}/entities/${entity}`;

            return {
                list(sort?: string, limit?: number, skip?: number, fields?: string[] | string) {
                    const query: QueryParams = {};
                    if (sort) query.sort = sort;
                    if (typeof limit === 'number') query.limit = limit;
                    if (typeof skip === 'number') query.skip = skip;
                    if (fields) query.fields = Array.isArray(fields) ? fields.join(',') : fields;
                    return http.get(basePath, query);
                },
                filter(where: unknown = {}, sort?: string, limit?: number, skip?: number, fields?: string[] | string) {
                    const query: QueryParams = { q: JSON.stringify(where || {}) };
                    if (sort) query.sort = sort;
                    if (typeof limit === 'number') query.limit = limit;
                    if (typeof skip === 'number') query.skip = skip;
                    if (fields) query.fields = Array.isArray(fields) ? fields.join(',') : fields;
                    return http.get(basePath, query);
                },
                get(id: string) {
                    return http.get(`${basePath}/${id}`);
                },
                create(payload: unknown) {
                    return http.post(basePath, payload);
                },
                update(id: string, payload: unknown) {
                    return http.put(`${basePath}/${id}`, payload);
                },
                delete(id: string) {
                    return http.delete(`${basePath}/${id}`);
                }
            };
        }
    });
}

function createFunctionsApi(http: ReturnType<typeof createHttpClient>, appId: string) {
    return {
        invoke(functionName: string, payload: Record<string, unknown> = {}) {
            const body = hasFileLike(payload) ? toFormData(payload) : payload;
            return http.post(`/apps/${appId}/functions/${functionName}`, body);
        }
    };
}

function createIntegrationsApi(http: ReturnType<typeof createHttpClient>, appId: string) {
    return new Proxy({}, {
        get(_, integrationName) {
            if (typeof integrationName !== 'string') return undefined;
            return new Proxy({}, {
                get(__, operationName) {
                    if (typeof operationName !== 'string') return undefined;
                    return (payload: Record<string, unknown> = {}) => {
                        const body = hasFileLike(payload) ? toFormData(payload) : payload;
                        const path = integrationName === 'Core'
                            ? `/apps/${appId}/integration-endpoints/Core/${operationName}`
                            : `/apps/${appId}/integration-endpoints/installable/${integrationName}/integration-endpoints/${operationName}`;
                        return http.post(path, body);
                    };
                }
            });
        }
    });
}

function createClient(
    baseUrl: string,
    appId: string,
    token?: string,
    extraHeaders: Record<string, string> = {},
    includeServiceRole = true
) {
    const http = createHttpClient(baseUrl, appId, token, extraHeaders);
    const client = {
        auth: {
            me() {
                return http.get(`/apps/${appId}/entities/User/me`);
            }
        },
        entities: createEntityApi(http, appId),
        functions: createFunctionsApi(http, appId),
        integrations: createIntegrationsApi(http, appId),
        connectors: {
            getAccessToken(type: string) {
                return http.get(`/apps/${appId}/external-auth/tokens/${type}`);
            }
        }
    };

    const fullClient = {
        ...client,
        asServiceRole: includeServiceRole
            ? createClient(baseUrl, appId, token, { ...extraHeaders, 'X-Service-Role': 'true' }, false)
            : client
    };
    return fullClient;
}

export function createClientFromRequest(req: Request) {
    const url = new URL(req.url);
    const appId = req.headers.get('X-App-Id')
        || url.searchParams.get('app_id')
        || Deno.env.get('CIVANT_APP_ID')
        || '';

    if (!appId) {
        throw new Error('Civant app id is required. Provide X-App-Id header or CIVANT_APP_ID env var.');
    }

    const baseUrl = Deno.env.get('CIVANT_API_BASE_URL')
        || Deno.env.get('CIVANT_APP_BASE_URL')
        || `${url.protocol}//${url.host}`;

    const token = req.headers.get('Authorization') || undefined;
    return createClient(baseUrl, appId, token);
}
