import axios from 'axios';
import { sendClientTelemetry } from '@/lib/client-telemetry';

const ACTIVE_TENANT_STORAGE_KEY = 'civant_active_tenant';
const SLOW_API_THRESHOLD_MS = 2500;

function monotonicNowMs() {
    if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
        return performance.now();
    }
    return Date.now();
}

function normalizeValue(value) {
    const text = String(value ?? '').trim();
    if (!text) return '';
    const lowered = text.toLowerCase();
    if (lowered === 'null' || lowered === 'undefined') return '';
    return text;
}

function createClientError(code, message) {
    const error = /** @type {any} */ (new Error(message));
    error.code = code;
    return error;
}

const hasFileLike = (value) => {
    if (!value) return false;
    if (typeof File !== 'undefined' && value instanceof File) return true;
    if (typeof Blob !== 'undefined' && value instanceof Blob) return true;
    if (Array.isArray(value)) return value.some(hasFileLike);
    if (typeof value === 'object') return Object.values(value).some(hasFileLike);
    return false;
};

const isMissingColumnError = (error) => {
    const message = String(error?.data?.error || error?.message || '').toLowerCase();
    return message.includes('does not exist') || message.includes('unknown column');
};

const withSortFallback = async (request, fallbackRequest, sort) => {
    try {
        return await request();
    } catch (error) {
        if (!sort || !isMissingColumnError(error)) {
            throw error;
        }
        return fallbackRequest();
    }
};

const toFormData = (input) => {
    if (input instanceof FormData) return input;

    const formData = new FormData();
    Object.entries(input || {}).forEach(([key, value]) => {
        if (value === undefined || value === null) return;
        if ((typeof File !== 'undefined' && value instanceof File) || (typeof Blob !== 'undefined' && value instanceof Blob)) {
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
};

const createEntityApi = (http, getAppId, getOptionalTenantHeaders) => new Proxy({}, {
    get(_, entityName) {
        if (typeof entityName !== 'string' || entityName === 'then' || entityName.startsWith('_')) {
            return undefined;
        }

        const getBasePath = () => `/apps/${getAppId()}/entities/${entityName}`;
        const withTenantHeaders = (config = {}) => ({
            ...config,
            headers: {
                ...(config.headers || {}),
                ...(typeof getOptionalTenantHeaders === 'function' ? getOptionalTenantHeaders() : {})
            }
        });
        return {
            list(sort, limit, skip, fields) {
                const params = {};
                if (sort) params.sort = sort;
                if (typeof limit === 'number') params.limit = limit;
                if (typeof skip === 'number') params.skip = skip;
                if (fields) params.fields = Array.isArray(fields) ? fields.join(',') : fields;
                const config = withTenantHeaders({ params });
                return withSortFallback(
                    () => http.get(getBasePath(), config),
                    () => {
                        const retryParams = { ...params };
                        delete retryParams.sort;
                        return http.get(getBasePath(), withTenantHeaders({ params: retryParams }));
                    },
                    sort
                );
            },
            filter(query = {}, sort, limit, skip, fields) {
                /** @type {Record<string, any>} */
                const params = { q: JSON.stringify(query || {}) };
                if (sort) params.sort = sort;
                if (typeof limit === 'number') params.limit = limit;
                if (typeof skip === 'number') params.skip = skip;
                if (fields) params.fields = Array.isArray(fields) ? fields.join(',') : fields;
                const config = withTenantHeaders({ params });
                return withSortFallback(
                    () => http.get(getBasePath(), config),
                    () => {
                        const retryParams = { ...params };
                        delete retryParams.sort;
                        return http.get(getBasePath(), withTenantHeaders({ params: retryParams }));
                    },
                    sort
                );
            },
            get(id) {
                return http.get(`${getBasePath()}/${id}`, withTenantHeaders());
            },
            create(payload) {
                return http.post(getBasePath(), payload, withTenantHeaders());
            },
            update(id, payload) {
                return http.put(`${getBasePath()}/${id}`, payload, withTenantHeaders());
            },
            delete(id) {
                return http.delete(`${getBasePath()}/${id}`, withTenantHeaders());
            },
            deleteMany(payload) {
                return http.delete(getBasePath(), withTenantHeaders({ data: payload }));
            },
            bulkCreate(payload) {
                return http.post(`${getBasePath()}/bulk`, payload, withTenantHeaders());
            }
        };
    }
});

const createFunctionsApi = (http, getAppId, getOptionalTenantHeaders) => ({
    invoke(functionName, payload, options = {}) {
        const headers = options.headers || undefined;
        const endpoint = `/apps/${getAppId()}/functions/${functionName}`;
        const withTenantHeaders = (nextHeaders = {}) => ({
            ...(typeof getOptionalTenantHeaders === 'function' ? getOptionalTenantHeaders() : {}),
            ...(nextHeaders || {})
        });
        if (hasFileLike(payload)) {
            const data = toFormData(payload);
            return http.post(endpoint, data, {
                headers: { 'Content-Type': 'multipart/form-data', ...withTenantHeaders(headers || {}) }
            });
        }
        return http.post(endpoint, payload || {}, {
            headers: withTenantHeaders(headers || {})
        });
    }
});

const createIntegrationsApi = (http, getAppId) => new Proxy({}, {
    get(_, integrationName) {
        if (typeof integrationName !== 'string' || integrationName === 'then' || integrationName.startsWith('_')) {
            return undefined;
        }

        return new Proxy({}, {
            get(__, operationName) {
                if (typeof operationName !== 'string' || operationName === 'then' || operationName.startsWith('_')) {
                    return undefined;
                }

                return async (payload = {}) => {
                    const appId = getAppId();
                    const endpoint = integrationName === 'Core'
                        ? `/apps/${appId}/integration-endpoints/Core/${operationName}`
                        : `/apps/${appId}/integration-endpoints/installable/${integrationName}/integration-endpoints/${operationName}`;

                    if (hasFileLike(payload)) {
                        const data = toFormData(payload);
                        return http.post(endpoint, data, {
                            headers: { 'Content-Type': 'multipart/form-data' }
                        });
                    }

                    return http.post(endpoint, payload);
                };
            }
        });
    }
});

/**
 * @param {{
 *   baseURL?: string;
 *   headers?: Record<string, string>;
 *   token?: string;
 *   interceptResponses?: boolean;
 *   onError?: (error: any) => void;
 * }} [config]
 * @returns {any}
 */
export const createAxiosClient = ({
    baseURL,
    headers = {},
    token,
    interceptResponses = true,
    onError
} = {}) => {
    const client = axios.create({
        baseURL,
        headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json',
            ...headers
        }
    });

    client.interceptors.request.use((requestConfig) => {
        const mutableConfig = /** @type {any} */ (requestConfig);
        mutableConfig.metadata = mutableConfig.metadata || {};
        mutableConfig.metadata.startedAtMs = monotonicNowMs();
        return mutableConfig;
    });

    if (token) {
        client.defaults.headers.common.Authorization = `Bearer ${token}`;
    }

    if (interceptResponses) {
        client.interceptors.response.use(
            (response) => {
                const responseConfig = /** @type {any} */ (response?.config || {});
                const startedAtMs = Number(responseConfig?.metadata?.startedAtMs || 0);
                if (startedAtMs > 0) {
                    const durationMs = Math.round(monotonicNowMs() - startedAtMs);
                    if (durationMs >= SLOW_API_THRESHOLD_MS) {
                        sendClientTelemetry({
                            event_type: 'slow_api',
                            severity: 'warning',
                            path: typeof window !== 'undefined' ? window.location.pathname : '',
                            message: `${String(response?.config?.method || 'get').toUpperCase()} ${String(response?.config?.url || '')}`,
                            status_code: Number(response?.status) || null,
                            duration_ms: durationMs
                        });
                    }
                }
                return response.data;
            },
            (error) => {
                const errorConfig = /** @type {any} */ (error?.config || {});
                const startedAtMs = Number(errorConfig?.metadata?.startedAtMs || 0);
                const durationMs = startedAtMs > 0 ? Math.round(monotonicNowMs() - startedAtMs) : null;
                const normalizedError = /** @type {any} */ (new Error(
                    error?.response?.data?.message || error?.response?.data?.error || error?.message || 'Request failed'
                ));
                normalizedError.status = error?.response?.status;
                normalizedError.code = error?.response?.data?.code;
                normalizedError.data = error?.response?.data;
                normalizedError.originalError = error;

                sendClientTelemetry({
                    event_type: 'api_error',
                    severity: 'error',
                    path: typeof window !== 'undefined' ? window.location.pathname : '',
                    message: `${String(error?.config?.method || 'get').toUpperCase()} ${String(error?.config?.url || '')} :: ${normalizedError.message}`,
                    stack: normalizedError.stack || '',
                    status_code: Number(normalizedError.status) || null,
                    duration_ms: durationMs
                });

                if (typeof onError === 'function') {
                    onError(normalizedError);
                }
                return Promise.reject(normalizedError);
            }
        );
    }

    return client;
};

/**
 * @param {{
 *   appId: string;
 *   token?: string;
 *   appBaseUrl?: string;
 *   serverUrl?: string;
 *   functionsVersion?: string;
 *   requiresAuth?: boolean;
 * }} config
 * @returns {any}
 */
export const createClient = ({
    appId,
    token,
    appBaseUrl,
    serverUrl = '',
    functionsVersion,
    requiresAuth
}) => {
    void functionsVersion;
    void requiresAuth;

    let resolvedAppId = normalizeValue(appId);
    const baseURL = serverUrl || appBaseUrl || '';
    const http = createAxiosClient({ baseURL, token, interceptResponses: true });

    const ensureAppId = () => {
        if (!resolvedAppId) {
            throw createClientError(
                'MISSING_APP_ID',
                'Missing Civant app id (VITE_CIVANT_APP_ID). Configure runtime env before making API calls.'
            );
        }
        return resolvedAppId;
    };

    let activeTenantId = typeof window !== 'undefined' && window.localStorage
        ? normalizeValue(window.localStorage.getItem(ACTIVE_TENANT_STORAGE_KEY))
        : '';

    const setActiveTenantId = (tenantId, persist = true, options = {}) => {
        const fallbackToDefault = options.fallbackToDefault !== false;
        const normalized = normalizeValue(tenantId).toLowerCase();
        activeTenantId = normalized || (fallbackToDefault ? 'civant_default' : '');

        if (typeof window !== 'undefined' && window.localStorage) {
            if (persist && activeTenantId) {
                window.localStorage.setItem(ACTIVE_TENANT_STORAGE_KEY, activeTenantId);
            } else if (!activeTenantId) {
                window.localStorage.removeItem(ACTIVE_TENANT_STORAGE_KEY);
            }
        }
    };

    const getActiveTenantId = () => activeTenantId;

    const tenantHeaders = (headers = {}) => {
        const effectiveTenantId = getActiveTenantId();
        if (!effectiveTenantId) {
            throw createClientError('MISSING_TENANT_ID', 'Select a tenant before making tenant-scoped requests.');
        }

        return {
            ...headers,
            'x-tenant-id': effectiveTenantId
        };
    };

    const optionalTenantHeaders = (headers = {}) => {
        const effectiveTenantId = getActiveTenantId();
        if (!effectiveTenantId) {
            return { ...headers };
        }
        return {
            ...headers,
            'x-tenant-id': effectiveTenantId
        };
    };

    const functionsApi = createFunctionsApi(http, ensureAppId, optionalTenantHeaders);

    const setToken = (nextToken, persist = true) => {
        if (nextToken) {
            http.defaults.headers.common.Authorization = `Bearer ${nextToken}`;
            if (persist && typeof window !== 'undefined' && window.localStorage) {
                window.localStorage.setItem('civant_access_token', nextToken);
                window.localStorage.setItem('token', nextToken);
            }
        } else {
            delete http.defaults.headers.common.Authorization;
            if (typeof window !== 'undefined' && window.localStorage) {
                window.localStorage.removeItem('civant_access_token');
                window.localStorage.removeItem('token');
            }
        }
    };

    const auth = {
        me() {
            return http.get(`/apps/${ensureAppId()}/entities/User/me`);
        },
        createSession(email, tenantId) {
            return functionsApi.invoke('createSession', { email, tenantId });
        },
        getCurrentUser() {
            return functionsApi.invoke('getCurrentUser', {});
        },
        getMyProfile() {
            return functionsApi.invoke('getMyProfile', {});
        },
        setToken,
        redirectToLogin(fromUrl) {
            if (typeof window === 'undefined') return;
            const source = fromUrl || window.location.href;
            const loginUrl = `${appBaseUrl || ''}/login?from_url=${encodeURIComponent(source)}`;
            window.location.href = loginUrl;
        },
        logout(redirectUrl) {
            if (typeof window !== 'undefined' && window.localStorage) {
                window.localStorage.removeItem('civant_access_token');
                window.localStorage.removeItem('token');
                window.localStorage.removeItem(ACTIVE_TENANT_STORAGE_KEY);
            }
            delete http.defaults.headers.common.Authorization;
            activeTenantId = '';
            if (typeof window !== 'undefined') {
                if (redirectUrl) {
                    window.location.href = redirectUrl;
                } else {
                    window.location.reload();
                }
            }
        }
    };

    const client = /** @type {any} */ ({
        auth,
        setActiveTenantId,
        getActiveTenantId,
        setAppId(nextAppId) {
            resolvedAppId = normalizeValue(nextAppId);
        },
        entities: createEntityApi(http, ensureAppId, optionalTenantHeaders),
        functions: functionsApi,
        integrations: createIntegrationsApi(http, ensureAppId),
        system: {
            listTenants() {
                return functionsApi.invoke('listTenants', {});
            },
            createTenant(payload = {}) {
                return functionsApi.invoke('createTenant', payload);
            },
            getCapabilities() {
                return functionsApi.invoke('getCapabilities', {}, { headers: tenantHeaders() });
            },
            getTenant() {
                return functionsApi.invoke('getTenant', {}, { headers: tenantHeaders() });
            },
            listTenantUsers() {
                return functionsApi.invoke('listTenantUsers', {}, { headers: tenantHeaders() });
            },
            getConnectorStatus() {
                return functionsApi.invoke('getConnectorStatus', {}, { headers: tenantHeaders() });
            },
            enableSupportAccess(payload = {}) {
                return functionsApi.invoke('enableSupportAccess', payload, { headers: tenantHeaders() });
            },
            revokeSupportAccess(payload = {}) {
                return functionsApi.invoke('revokeSupportAccess', payload, { headers: tenantHeaders() });
            },
            getSupportAccessStatus(payload = {}) {
                return functionsApi.invoke('getSupportAccessStatus', payload, { headers: tenantHeaders() });
            },
            listSupportAccessAudit(payload = {}) {
                return functionsApi.invoke('listSupportAccessAudit', payload, { headers: tenantHeaders() });
            }
        },
        connectors: {
            getAccessToken(integrationType) {
                return http.get(`/apps/${ensureAppId()}/external-auth/tokens/${integrationType}`);
            }
        },
        appLogs: {
            logUserInApp(pageName) {
                return http.post(`/app-logs/${ensureAppId()}/log-user-in-app/${pageName}`);
            }
        }
    });

    client.asServiceRole = client;
    return client;
};
