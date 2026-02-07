import axios from 'axios';

const hasFileLike = (value) => {
    if (!value) return false;
    if (typeof File !== 'undefined' && value instanceof File) return true;
    if (typeof Blob !== 'undefined' && value instanceof Blob) return true;
    if (Array.isArray(value)) return value.some(hasFileLike);
    if (typeof value === 'object') return Object.values(value).some(hasFileLike);
    return false;
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

const createEntityApi = (http, appId) => new Proxy({}, {
    get(_, entityName) {
        if (typeof entityName !== 'string' || entityName === 'then' || entityName.startsWith('_')) {
            return undefined;
        }

        const basePath = `/apps/${appId}/entities/${entityName}`;
        return {
            list(sort, limit, skip, fields) {
                const params = {};
                if (sort) params.sort = sort;
                if (typeof limit === 'number') params.limit = limit;
                if (typeof skip === 'number') params.skip = skip;
                if (fields) params.fields = Array.isArray(fields) ? fields.join(',') : fields;
                return http.get(basePath, { params });
            },
            filter(query = {}, sort, limit, skip, fields) {
                const params = { q: JSON.stringify(query || {}) };
                if (sort) params.sort = sort;
                if (typeof limit === 'number') params.limit = limit;
                if (typeof skip === 'number') params.skip = skip;
                if (fields) params.fields = Array.isArray(fields) ? fields.join(',') : fields;
                return http.get(basePath, { params });
            },
            get(id) {
                return http.get(`${basePath}/${id}`);
            },
            create(payload) {
                return http.post(basePath, payload);
            },
            update(id, payload) {
                return http.put(`${basePath}/${id}`, payload);
            },
            delete(id) {
                return http.delete(`${basePath}/${id}`);
            },
            deleteMany(payload) {
                return http.delete(basePath, { data: payload });
            },
            bulkCreate(payload) {
                return http.post(`${basePath}/bulk`, payload);
            }
        };
    }
});

const createFunctionsApi = (http, appId) => ({
    invoke(functionName, payload) {
        if (hasFileLike(payload)) {
            const data = toFormData(payload);
            return http.post(`/apps/${appId}/functions/${functionName}`, data, {
                headers: { 'Content-Type': 'multipart/form-data' }
            });
        }
        return http.post(`/apps/${appId}/functions/${functionName}`, payload || {});
    }
});

const createIntegrationsApi = (http, appId) => new Proxy({}, {
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

    if (token) {
        client.defaults.headers.common.Authorization = `Bearer ${token}`;
    }

    if (interceptResponses) {
        client.interceptors.response.use(
            (response) => response.data,
            (error) => {
                const normalizedError = /** @type {any} */ (new Error(
                    error?.response?.data?.message || error?.message || 'Request failed'
                ));
                normalizedError.status = error?.response?.status;
                normalizedError.code = error?.response?.data?.code;
                normalizedError.data = error?.response?.data;
                normalizedError.originalError = error;

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
    const baseURL = serverUrl || appBaseUrl || '';
    const http = createAxiosClient({ baseURL, token, interceptResponses: true });

    const setToken = (nextToken, persist = true) => {
        if (nextToken) {
            http.defaults.headers.common.Authorization = `Bearer ${nextToken}`;
            if (persist && typeof window !== 'undefined' && window.localStorage) {
                window.localStorage.setItem('civant_access_token', nextToken);
                window.localStorage.setItem('token', nextToken);
            }
        } else {
            delete http.defaults.headers.common.Authorization;
        }
    };

    const auth = {
        me() {
            return http.get(`/apps/${appId}/entities/User/me`);
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
            }
            delete http.defaults.headers.common.Authorization;
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
        entities: createEntityApi(http, appId),
        functions: createFunctionsApi(http, appId),
        integrations: createIntegrationsApi(http, appId),
        connectors: {
            getAccessToken(integrationType) {
                return http.get(`/apps/${appId}/external-auth/tokens/${integrationType}`);
            }
        },
        appLogs: {
            logUserInApp(pageName) {
                return http.post(`/app-logs/${appId}/log-user-in-app/${pageName}`);
            }
        }
    });

    client.asServiceRole = client;
    return client;
};
