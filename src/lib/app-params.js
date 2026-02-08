import { normalizeRuntimeValue, runtimeConfig } from '@/config';

const viteEnv = /** @type {any} */ (import.meta.env);
const viteFunctionsVersion = normalizeRuntimeValue(viteEnv.VITE_CIVANT_FUNCTIONS_VERSION);
const viteAppBaseUrl = normalizeRuntimeValue(viteEnv.VITE_CIVANT_APP_BASE_URL);

const isNode = typeof window === 'undefined';
const memoryStore = new Map();
const memoryStorage = {
	setItem: (key, value) => memoryStore.set(key, String(value)),
	getItem: (key) => (memoryStore.has(key) ? memoryStore.get(key) : null),
	removeItem: (key) => memoryStore.delete(key),
};
const storage = isNode ? memoryStorage : window.localStorage;

const toSnakeCase = (str) => {
	return str.replace(/([A-Z])/g, '_$1').toLowerCase();
};

const normalizeParamValue = (value) => {
	return normalizeRuntimeValue(value);
};

const getAppParamValue = (paramName, { defaultValue = undefined, removeFromUrl = false } = {}) => {
	if (isNode) {
		return normalizeParamValue(defaultValue);
	}

	const storageKey = `civant_${toSnakeCase(paramName)}`;
	const urlParams = new URLSearchParams(window.location.search);
	const searchParam = normalizeParamValue(urlParams.get(paramName));

	if (removeFromUrl) {
		urlParams.delete(paramName);
		const newUrl = `${window.location.pathname}${urlParams.toString() ? `?${urlParams.toString()}` : ''}${window.location.hash}`;
		window.history.replaceState({}, document.title, newUrl);
	}

	if (searchParam) {
		storage.setItem(storageKey, searchParam);
		return searchParam;
	}

	const normalizedDefault = normalizeParamValue(defaultValue);
	if (normalizedDefault) {
		storage.setItem(storageKey, normalizedDefault);
		return normalizedDefault;
	}

	const storedValue = normalizeParamValue(storage.getItem(storageKey));
	if (storedValue) {
		return storedValue;
	}

	storage.removeItem(storageKey);
	return null;
};

const getAppParams = () => {
	if (getAppParamValue('clear_access_token') === 'true') {
		storage.removeItem('civant_access_token');
		storage.removeItem('token');
	}

	return {
		appId: getAppParamValue('app_id', { defaultValue: runtimeConfig.civantAppId }),
		token: getAppParamValue('access_token', { removeFromUrl: true }),
		fromUrl: getAppParamValue('from_url', { defaultValue: isNode ? '' : window.location.href }),
		functionsVersion: getAppParamValue('functions_version', { defaultValue: viteFunctionsVersion }),
		appBaseUrl: getAppParamValue('app_base_url', { defaultValue: viteAppBaseUrl }),
	};
};

export const appParams = {
	...getAppParams()
};
