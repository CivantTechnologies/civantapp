import { createClient } from '@/lib/civant-sdk';
import { appParams } from '@/lib/app-params';

const { appId, token, functionsVersion, appBaseUrl } = appParams;
const persistedToken = typeof window !== 'undefined'
  ? (window.localStorage.getItem('civant_access_token') || window.localStorage.getItem('token') || '')
  : '';
const effectiveToken = token || persistedToken || '';
const env = /** @type {any} */ (import.meta).env || {};
const apiBaseUrl = env.VITE_API_BASE_URL || '/api';

export const civant = createClient({
  appId,
  token: effectiveToken,
  functionsVersion,
  serverUrl: apiBaseUrl,
  requiresAuth: false,
  appBaseUrl
});
