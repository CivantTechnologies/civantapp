import { createClient } from '@/lib/civant-sdk';
import { appParams } from '@/lib/app-params';
import { runtimeConfig } from '@/config';

const { appId, token, functionsVersion, appBaseUrl } = appParams;
const persistedToken = typeof window !== 'undefined'
  ? (window.localStorage.getItem('civant_access_token') || window.localStorage.getItem('token') || '')
  : '';
const effectiveToken = token || persistedToken || '';
const effectiveAppId = appId || runtimeConfig.civantAppId || '';
const apiBaseUrl = runtimeConfig.apiBaseUrl || '/api';

export const civant = createClient({
  appId: effectiveAppId,
  token: effectiveToken,
  functionsVersion,
  serverUrl: apiBaseUrl,
  requiresAuth: false,
  appBaseUrl
});
