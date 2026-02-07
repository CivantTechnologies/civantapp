import { createClient } from '@/lib/civant-sdk';
import { appParams } from '@/lib/app-params';

const { appId, token, functionsVersion, appBaseUrl } = appParams;
const persistedToken = typeof window !== 'undefined'
  ? (window.localStorage.getItem('civant_access_token') || window.localStorage.getItem('token') || '')
  : '';
const effectiveToken = token || persistedToken || '';

export const civant = createClient({
  appId,
  token: effectiveToken,
  functionsVersion,
  serverUrl: '',
  requiresAuth: false,
  appBaseUrl
});
