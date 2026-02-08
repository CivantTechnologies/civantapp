const env = /** @type {any} */ (import.meta).env || {};

const normalize = (value) => {
  const text = String(value ?? '').trim();
  if (!text) return '';
  const lowered = text.toLowerCase();
  if (lowered === 'null' || lowered === 'undefined') return '';
  return text;
};

const parseBoolean = (value) => {
  const normalized = normalize(value).toLowerCase();
  return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on';
};

const missingRequired = [];

const supabaseUrl = normalize(env.VITE_SUPABASE_URL);
if (!supabaseUrl) missingRequired.push('VITE_SUPABASE_URL');

const supabaseAnonKey = normalize(env.VITE_SUPABASE_ANON_KEY);
if (!supabaseAnonKey) missingRequired.push('VITE_SUPABASE_ANON_KEY');

const civantAppId = normalize(env.VITE_CIVANT_APP_ID);
if (!civantAppId) missingRequired.push('VITE_CIVANT_APP_ID');

const apiBaseUrl = normalize(env.VITE_API_BASE_URL) || '/api';
const debug = parseBoolean(env.VITE_DEBUG);

let hasLoggedConfigStatus = false;

export const runtimeConfig = {
  supabaseUrl,
  supabaseAnonKey,
  civantAppId,
  apiBaseUrl,
  debug,
  missingRequired,
  isValid: missingRequired.length === 0
};

export function logRuntimeConfigStatus() {
  if (hasLoggedConfigStatus) return;
  hasLoggedConfigStatus = true;

  if (runtimeConfig.debug) {
    // eslint-disable-next-line no-console
    console.info('[Civant config] SUPABASE_URL present:', Boolean(runtimeConfig.supabaseUrl));
    // eslint-disable-next-line no-console
    console.info(
      '[Civant config] SUPABASE_ANON_KEY present:',
      Boolean(runtimeConfig.supabaseAnonKey),
      `(len=${runtimeConfig.supabaseAnonKey.length})`
    );
    // eslint-disable-next-line no-console
    console.info('[Civant config] CIVANT_APP_ID present:', Boolean(runtimeConfig.civantAppId));
    // eslint-disable-next-line no-console
    console.info('[Civant config] API_BASE_URL:', runtimeConfig.apiBaseUrl || '(empty)');
  }

  if (!runtimeConfig.isValid) {
    // eslint-disable-next-line no-console
    console.error('[Civant config] Missing required runtime env vars:', runtimeConfig.missingRequired.join(', '));
  }
}

export function normalizeRuntimeValue(value) {
  return normalize(value);
}
