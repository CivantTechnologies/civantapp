const viteEnv = /** @type {any} */ (import.meta.env);

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

const issues = [];

const supabaseUrl = normalize(viteEnv.VITE_SUPABASE_URL);
if (!supabaseUrl) {
  issues.push('VITE_SUPABASE_URL is missing');
} else {
  try {
    const url = new URL(supabaseUrl);
    if (!['http:', 'https:'].includes(url.protocol)) {
      issues.push('VITE_SUPABASE_URL must be an absolute http(s) URL');
    }
  } catch {
    issues.push('VITE_SUPABASE_URL is not a valid URL');
  }
}

const supabaseAnonKey = normalize(viteEnv.VITE_SUPABASE_ANON_KEY);
if (!supabaseAnonKey) issues.push('VITE_SUPABASE_ANON_KEY is missing');

const civantAppId = normalize(viteEnv.VITE_CIVANT_APP_ID);
if (!civantAppId) issues.push('VITE_CIVANT_APP_ID is missing');

const apiBaseUrl = normalize(viteEnv.VITE_API_BASE_URL) || '/api';
if (apiBaseUrl && !apiBaseUrl.startsWith('/')) {
  try {
    const parsed = new URL(apiBaseUrl);
    if (!['http:', 'https:'].includes(parsed.protocol)) {
      issues.push('VITE_API_BASE_URL must be relative (/api) or absolute http(s)');
    }
  } catch {
    issues.push('VITE_API_BASE_URL is invalid');
  }
}
const debug = parseBoolean(viteEnv.VITE_DEBUG);

let hasLoggedConfigStatus = false;

export const runtimeConfig = {
  supabaseUrl,
  supabaseAnonKey,
  civantAppId,
  apiBaseUrl,
  debug,
  issues,
  isValid: issues.length === 0
};

export function logRuntimeConfigStatus() {
  if (hasLoggedConfigStatus) return;
  hasLoggedConfigStatus = true;

  if (runtimeConfig.debug) {
    console.info('[Civant config] SUPABASE_URL present:', Boolean(runtimeConfig.supabaseUrl));
    console.info(
      '[Civant config] SUPABASE_ANON_KEY present:',
      Boolean(runtimeConfig.supabaseAnonKey),
      `(len=${runtimeConfig.supabaseAnonKey.length})`
    );
    console.info('[Civant config] CIVANT_APP_ID present:', Boolean(runtimeConfig.civantAppId));
    console.info('[Civant config] API_BASE_URL:', runtimeConfig.apiBaseUrl || '(empty)');
  }

  if (!runtimeConfig.isValid) {
    console.error('[Civant config] Invalid runtime configuration:', runtimeConfig.issues.join('; '));
  }
}

export function normalizeRuntimeValue(value) {
  return normalize(value);
}
