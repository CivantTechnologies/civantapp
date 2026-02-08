import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import type { ApiDatabase } from './db.types.js';

let cachedClient: SupabaseClient<ApiDatabase> | null = null;

function requiredEnv(primary: string, fallback?: string) {
  const primaryValue = String(process.env[primary] || '').trim();
  const fallbackValue = fallback ? String(process.env[fallback] || '').trim() : '';
  const value = primaryValue || fallbackValue;
  if (!value) {
    const message = fallback
      ? `Missing required environment variable: ${primary} (or ${fallback})`
      : `Missing required environment variable: ${primary}`;
    throw Object.assign(new Error(message), { status: 500 });
  }
  return value;
}

export function getServerSupabase() {
  if (cachedClient) return cachedClient;

  const supabaseUrl = requiredEnv('SUPABASE_URL', 'VITE_SUPABASE_URL');
  const serviceRoleKey = requiredEnv('SUPABASE_SERVICE_ROLE_KEY');

  cachedClient = createClient<ApiDatabase>(supabaseUrl, serviceRoleKey, {
    auth: {
      autoRefreshToken: false,
      persistSession: false
    }
  });

  return cachedClient;
}
