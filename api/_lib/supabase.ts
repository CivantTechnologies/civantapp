import { createClient } from '@supabase/supabase-js';

let cachedClient: ReturnType<typeof createClient> | null = null;

function requiredEnv(name: string) {
  const value = String(process.env[name] || '').trim();
  if (!value) {
    throw Object.assign(new Error(`Missing required environment variable: ${name}`), { status: 500 });
  }
  return value;
}

export function getServerSupabase() {
  if (cachedClient) return cachedClient;

  const supabaseUrl = requiredEnv('SUPABASE_URL');
  const serviceRoleKey = requiredEnv('SUPABASE_SERVICE_ROLE_KEY');

  cachedClient = createClient(supabaseUrl, serviceRoleKey, {
    auth: {
      autoRefreshToken: false,
      persistSession: false
    }
  });

  return cachedClient;
}
