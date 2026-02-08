import { createClient } from '@supabase/supabase-js';
import { runtimeConfig } from '@/config';

function createDisabledSupabase(reason) {
  const error = new Error(reason);

  return {
    auth: {
      async getSession() {
        return { data: { session: null }, error };
      },
      onAuthStateChange() {
        return {
          data: {
            subscription: {
              unsubscribe() {}
            }
          }
        };
      },
      async signInWithPassword() {
        return { data: null, error };
      },
      async resetPasswordForEmail() {
        return { data: null, error };
      },
      async signOut() {
        return { error: null };
      }
    }
  };
}

export const supabase = runtimeConfig.isValid
  ? createClient(runtimeConfig.supabaseUrl, runtimeConfig.supabaseAnonKey)
  : createDisabledSupabase(
      `Supabase client unavailable: ${runtimeConfig.issues.join('; ') || 'missing runtime configuration'}`
    );
