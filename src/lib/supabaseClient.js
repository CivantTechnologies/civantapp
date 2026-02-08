import { createClient } from '@supabase/supabase-js';
import { runtimeConfig } from '@/config';

export const supabase = createClient(runtimeConfig.supabaseUrl || '', runtimeConfig.supabaseAnonKey || '');
