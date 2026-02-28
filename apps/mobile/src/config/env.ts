const requiredPublicEnvVars = [
  "EXPO_PUBLIC_API_BASE_URL",
  "EXPO_PUBLIC_SUPABASE_URL",
  "EXPO_PUBLIC_SUPABASE_ANON_KEY",
] as const;

type PublicEnvKey = (typeof requiredPublicEnvVars)[number];

function readEnv(key: PublicEnvKey): string {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

export function getPublicEnv() {
  return {
    apiBaseUrl: readEnv("EXPO_PUBLIC_API_BASE_URL"),
    supabaseUrl: readEnv("EXPO_PUBLIC_SUPABASE_URL"),
    supabaseAnonKey: readEnv("EXPO_PUBLIC_SUPABASE_ANON_KEY"),
  };
}
