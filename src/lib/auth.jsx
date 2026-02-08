import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { supabase } from '@/lib/supabaseClient';
import { civant } from '@/api/civantClient';

const AuthContext = createContext(null);
const PROFILE_CACHE_KEY = 'civant_last_profile';
const PROFILE_RETRY_DELAYS_MS = [0, 500, 2000, 5000];
const AUTH_INIT_TIMEOUT_MS = 12000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(promise, timeoutMs, message) {
  let timeoutId = null;
  const timeout = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(message)), timeoutMs);
  });

  return Promise.race([promise, timeout]).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
}

function unwrapResponse(response) {
  return response?.data ?? response ?? null;
}

function normalizeProfile(payload) {
  const profile = payload || {};
  const roles = Array.isArray(profile.roles)
    ? profile.roles.map((role) => String(role || '').toLowerCase()).filter(Boolean)
    : [];

  return {
    email: String(profile.email || '').trim().toLowerCase(),
    tenant_id: String(profile.tenant_id || '').trim().toLowerCase(),
    roles
  };
}

function loadCachedProfile() {
  if (typeof window === 'undefined' || !window.sessionStorage) return null;

  try {
    const raw = window.sessionStorage.getItem(PROFILE_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    return normalizeProfile(parsed);
  } catch {
    return null;
  }
}

function cacheProfile(profile) {
  if (typeof window === 'undefined' || !window.sessionStorage) return;

  if (!profile) {
    window.sessionStorage.removeItem(PROFILE_CACHE_KEY);
    return;
  }

  window.sessionStorage.setItem(PROFILE_CACHE_KEY, JSON.stringify(profile));
}

export function AuthProvider({ children }) {
  const [session, setSession] = useState(null);
  const [profile, setProfile] = useState(null);
  const [staleProfile, setStaleProfile] = useState(loadCachedProfile);
  const [profileStatus, setProfileStatus] = useState('idle');
  const [isLoadingAuth, setIsLoadingAuth] = useState(true);
  const [authError, setAuthError] = useState('');
  const [authWarning, setAuthWarning] = useState('');
  const [profileRetryCount, setProfileRetryCount] = useState(0);

  const clearClientAuth = () => {
    civant.auth.setToken(null, false);
    civant.setActiveTenantId('', true, { fallbackToDefault: false });
    setProfile(null);
    setStaleProfile(null);
    setProfileStatus('idle');
    setProfileRetryCount(0);
    setAuthWarning('');
    cacheProfile(null);
  };

  const loadProfileWithRetry = async (accessToken, sessionSnapshot = null) => {
    if (!accessToken) {
      clearClientAuth();
      return { ok: false, error: 'Missing access token' };
    }

    civant.auth.setToken(accessToken, true);
    setProfileStatus('loading');
    setProfileRetryCount(0);
    setAuthWarning('');

    let lastError = null;
    for (let attempt = 0; attempt < PROFILE_RETRY_DELAYS_MS.length; attempt += 1) {
      if (attempt > 0) {
        await sleep(PROFILE_RETRY_DELAYS_MS[attempt]);
      }

      setProfileRetryCount(attempt);

      try {
        const payload = unwrapResponse(await civant.auth.getMyProfile());
        const normalized = normalizeProfile(payload);

        setProfile(normalized);
        setStaleProfile(normalized);
        setProfileStatus('ready');
        setProfileRetryCount(attempt);
        setAuthWarning('');
        cacheProfile(normalized);

        return { ok: true };
      } catch (error) {
        lastError = error;
      }
    }

    const sessionEmail = String(sessionSnapshot?.user?.email || '').trim().toLowerCase();
    const previousProfile = profile || staleProfile || loadCachedProfile();
    const stale = previousProfile && (!sessionEmail || previousProfile.email === sessionEmail)
      ? previousProfile
      : null;

    setProfile(null);
    setStaleProfile(stale);
    setProfileStatus('error');
    setAuthWarning(lastError?.message || 'Unable to confirm permissions. Retry or sign out.');

    return { ok: false, error: lastError?.message || 'Failed to load profile' };
  };

  const retryProfile = async () => {
    const accessToken = session?.access_token;
    if (!accessToken) {
      return { ok: false, error: 'No active session' };
    }

    return loadProfileWithRetry(accessToken, session);
  };

  useEffect(() => {
    let mounted = true;

    const bootstrap = async () => {
      setIsLoadingAuth(true);
      setAuthError('');

      try {
        const { data, error } = await withTimeout(
          supabase.auth.getSession(),
          AUTH_INIT_TIMEOUT_MS,
          'Auth session check timed out. Verify Supabase URL/key and network access.'
        );
        if (error) throw error;
        if (!mounted) return;

        const nextSession = data?.session ?? null;
        setSession(nextSession);

        if (nextSession?.access_token) {
          await withTimeout(
            loadProfileWithRetry(nextSession.access_token, nextSession),
            AUTH_INIT_TIMEOUT_MS,
            'Profile check timed out. Retry to continue.'
          );
        } else {
          clearClientAuth();
        }
      } catch (error) {
        if (!mounted) return;
        clearClientAuth();
        setSession(null);
        setAuthError(error?.message || 'Failed to initialise authentication');
      } finally {
        if (mounted) setIsLoadingAuth(false);
      }
    };

    bootstrap();

    const { data: subscription } = supabase.auth.onAuthStateChange(async (_event, nextSession) => {
      if (!mounted) return;

      setSession(nextSession || null);
      setIsLoadingAuth(true);
      setAuthError('');

      try {
        if (nextSession?.access_token) {
          await withTimeout(
            loadProfileWithRetry(nextSession.access_token, nextSession),
            AUTH_INIT_TIMEOUT_MS,
            'Profile check timed out. Retry to continue.'
          );
        } else {
          clearClientAuth();
        }
      } catch (error) {
        clearClientAuth();
        setAuthError(error?.message || 'Failed to load profile');
      } finally {
        if (mounted) setIsLoadingAuth(false);
      }
    });

    return () => {
      mounted = false;
      subscription?.subscription?.unsubscribe();
    };
  }, []);

  const loginWithPassword = async ({ email, password }) => {
    setIsLoadingAuth(true);
    setAuthError('');

    const { error } = await supabase.auth.signInWithPassword({ email, password });
    if (error) {
      setIsLoadingAuth(false);
      setAuthError(error.message || 'Login failed');
      return { ok: false, error: error.message || 'Login failed' };
    }

    return { ok: true };
  };

  const logout = async () => {
    setIsLoadingAuth(true);
    setAuthError('');
    await supabase.auth.signOut();
    clearClientAuth();
    setSession(null);
    setIsLoadingAuth(false);
  };

  const effectiveRoles = profileStatus === 'ready' ? (profile?.roles || []) : null;
  const tenantId = profileStatus === 'ready' ? (profile?.tenant_id || '') : null;
  const isAuthenticated = Boolean(session?.access_token);
  const isPrivileged = Array.isArray(effectiveRoles) && (effectiveRoles.includes('admin') || effectiveRoles.includes('creator'));

  const value = useMemo(() => ({
    session,
    profile,
    staleProfile,
    profileStatus,
    profileRetryCount,
    currentUser: {
      email: profile?.email || session?.user?.email || staleProfile?.email || '',
      tenantId,
      roles: effectiveRoles
    },
    roles: effectiveRoles,
    tenantId,
    isPrivileged,
    isAuthenticated,
    isLoadingAuth,
    authError,
    authWarning,
    retryProfile,
    loginWithPassword,
    logout
  }), [
    session,
    profile,
    staleProfile,
    profileStatus,
    profileRetryCount,
    tenantId,
    isPrivileged,
    isAuthenticated,
    isLoadingAuth,
    authError,
    authWarning,
    JSON.stringify(effectiveRoles)
  ]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth must be used within AuthProvider');
  return context;
}
