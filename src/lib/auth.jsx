import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { supabase } from '@/lib/supabaseClient';
import { civant } from '@/api/civantClient';

const AuthContext = createContext(null);

function unwrapResponse(response) {
  return response?.data ?? response ?? null;
}

function normalizeProfile(payload) {
  const profile = payload || {};
  const roles = Array.isArray(profile.roles)
    ? profile.roles.map((role) => String(role || '').toLowerCase()).filter(Boolean)
    : [];

  return {
    email: String(profile.email || ''),
    tenant_id: String(profile.tenant_id || ''),
    roles
  };
}

export function AuthProvider({ children }) {
  const [session, setSession] = useState(null);
  const [profile, setProfile] = useState(null);
  const [isLoadingAuth, setIsLoadingAuth] = useState(true);
  const [authError, setAuthError] = useState('');

  const clearClientAuth = () => {
    civant.auth.setToken(null, false);
    setProfile(null);
  };

  const loadProfile = async (accessToken) => {
    if (!accessToken) {
      clearClientAuth();
      return;
    }

    civant.auth.setToken(accessToken, true);
    const payload = unwrapResponse(await civant.auth.getMyProfile());
    setProfile(normalizeProfile(payload));
  };

  useEffect(() => {
    let mounted = true;

    const bootstrap = async () => {
      setIsLoadingAuth(true);
      setAuthError('');
      try {
        const { data, error } = await supabase.auth.getSession();
        if (error) throw error;
        if (!mounted) return;

        const nextSession = data?.session ?? null;
        setSession(nextSession);
        if (nextSession?.access_token) {
          await loadProfile(nextSession.access_token);
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
          await loadProfile(nextSession.access_token);
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

  const roles = profile?.roles || [];
  const tenantId = profile?.tenant_id || '';
  const isAuthenticated = Boolean(session?.access_token && profile?.email);
  const isPrivileged = roles.includes('admin') || roles.includes('creator');

  const value = useMemo(() => ({
    session,
    profile,
    currentUser: {
      email: profile?.email || '',
      tenantId,
      roles
    },
    roles,
    tenantId,
    isPrivileged,
    isAuthenticated,
    isLoadingAuth,
    authError,
    loginWithPassword,
    logout
  }), [session, profile, tenantId, roles.join(','), isPrivileged, isAuthenticated, isLoadingAuth, authError]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth must be used within AuthProvider');
  return context;
}
