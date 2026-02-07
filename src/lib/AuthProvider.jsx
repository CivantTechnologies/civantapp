import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { civant } from '@/api/civantClient';

const AuthContext = createContext(null);

function unwrapResponse(response) {
  return response?.data ?? response ?? null;
}

function clearStoredToken() {
  if (typeof window === 'undefined') return;
  window.localStorage.removeItem('civant_access_token');
  window.localStorage.removeItem('token');
}

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoadingAuth, setIsLoadingAuth] = useState(true);
  const [authError, setAuthError] = useState('');

  const [capabilities, setCapabilities] = useState({ isAdmin: false, tenantId: 'civant_default' });
  const [tenantInfo, setTenantInfo] = useState(null);
  const [isLoadingCapabilities, setIsLoadingCapabilities] = useState(false);

  const hydrateFromToken = async () => {
    setIsLoadingAuth(true);
    setAuthError('');

    try {
      const me = unwrapResponse(await civant.auth.getCurrentUser());
      if (!me?.userId) {
        throw new Error('Invalid user session');
      }

      const nextUser = {
        id: me.userId,
        userId: me.userId,
        email: me.email || '',
        role: me.role || 'user',
        tenantId: me.tenantId || 'civant_default'
      };

      setUser(nextUser);
      setIsAuthenticated(true);
      await loadCapabilities(nextUser.tenantId);
    } catch (error) {
      setUser(null);
      setIsAuthenticated(false);
      setCapabilities({ isAdmin: false, tenantId: 'civant_default' });
      setTenantInfo(null);
      clearStoredToken();
      civant.auth.setToken(null, false);

      const status = error?.status || error?.response?.status;
      if (status && status !== 401) {
        setAuthError(error?.message || 'Authentication failed');
      }
    } finally {
      setIsLoadingAuth(false);
    }
  };

  const loadCapabilities = async (tenantId) => {
    setIsLoadingCapabilities(true);
    try {
      const capabilitiesResp = unwrapResponse(await civant.system.getCapabilities(tenantId));
      const nextCapabilities = {
        isAdmin: Boolean(capabilitiesResp?.isAdmin),
        tenantId: capabilitiesResp?.tenantId || tenantId || 'civant_default'
      };
      setCapabilities(nextCapabilities);

      if (nextCapabilities.isAdmin) {
        const tenantResp = unwrapResponse(await civant.system.getTenant(nextCapabilities.tenantId));
        setTenantInfo(tenantResp || null);
      } else {
        setTenantInfo({
          tenantId: nextCapabilities.tenantId,
          name: 'Civant',
          regions: [],
          planTier: null,
          createdAt: null
        });
      }
    } catch {
      setCapabilities({ isAdmin: false, tenantId: tenantId || 'civant_default' });
      setTenantInfo(null);
    } finally {
      setIsLoadingCapabilities(false);
    }
  };

  useEffect(() => {
    hydrateFromToken();
  }, []);

  const loginWithEmail = async ({ email, tenantId }) => {
    setIsLoadingAuth(true);
    setAuthError('');

    try {
      const payload = unwrapResponse(await civant.auth.createSession(email, tenantId));
      const token = String(payload?.token || '').trim();
      if (!token) throw new Error('No session token returned');

      civant.auth.setToken(token, true);
      await hydrateFromToken();
      return { ok: true };
    } catch (error) {
      setIsLoadingAuth(false);
      const message = error?.message || 'Failed to log in';
      setAuthError(message);
      return { ok: false, error: message };
    }
  };

  const logout = () => {
    clearStoredToken();
    civant.auth.setToken(null, false);
    setUser(null);
    setIsAuthenticated(false);
    setCapabilities({ isAdmin: false, tenantId: 'civant_default' });
    setTenantInfo(null);
    setAuthError('');
  };

  const value = useMemo(() => ({
    user,
    isAuthenticated,
    isLoadingAuth,
    authError,
    capabilities,
    tenantInfo,
    isLoadingCapabilities,
    loginWithEmail,
    logout,
    refreshAuth: hydrateFromToken
  }), [user, isAuthenticated, isLoadingAuth, authError, capabilities, tenantInfo, isLoadingCapabilities]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth must be used within AuthProvider');
  return context;
}
