import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { civant } from '@/api/civantClient';
import { useAuth } from '@/lib/auth';

const TenantContext = createContext(null);
const ACTIVE_TENANT_STORAGE_KEY = 'civant_active_tenant';

function unwrapResponse(response) {
  return response?.data ?? response ?? null;
}

function normalizeTenants(payload) {
  const rows = Array.isArray(payload?.tenants) ? payload.tenants : Array.isArray(payload) ? payload : [];
  return rows
    .map((row) => ({
      id: String(row?.id || '').trim().toLowerCase(),
      name: String(row?.name || row?.id || '').trim()
    }))
    .filter((row) => row.id);
}

function getStoredActiveTenantId() {
  if (typeof window === 'undefined' || !window.localStorage) return '';
  return String(window.localStorage.getItem(ACTIVE_TENANT_STORAGE_KEY) || '').trim().toLowerCase();
}

function setStoredActiveTenantId(tenantId) {
  if (typeof window === 'undefined' || !window.localStorage) return;
  const value = String(tenantId || '').trim().toLowerCase();
  if (value) {
    window.localStorage.setItem(ACTIVE_TENANT_STORAGE_KEY, value);
  } else {
    window.localStorage.removeItem(ACTIVE_TENANT_STORAGE_KEY);
  }
}

export function TenantProvider({ children }) {
  const { isAuthenticated, isLoadingAuth, roles, tenantId: userTenantId } = useAuth();
  const [tenants, setTenants] = useState([]);
  const [activeTenantId, setActiveTenantIdState] = useState(getStoredActiveTenantId);
  const [isLoadingTenants, setIsLoadingTenants] = useState(false);
  const [tenantError, setTenantError] = useState('');

  const setActiveTenantId = (nextTenantId) => {
    const normalized = String(nextTenantId || '').trim().toLowerCase();
    setActiveTenantIdState(normalized);
    setStoredActiveTenantId(normalized);
    civant.setActiveTenantId(normalized, true);
  };

  const refreshTenants = async () => {
    if (!isAuthenticated) {
      setTenants([]);
      setTenantError('');
      setActiveTenantId('');
      return [];
    }

    setIsLoadingTenants(true);
    setTenantError('');

    try {
      const payload = unwrapResponse(await civant.system.listTenants());
      const nextTenants = normalizeTenants(payload);
      setTenants(nextTenants);

      const existingId = getStoredActiveTenantId() || activeTenantId;
      const hasExisting = nextTenants.some((tenant) => tenant.id === existingId);
      const userTenant = String(userTenantId || '').trim().toLowerCase();
      const hasUserTenant = userTenant && nextTenants.some((tenant) => tenant.id === userTenant);

      const nextActiveTenantId = hasExisting
        ? existingId
        : hasUserTenant
          ? userTenant
          : (nextTenants[0]?.id || '');

      setActiveTenantId(nextActiveTenantId);
      return nextTenants;
    } catch (error) {
      setTenantError(error?.message || 'Failed to load tenants');
      return [];
    } finally {
      setIsLoadingTenants(false);
    }
  };

  const createTenant = async ({ name, id }) => {
    const payload = { name, ...(id ? { id } : {}) };
    const result = unwrapResponse(await civant.system.createTenant(payload));
    await refreshTenants();

    const createdTenantId = String(result?.tenant?.id || '').trim().toLowerCase();
    if (createdTenantId) {
      setActiveTenantId(createdTenantId);
    }

    return result;
  };

  useEffect(() => {
    if (isLoadingAuth) return;
    refreshTenants();
  }, [isAuthenticated, isLoadingAuth]);

  const canCreateTenant = Array.isArray(roles) && roles.includes('creator');

  const value = useMemo(() => ({
    tenants,
    activeTenantId,
    setActiveTenantId,
    refreshTenants,
    createTenant,
    canCreateTenant,
    isLoadingTenants,
    tenantError
  }), [tenants, activeTenantId, canCreateTenant, isLoadingTenants, tenantError]);

  return <TenantContext.Provider value={value}>{children}</TenantContext.Provider>;
}

export function useTenant() {
  const context = useContext(TenantContext);
  if (!context) {
    throw new Error('useTenant must be used within TenantProvider');
  }
  return context;
}
