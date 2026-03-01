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
  if (typeof window === 'undefined' || !window.localStorage) return null;
  const stored = String(window.localStorage.getItem(ACTIVE_TENANT_STORAGE_KEY) || '').trim().toLowerCase();
  return stored || null;
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

const COMPANY_PROFILE_FIELDS =
  'target_cpv_clusters,target_countries,target_buyer_types,contract_size_min_eur,contract_size_max_eur,company_scope_filter_enabled,known_competitors,onboarding_completed';

export function TenantProvider({ children }) {
  const { isAuthenticated, isLoadingAuth, profileStatus, roles, tenantId: userTenantId } = useAuth();
  const [tenants, setTenants] = useState([]);
  const [activeTenantId, setActiveTenantIdState] = useState(getStoredActiveTenantId);
  const [isLoadingTenants, setIsLoadingTenants] = useState(false);
  const [tenantError, setTenantError] = useState('');
  const [companyProfile, setCompanyProfile] = useState(null);
  const [isLoadingProfile, setIsLoadingProfile] = useState(false);

  const setActiveTenantId = (nextTenantId, options = {}) => {
    const fallbackToDefault = options.fallbackToDefault !== false;
    const normalized = String(nextTenantId || '').trim().toLowerCase();
    const effective = normalized || (fallbackToDefault ? 'civant_default' : null);

    setActiveTenantIdState(effective);
    setStoredActiveTenantId(effective || '');
    civant.setActiveTenantId(effective || '', true, { fallbackToDefault: false });
  };

  const refreshTenants = async () => {
    if (!isAuthenticated || profileStatus !== 'ready') {
      setTenants([]);
      setTenantError('');
      setActiveTenantId(null, { fallbackToDefault: false });
      return [];
    }

    setIsLoadingTenants(true);
    setTenantError('');

    try {
      const payload = unwrapResponse(await civant.system.listTenants());
      const nextTenants = normalizeTenants(payload);
      setTenants(nextTenants);

      const existingId = getStoredActiveTenantId() || activeTenantId;
      const hasExisting = existingId ? nextTenants.some((tenant) => tenant.id === existingId) : false;
      const userTenant = String(userTenantId || '').trim().toLowerCase();
      const hasUserTenant = userTenant && nextTenants.some((tenant) => tenant.id === userTenant);

      const nextActiveTenantId = hasExisting
        ? existingId
        : hasUserTenant
          ? userTenant
          : (nextTenants[0]?.id || null);

      setActiveTenantId(nextActiveTenantId, { fallbackToDefault: false });
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
      setActiveTenantId(createdTenantId, { fallbackToDefault: false });
    }

    return result;
  };

  useEffect(() => {
    if (isLoadingAuth || profileStatus === 'loading' || profileStatus === 'idle') return;
    refreshTenants();
  }, [isAuthenticated, isLoadingAuth, profileStatus]);

  const refreshCompanyProfile = async (tenantId = activeTenantId) => {
    if (!tenantId) { setCompanyProfile(null); return; }
    setIsLoadingProfile(true);
    try {
      const rows = await civant.entities.company_profiles.filter(
        { tenant_id: tenantId }, '-updated_at', 1, COMPANY_PROFILE_FIELDS
      );
      setCompanyProfile(Array.isArray(rows) && rows.length > 0 ? rows[0] : null);
    } catch {
      setCompanyProfile(null);
    } finally {
      setIsLoadingProfile(false);
    }
  };

  useEffect(() => {
    if (!activeTenantId) { setCompanyProfile(null); return; }
    let cancelled = false;
    setIsLoadingProfile(true);
    civant.entities.company_profiles
      .filter({ tenant_id: activeTenantId }, '-updated_at', 1, COMPANY_PROFILE_FIELDS)
      .then((rows) => {
        if (!cancelled) setCompanyProfile(Array.isArray(rows) && rows.length > 0 ? rows[0] : null);
      })
      .catch(() => { if (!cancelled) setCompanyProfile(null); })
      .finally(() => { if (!cancelled) setIsLoadingProfile(false); });
    return () => { cancelled = true; };
  }, [activeTenantId]);

  const canCreateTenant = Array.isArray(roles) && roles.includes('creator');

  const value = useMemo(() => ({
    tenants,
    activeTenantId,
    setActiveTenantId,
    refreshTenants,
    createTenant,
    canCreateTenant,
    isLoadingTenants,
    tenantError,
    companyProfile,
    isLoadingProfile,
    refreshCompanyProfile
  }), [tenants, activeTenantId, canCreateTenant, isLoadingTenants, tenantError, companyProfile, isLoadingProfile]);

  return <TenantContext.Provider value={value}>{children}</TenantContext.Provider>;
}

export function useTenant() {
  const context = useContext(TenantContext);
  if (!context) {
    throw new Error('useTenant must be used within TenantProvider');
  }
  return context;
}
