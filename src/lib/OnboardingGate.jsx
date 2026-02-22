import React, { useState, useEffect, createContext, useContext } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { civant } from '@/api/civantClient';
import { useTenant } from '@/lib/tenant';

const ONBOARDING_STATUS_CACHE_KEY = 'civant_onboarding_status_v1';

const OnboardingContext = createContext({
    onboardingComplete: false,
    onboardingStatus: 'loading',
    refreshOnboarding: () => {}
});

function readCachedOnboardingMap() {
    if (typeof window === 'undefined' || !window.localStorage) return {};
    try {
        const raw = window.localStorage.getItem(ONBOARDING_STATUS_CACHE_KEY);
        const parsed = raw ? JSON.parse(raw) : {};
        return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
        return {};
    }
}

function readCachedOnboardingStatus(tenantId) {
    const map = readCachedOnboardingMap();
    if (!tenantId || typeof map[tenantId] !== 'boolean') return null;
    return map[tenantId] ? 'complete' : 'incomplete';
}

function cacheOnboardingStatus(tenantId, completed) {
    if (typeof window === 'undefined' || !window.localStorage || !tenantId) return;
    const map = readCachedOnboardingMap();
    map[tenantId] = Boolean(completed);
    window.localStorage.setItem(ONBOARDING_STATUS_CACHE_KEY, JSON.stringify(map));
}

export function useOnboarding() {
    return useContext(OnboardingContext);
}

export function OnboardingProvider({ children }) {
    const { activeTenantId, isLoadingTenants } = useTenant();
    const [status, setStatus] = useState('loading'); // 'loading' | 'complete' | 'incomplete'

    const check = async (tenantId) => {
        if (!tenantId) return;
        try {
            const rows = await civant.entities.company_profiles.filter(
                { tenant_id: tenantId },
                '-updated_at',
                1
            );
            const profile = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
            const nextStatus = profile?.onboarding_completed ? 'complete' : 'incomplete';
            setStatus(nextStatus);
            cacheOnboardingStatus(tenantId, nextStatus === 'complete');
        } catch (e) {
            console.error('Onboarding check failed:', e);
            if (readCachedOnboardingStatus(tenantId) === null) {
                setStatus('incomplete');
            }
        }
    };

    useEffect(() => {
        if (isLoadingTenants) return;

        if (activeTenantId) {
            const cached = readCachedOnboardingStatus(activeTenantId);
            setStatus(cached || 'loading');
            check(activeTenantId);
        } else {
            // Tenants finished loading but no active tenant — treat as incomplete
            setStatus('incomplete');
        }
    }, [activeTenantId, isLoadingTenants]);

    const refreshOnboarding = () => {
        if (!activeTenantId) return;
        check(activeTenantId);
    };

    return (
        <OnboardingContext.Provider value={{
            onboardingComplete: status === 'complete',
            onboardingStatus: status,
            refreshOnboarding
        }}>
            {children}
        </OnboardingContext.Provider>
    );
}

export function RequireOnboarding({ children }) {
    const location = useLocation();
    const { onboardingComplete, onboardingStatus } = useOnboarding();

    const isOnboardingPage = location.pathname.toLowerCase() === '/companyprofile';
    if (isOnboardingPage) return children;

    const exempt = ['/login', '/system'];
    if (exempt.includes(location.pathname.toLowerCase())) return children;

    if (onboardingStatus === 'loading') return children;

    if (!onboardingComplete) {
        return <Navigate to="/companyprofile" replace />;
    }
    return children;
}
