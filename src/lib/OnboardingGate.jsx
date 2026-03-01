import React, { useState, useEffect, createContext, useContext } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { supabase } from '@/lib/supabaseClient';
import { useTenant } from '@/lib/tenant';

const OnboardingContext = createContext({ onboardingComplete: false, refreshOnboarding: () => {} });

export function useOnboarding() {
    return useContext(OnboardingContext);
}

export function OnboardingProvider({ children }) {
    const { activeTenantId, isLoadingTenants } = useTenant();
    const [status, setStatus] = useState('loading'); // 'loading' | 'complete' | 'incomplete'

    const check = async () => {
        if (!activeTenantId) { setStatus('incomplete'); return; }
        try {
            const { data, error } = await supabase
                .from('company_profiles')
                .select('onboarding_completed')
                .eq('tenant_id', activeTenantId)
                .maybeSingle();
            if (error && error.code !== 'PGRST116') throw error;
            setStatus(data?.onboarding_completed ? 'complete' : 'incomplete');
        } catch (e) {
            console.error('Onboarding check failed:', e);
            setStatus('incomplete');
        }
    };

    useEffect(() => {
        if (!isLoadingTenants) check();
    }, [activeTenantId, isLoadingTenants]);

    const refreshOnboarding = () => { check(); };

    return (
        <OnboardingContext.Provider value={{ onboardingComplete: status === 'complete', refreshOnboarding }}>
            {status === 'loading' && !isLoadingTenants ? null : children}
        </OnboardingContext.Provider>
    );
}

export function RequireOnboarding({ children }) {
    const location = useLocation();
    const { onboardingComplete } = useOnboarding();

    // Allow access to company profile page (for the wizard itself)
    const isOnboardingPage = location.pathname.toLowerCase() === '/companyprofile';
    if (isOnboardingPage) return children;

    // Allow access to login and system pages
    const exempt = ['/login', '/system'];
    if (exempt.includes(location.pathname.toLowerCase())) return children;

    if (!onboardingComplete) {
        return <Navigate to="/company" replace />;
    }

    return children;
}
