import { Toaster } from '@/components/ui/toaster';
import { QueryClientProvider } from '@tanstack/react-query';
import { queryClientInstance } from '@/lib/query-client';
import NavigationTracker from '@/lib/NavigationTracker';
import { pagesConfig } from './pages.config';
import { Suspense, useEffect } from 'react';
import { BrowserRouter as Router, Navigate, Route, Routes, useLocation, useSearchParams } from 'react-router-dom';
import PageNotFound from './lib/PageNotFound';
import { AuthProvider, useAuth } from '@/lib/auth';
import { TenantProvider } from '@/lib/tenant';
import { OnboardingProvider, RequireOnboarding } from "@/lib/OnboardingGate";
import Login from '@/pages/Login';
import AcceptInvitationPage from '@/pages/AcceptInvitation';
import { Button } from '@/components/ui/button';

const { Pages, Layout, prefetchCorePages } = pagesConfig;
const HomePage = Pages.Home;
const ForecastPage = Pages.Forecast;
const CompetitorsPage = Pages.Competitors;
const CompanyProfilePage = Pages.CompanyProfile;
const ProfilePage = Pages.Profile;
const SearchPage = Pages.Search;
const AlertsPage = Pages.Alerts;
const PipelineAdminPage = Pages.PipelineAdmin;
const ArchitecturePage = Pages.Architecture;
const SystemPage = Pages.System;
const TenderDetailPage = Pages.TenderDetail;
const ReportsPage = Pages.Reports;

const LayoutWrapper = ({ children, currentPageName }) => Layout
  ? <Layout currentPageName={currentPageName}>{children}</Layout>
  : <>{children}</>;

function FullscreenLoader() {
  return (
    <div className="fixed inset-0 flex flex-col items-center justify-center gap-3 bg-background/90">
      <div className="w-8 h-8 border-4 border-slate-200 border-t-teal-400 rounded-full animate-spin" />
      <p className="text-sm text-muted-foreground">Loading Civant…</p>
    </div>
  );
}

function AccessDeniedPage() {
  return (
    <LayoutWrapper currentPageName="Operations">
      <div className="min-h-[40vh] flex items-center justify-center">
        <div className="max-w-md rounded-xl border border-border bg-card p-8 text-center space-y-2">
          <h1 className="text-xl font-semibold text-card-foreground">Not authorised</h1>
          <p className="text-sm text-muted-foreground">
            You do not have permission to access Operations pages.
          </p>
        </div>
      </div>
    </LayoutWrapper>
  );
}

function LoginRoute() {
  const { isAuthenticated } = useAuth();
  const [searchParams] = useSearchParams();
  const returnTo = searchParams.get('returnTo') || '/';
  return isAuthenticated ? <Navigate to={returnTo} replace /> : <Login />;
}

function RouteLoader() {
  return (
    <div className="min-h-[40vh] flex items-center justify-center">
      <div className="w-8 h-8 border-4 border-slate-200 border-t-teal-400 rounded-full animate-spin" />
    </div>
  );
}

function RequireAuth({ children }) {
  const { isAuthenticated, isLoadingAuth } = useAuth();
  const location = useLocation();

  if (isLoadingAuth) return <FullscreenLoader />;
  if (!isAuthenticated) return <Navigate to="/login" replace state={{ from: location }} />;
  return children;
}

function RequireOperationsRole({ children }) {
  const { isLoadingAuth, profileStatus, roles, authWarning, retryProfile, logout } = useAuth();

  if (isLoadingAuth) return <FullscreenLoader />;
  if (profileStatus === 'loading' || profileStatus === 'idle') {
    return (
      <LayoutWrapper currentPageName="Operations">
        <div className="min-h-[40vh] flex items-center justify-center">
          <div className="max-w-md rounded-xl border border-border bg-card p-8 text-center space-y-2">
            <h1 className="text-xl font-semibold text-card-foreground">Checking permissions</h1>
            <p className="text-sm text-muted-foreground">Validating your role for Operations.</p>
          </div>
        </div>
      </LayoutWrapper>
    );
  }
  if (profileStatus !== 'ready') {
    return (
      <LayoutWrapper currentPageName="Operations">
        <div className="min-h-[40vh] flex items-center justify-center px-4">
          <div className="max-w-md rounded-xl border border-border bg-card p-8 text-center space-y-3">
            <h1 className="text-xl font-semibold text-card-foreground">Role check unavailable</h1>
            <p className="text-sm text-muted-foreground">
              {authWarning || 'Unable to confirm your permissions right now.'}
            </p>
            <div className="flex items-center justify-center gap-2">
              <Button variant="secondary" onClick={() => retryProfile()}>Retry</Button>
              <Button variant="ghost" onClick={logout}>Sign out</Button>
            </div>
          </div>
        </div>
      </LayoutWrapper>
    );
  }
  const allowed = Array.isArray(roles) && roles.includes('super_admin');
  if (!allowed) return <AccessDeniedPage />;
  return children;
}

function ProtectedRoutes() {
  return (
    <Routes>
      <Route
        path="/"
        element={
          <LayoutWrapper currentPageName="Home">
            <Suspense fallback={<RouteLoader />}>
              <HomePage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route path="/home" element={<Navigate to="/" replace />} />
      <Route path="/Home" caseSensitive element={<Navigate to="/" replace />} />

      <Route
        path="/forecast"
        element={
          <LayoutWrapper currentPageName="Forecast">
            <Suspense fallback={<RouteLoader />}>
              <ForecastPage />
            </Suspense>
          </LayoutWrapper>
        }
      />
      <Route path="/Predictions" caseSensitive element={<Navigate to="/forecast" replace />} />
      <Route path="/predictions" element={<Navigate to="/forecast" replace />} />

      <Route
        path="/competitors"
        element={
          <LayoutWrapper currentPageName="Competitors">
            <Suspense fallback={<RouteLoader />}>
              <CompetitorsPage />
            </Suspense>
          </LayoutWrapper>
        }
      />
      <Route
        path="/competitors/:competitorId"
        element={
          <LayoutWrapper currentPageName="Competitors">
            <Suspense fallback={<RouteLoader />}>
              <CompetitorsPage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route
        path="/company"
        element={
          <LayoutWrapper currentPageName="Company">
            <Suspense fallback={<RouteLoader />}>
              <CompanyProfilePage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route
        path="/company/integrations"
        element={<Navigate to="/operations/system" replace />}
      />

      <Route
        path="/company/profile"
        element={
          <LayoutWrapper currentPageName="Profile">
            <Suspense fallback={<RouteLoader />}>
              <ProfilePage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route
        path="/operations"
        element={
          <RequireOperationsRole>
            <Navigate to="/operations/pipeline" replace />
          </RequireOperationsRole>
        }
      />
      <Route
        path="/operations/connectors"
        element={
          <RequireOperationsRole>
            <Navigate to="/operations/pipeline" replace />
          </RequireOperationsRole>
        }
      />

      <Route
        path="/operations/pipeline"
        element={
          <RequireOperationsRole>
            <LayoutWrapper currentPageName="Operations">
              <Suspense fallback={<RouteLoader />}>
                <PipelineAdminPage />
              </Suspense>
            </LayoutWrapper>
          </RequireOperationsRole>
        }
      />

      <Route
        path="/operations/architecture"
        element={
          <RequireOperationsRole>
            <LayoutWrapper currentPageName="Operations">
              <Suspense fallback={<RouteLoader />}>
                <ArchitecturePage />
              </Suspense>
            </LayoutWrapper>
          </RequireOperationsRole>
        }
      />

      <Route
        path="/operations/system"
        element={
          <RequireOperationsRole>
            <LayoutWrapper currentPageName="Operations">
              <Suspense fallback={<RouteLoader />}>
                <SystemPage />
              </Suspense>
            </LayoutWrapper>
          </RequireOperationsRole>
        }
      />

      <Route
        path="/workbench/search"
        element={
          <LayoutWrapper currentPageName="Search">
            <Suspense fallback={<RouteLoader />}>
              <SearchPage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route
        path="/workbench/alerts"
        element={
          <LayoutWrapper currentPageName="Alerts">
            <Suspense fallback={<RouteLoader />}>
              <AlertsPage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route path="/workbench/insights" element={<Navigate to="/reports" replace />} />

      <Route
        path="/reports"
        element={
          <LayoutWrapper currentPageName="Reports">
            <Suspense fallback={<RouteLoader />}>
              <ReportsPage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route
        path="/tenderdetail"
        element={
          <LayoutWrapper currentPageName="Search">
            <Suspense fallback={<RouteLoader />}>
              <TenderDetailPage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route path="/alerts" element={<Navigate to="/workbench/alerts" replace />} />
      <Route path="/search" element={<Navigate to="/workbench/search" replace />} />
      <Route path="/insights" element={<Navigate to="/reports" replace />} />
      <Route path="/integrations" element={<Navigate to="/operations/system" replace />} />
      <Route path="/connectors" element={<Navigate to="/operations/pipeline" replace />} />
      <Route path="/pipeline" element={<Navigate to="/operations/pipeline" replace />} />
      <Route path="/pipelineadmin" element={<Navigate to="/operations/pipeline" replace />} />
      <Route path="/architecture" element={<Navigate to="/operations/architecture" replace />} />
      <Route path="/system" element={<Navigate to="/operations/system" replace />} />
      <Route path="/profile" element={<Navigate to="/company/profile" replace />} />
      <Route path="/companyprofile" element={<Navigate to="/company" replace />} />

      <Route path="/Alerts" caseSensitive element={<Navigate to="/workbench/alerts" replace />} />
      <Route path="/Search" caseSensitive element={<Navigate to="/workbench/search" replace />} />
      <Route path="/Insights" caseSensitive element={<Navigate to="/reports" replace />} />
      <Route path="/Integrations" caseSensitive element={<Navigate to="/operations/system" replace />} />
      <Route path="/Connectors" caseSensitive element={<Navigate to="/operations/pipeline" replace />} />
      <Route path="/PipelineAdmin" caseSensitive element={<Navigate to="/operations/pipeline" replace />} />
      <Route path="/Pipeline" caseSensitive element={<Navigate to="/operations/pipeline" replace />} />
      <Route path="/Architecture" caseSensitive element={<Navigate to="/operations/architecture" replace />} />
      <Route path="/System" caseSensitive element={<Navigate to="/operations/system" replace />} />
      <Route path="/Profile" caseSensitive element={<Navigate to="/company/profile" replace />} />
      <Route path="/CompanyProfile" caseSensitive element={<Navigate to="/company" replace />} />

      <Route path="*" element={<PageNotFound />} />
    </Routes>
  );
}

function AppRoutes() {
  const { isAuthenticated, isLoadingAuth } = useAuth();

  useEffect(() => {
    if (isLoadingAuth || !isAuthenticated) return;

    let cancelled = false;
    const runPrefetch = () => {
      if (cancelled) return;
      typeof prefetchCorePages === "function" && prefetchCorePages().catch(() => {});
    };

    if (typeof window !== 'undefined' && typeof window.requestIdleCallback === 'function') {
      const callbackId = window.requestIdleCallback(runPrefetch, { timeout: 2000 });
      return () => {
        cancelled = true;
        window.cancelIdleCallback(callbackId);
      };
    }

    const timeoutId = window.setTimeout(runPrefetch, 250);
    return () => {
      cancelled = true;
      window.clearTimeout(timeoutId);
    };
  }, [isAuthenticated, isLoadingAuth]);

  if (isLoadingAuth) return <FullscreenLoader />;

  return (
    <Routes>
      <Route path="/login" element={<LoginRoute />} />
      <Route path="/invite" element={<AcceptInvitationPage />} />
      <Route
        path="*"
        element={
          <RequireAuth><OnboardingProvider><RequireOnboarding>
            <ProtectedRoutes />
          </RequireOnboarding></OnboardingProvider></RequireAuth>
        }
      />
    </Routes>
  );
}

function App() {
  return (
    <AuthProvider>
      <TenantProvider>
        <QueryClientProvider client={queryClientInstance}>
          <Router>
            <NavigationTracker />
            <AppRoutes />
          </Router>
          <Toaster />
        </QueryClientProvider>
      </TenantProvider>
    </AuthProvider>
  );
}

export default App;
