import { Toaster } from '@/components/ui/toaster';
import { QueryClientProvider } from '@tanstack/react-query';
import { queryClientInstance } from '@/lib/query-client';
import NavigationTracker from '@/lib/NavigationTracker';
import { pagesConfig } from './pages.config';
import { Suspense } from 'react';
import { BrowserRouter as Router, Navigate, Route, Routes, useLocation } from 'react-router-dom';
import PageNotFound from './lib/PageNotFound';
import { AuthProvider, useAuth } from '@/lib/auth';
import { TenantProvider } from '@/lib/tenant';
import Login from '@/pages/Login';
import { Button } from '@/components/ui/button';

const { Pages, Layout, mainPage } = pagesConfig;
const mainPageKey = mainPage ?? Object.keys(Pages)[0];
const MainPage = mainPageKey ? Pages[mainPageKey] : () => null;
const SystemPage = Pages.System;

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
    <LayoutWrapper currentPageName="System">
      <div className="min-h-[40vh] flex items-center justify-center">
        <div className="max-w-md rounded-xl border border-border bg-card p-8 text-center space-y-2">
          <h1 className="text-xl font-semibold text-card-foreground">Not authorised</h1>
          <p className="text-sm text-muted-foreground">You do not have permission to access System settings.</p>
        </div>
      </div>
    </LayoutWrapper>
  );
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

function RequireSystemRole({ children }) {
  const { isLoadingAuth, profileStatus, roles, authWarning, retryProfile, logout } = useAuth();

  if (isLoadingAuth) return <FullscreenLoader />;
  if (profileStatus === 'loading' || profileStatus === 'idle') {
    return (
      <LayoutWrapper currentPageName="System">
        <div className="min-h-[40vh] flex items-center justify-center">
          <div className="max-w-md rounded-xl border border-border bg-card p-8 text-center space-y-2">
            <h1 className="text-xl font-semibold text-card-foreground">Checking permissions</h1>
            <p className="text-sm text-muted-foreground">Validating your role for System settings.</p>
          </div>
        </div>
      </LayoutWrapper>
    );
  }
  if (profileStatus !== 'ready') {
    return (
      <LayoutWrapper currentPageName="System">
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
  const allowed = Array.isArray(roles) && (roles.includes('admin') || roles.includes('creator'));
  if (!allowed) return <AccessDeniedPage />;
  return children;
}

function ProtectedRoutes() {
  return (
    <Routes>
      <Route
        path="/"
        element={
          <LayoutWrapper currentPageName={mainPageKey}>
            <Suspense fallback={<RouteLoader />}>
              <MainPage />
            </Suspense>
          </LayoutWrapper>
        }
      />

      <Route
        path="/System"
        element={
          <RequireSystemRole>
            <LayoutWrapper currentPageName="System">
              <Suspense fallback={<RouteLoader />}>
                <SystemPage />
              </Suspense>
            </LayoutWrapper>
          </RequireSystemRole>
        }
      />
      <Route
        path="/system"
        element={
          <RequireSystemRole>
            <LayoutWrapper currentPageName="System">
              <Suspense fallback={<RouteLoader />}>
                <SystemPage />
              </Suspense>
            </LayoutWrapper>
          </RequireSystemRole>
        }
      />

      <Route path="/Predictions" element={<Navigate to="/Forecast" replace />} />
      <Route path="/predictions" element={<Navigate to="/Forecast" replace />} />

      {Object.entries(Pages)
        .filter(([path]) => path !== 'System')
        .map(([path, Page]) => (
          <Route
            key={path}
            path={`/${path}`}
            element={
              <LayoutWrapper currentPageName={path}>
                <Suspense fallback={<RouteLoader />}>
                  <Page />
                </Suspense>
              </LayoutWrapper>
            }
          />
        ))}

      <Route path="*" element={<PageNotFound />} />
    </Routes>
  );
}

function AppRoutes() {
  const { isAuthenticated, isLoadingAuth } = useAuth();

  if (isLoadingAuth) return <FullscreenLoader />;

  return (
    <Routes>
      <Route path="/login" element={isAuthenticated ? <Navigate to="/" replace /> : <Login />} />
      <Route
        path="*"
        element={
          <RequireAuth>
            <ProtectedRoutes />
          </RequireAuth>
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
