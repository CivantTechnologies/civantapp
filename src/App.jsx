import { Toaster } from '@/components/ui/toaster';
import { QueryClientProvider } from '@tanstack/react-query';
import { queryClientInstance } from '@/lib/query-client';
import NavigationTracker from '@/lib/NavigationTracker';
import { pagesConfig } from './pages.config';
import { BrowserRouter as Router, Navigate, Route, Routes, useLocation } from 'react-router-dom';
import PageNotFound from './lib/PageNotFound';
import { AuthProvider, useAuth } from '@/lib/AuthProvider';
import Login from '@/pages/Login';

const { Pages, Layout, mainPage } = pagesConfig;
const mainPageKey = mainPage ?? Object.keys(Pages)[0];
const MainPage = mainPageKey ? Pages[mainPageKey] : <></>;
const SystemPage = Pages.System;

const LayoutWrapper = ({ children, currentPageName }) => Layout
  ? <Layout currentPageName={currentPageName}>{children}</Layout>
  : <>{children}</>;

function FullscreenLoader() {
  return (
    <div className="fixed inset-0 flex items-center justify-center">
      <div className="w-8 h-8 border-4 border-slate-200 border-t-slate-800 rounded-full animate-spin" />
    </div>
  );
}

function AccessDeniedPage() {
  return (
    <LayoutWrapper currentPageName="System">
      <div className="min-h-[40vh] flex items-center justify-center">
        <div className="max-w-md rounded-xl border border-border bg-card p-8 text-center space-y-2">
          <h1 className="text-xl font-semibold text-card-foreground">Access denied</h1>
          <p className="text-sm text-muted-foreground">You don’t have access to System settings.</p>
        </div>
      </div>
    </LayoutWrapper>
  );
}

function RequireAuth({ children }) {
  const { isAuthenticated, isLoadingAuth } = useAuth();
  const location = useLocation();

  if (isLoadingAuth) return <FullscreenLoader />;
  if (!isAuthenticated) return <Navigate to="/login" replace state={{ from: location }} />;
  return children;
}

function RequireAdmin({ children }) {
  const { isLoadingAuth, currentUser } = useAuth();

  if (isLoadingAuth) return <FullscreenLoader />;
  if (currentUser?.role !== 'admin') return <AccessDeniedPage />;
  return children;
}

function ProtectedRoutes() {
  return (
    <Routes>
      <Route
        path="/"
        element={
          <LayoutWrapper currentPageName={mainPageKey}>
            <MainPage />
          </LayoutWrapper>
        }
      />

      <Route
        path="/System"
        element={
          <RequireAdmin>
            <LayoutWrapper currentPageName="System">
              <SystemPage />
            </LayoutWrapper>
          </RequireAdmin>
        }
      />
      <Route
        path="/system"
        element={
          <RequireAdmin>
            <LayoutWrapper currentPageName="System">
              <SystemPage />
            </LayoutWrapper>
          </RequireAdmin>
        }
      />

      {Object.entries(Pages)
        .filter(([path]) => path !== 'System')
        .map(([path, Page]) => (
          <Route
            key={path}
            path={`/${path}`}
            element={
              <LayoutWrapper currentPageName={path}>
                <Page />
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
      <QueryClientProvider client={queryClientInstance}>
        <Router>
          <NavigationTracker />
          <AppRoutes />
        </Router>
        <Toaster />
      </QueryClientProvider>
    </AuthProvider>
  );
}

export default App;
