import React, { useEffect, useMemo, useState } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { createPageUrl } from './utils';
import { useAuth } from '@/lib/auth';
import { useTenant } from '@/lib/tenant';
import { civant } from '@/api/civantClient';
import {
  Building2,
  LayoutDashboard,
  Search,
  Bell,
  BarChart3,
  Settings,
  Menu,
  X,
  Radar,
  ChevronRight,
  LogOut,
  Plus,
  Loader2
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle
} from '@/components/ui/dialog';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger
} from '@/components/ui/dropdown-menu';

export default function Layout({ children, currentPageName }) {
  const { currentUser, roles, profileStatus, logout, authWarning } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const {
    tenants,
    activeTenantId,
    setActiveTenantId,
    createTenant,
    canCreateTenant,
    isLoadingTenants,
    tenantError
  } = useTenant();

  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [showCreateTenant, setShowCreateTenant] = useState(false);
  const [tenantNameInput, setTenantNameInput] = useState('');
  const [tenantIdInput, setTenantIdInput] = useState('');
  const [tenantActionLoading, setTenantActionLoading] = useState(false);
  const [tenantActionError, setTenantActionError] = useState('');
  const [globalSearchOpen, setGlobalSearchOpen] = useState(false);
  const [globalSearchQuery, setGlobalSearchQuery] = useState('');
  const [globalSearchLoading, setGlobalSearchLoading] = useState(false);
  const [globalSearchError, setGlobalSearchError] = useState('');
  const [globalSearchResults, setGlobalSearchResults] = useState([]);
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [alertsPreviewLoading, setAlertsPreviewLoading] = useState(false);
  const [alertsPreviewError, setAlertsPreviewError] = useState('');
  const [alertsPreview, setAlertsPreview] = useState([]);
  const [alertEventsPreview, setAlertEventsPreview] = useState([]);

  const selectedTenant = useMemo(
    () => tenants.find((tenant) => tenant.id === activeTenantId) || null,
    [tenants, activeTenantId]
  );

  const isSuperAdmin = profileStatus === 'ready'
    && Array.isArray(roles)
    && roles.includes('super_admin');
  const roleLabel = Array.isArray(roles) && roles.length ? roles.join(', ') : (profileStatus === 'ready' ? 'user' : 'Checking permissions...');

  const navItems = [
    {
      name: 'Home',
      page: 'Home',
      to: '/home',
      icon: LayoutDashboard,
      matches: ['Home', 'Search', 'Alerts', 'Insights']
    },
    {
      name: 'Forecast',
      page: 'Forecast',
      to: '/forecast',
      icon: BarChart3,
      matches: ['Forecast']
    },
    {
      name: 'Competitors',
      page: 'Competitors',
      to: '/competitors',
      icon: Radar,
      matches: ['Competitors']
    },
    {
      name: 'Company',
      page: 'Company',
      to: '/company',
      icon: Building2,
      matches: ['Company', 'CompanyProfile', 'Profile', 'Integrations']
    }
  ];

  if (isSuperAdmin) {
    navItems.push({
      name: 'Operations',
      page: 'Operations',
      to: '/operations',
      icon: Settings,
      matches: ['Operations', 'Connectors', 'PipelineAdmin', 'Architecture', 'System']
    });
  }

  useEffect(() => {
    if (typeof window === 'undefined') return undefined;

    const onKeyDown = (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        setGlobalSearchOpen(true);
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    let consumed = false;

    if (params.get('openSearch') === '1') {
      setGlobalSearchOpen(true);
      params.delete('openSearch');
      consumed = true;
    }

    if (params.get('openNotifications') === '1') {
      setNotificationsOpen(true);
      params.delete('openNotifications');
      consumed = true;
    }

    if (consumed) {
      const nextSearch = params.toString();
      navigate(
        {
          pathname: location.pathname,
          search: nextSearch ? `?${nextSearch}` : ''
        },
        { replace: true }
      );
    }
  }, [location.pathname, location.search, navigate]);

  useEffect(() => {
    if (!globalSearchOpen) return undefined;
    const keyword = globalSearchQuery.trim();
    if (!keyword) {
      setGlobalSearchResults([]);
      setGlobalSearchError('');
      return undefined;
    }

    let cancelled = false;
    const timeoutId = setTimeout(async () => {
      setGlobalSearchLoading(true);
      setGlobalSearchError('');
      try {
        const response = await civant.functions.invoke('searchTenders', {
          keyword,
          limit: 8
        });
        if (cancelled) return;
        const payload = response?.data ?? response ?? {};
        const items = Array.isArray(payload.items) ? payload.items : [];
        setGlobalSearchResults(items);
      } catch (error) {
        if (cancelled) return;
        setGlobalSearchResults([]);
        setGlobalSearchError(error?.message || 'Unable to search right now.');
      } finally {
        if (!cancelled) setGlobalSearchLoading(false);
      }
    }, 220);

    return () => {
      cancelled = true;
      clearTimeout(timeoutId);
    };
  }, [globalSearchOpen, globalSearchQuery]);

  useEffect(() => {
    if (!notificationsOpen || !currentUser?.email) return undefined;
    let cancelled = false;

    const loadNotifications = async () => {
      setAlertsPreviewLoading(true);
      setAlertsPreviewError('');
      try {
        const [alerts, events] = await Promise.all([
          civant.entities.Alerts.filter({ user_email: currentUser.email }),
          civant.entities.AlertEvents.list('-matched_at', 20)
        ]);
        if (cancelled) return;

        const safeAlerts = Array.isArray(alerts) ? alerts : [];
        const safeEvents = Array.isArray(events) ? events : [];
        const myAlertIds = new Set(safeAlerts.map((item) => item?.id).filter(Boolean));
        const myEvents = safeEvents.filter((event) => myAlertIds.has(event?.alert_id)).slice(0, 8);

        setAlertsPreview(safeAlerts.slice(0, 6));
        setAlertEventsPreview(myEvents);
      } catch (error) {
        if (cancelled) return;
        setAlertsPreview([]);
        setAlertEventsPreview([]);
        setAlertsPreviewError(error?.message || 'Unable to load notifications.');
      } finally {
        if (!cancelled) setAlertsPreviewLoading(false);
      }
    };

    loadNotifications();
    return () => {
      cancelled = true;
    };
  }, [notificationsOpen, currentUser?.email]);

  const onCreateTenant = async (event) => {
    event.preventDefault();
    setTenantActionError('');

    const name = tenantNameInput.trim();
    const id = tenantIdInput.trim();
    if (!name) {
      setTenantActionError('Tenant name is required.');
      return;
    }

    setTenantActionLoading(true);
    try {
      await createTenant({ name, ...(id ? { id } : {}) });
      setTenantNameInput('');
      setTenantIdInput('');
      setShowCreateTenant(false);
    } catch (error) {
      setTenantActionError(error?.message || 'Failed to create tenant');
    } finally {
      setTenantActionLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-background text-foreground">
      {authWarning ? (
        <div className="mx-4 mt-4 rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
          {authWarning}
        </div>
      ) : null}

      {/* Mobile header */}
      <header className="lg:hidden fixed top-0 left-0 right-0 h-16 bg-background/95 border-b border-white/[0.06] z-50 px-4 flex items-center justify-between backdrop-blur-md">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="icon" onClick={() => setSidebarOpen(!sidebarOpen)}>
            {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
          </Button>
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 overflow-hidden rounded-lg ring-1 ring-primary/30">
              <img src="/apple-touch-icon.png" alt="Civant mark" className="h-full w-full object-cover" />
            </div>
            <span className="font-semibold text-card-foreground">Civant</span>
          </div>
        </div>
        <Button variant="primary" size="sm" asChild>
          <Link to={createPageUrl('Home')}>Dashboard</Link>
        </Button>
      </header>

      {/* Sidebar */}
      <aside className={`
        fixed top-0 left-0 h-full w-72 bg-background/95 backdrop-blur-md border-r border-white/[0.06] z-40 flex flex-col overflow-hidden
        transform transition-transform duration-200 ease-in-out
        lg:translate-x-0
        ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}
      `}>
        {/* Brand + Tenant */}
        <div className="px-6 py-5 border-b border-white/[0.06] space-y-3">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 overflow-hidden rounded-xl ring-1 ring-primary/20">
              <img src="/apple-touch-icon.png" alt="Civant mark" className="h-full w-full object-cover" />
            </div>
            <div>
              <h1 className="font-bold text-card-foreground tracking-tight">Civant</h1>
              <p className="text-[11px] text-muted-foreground">Procurement Intelligence</p>
            </div>
          </div>

          <div className="space-y-1">
            <label className="text-[10px] uppercase tracking-widest text-slate-500 font-medium">Workspace</label>
            <select
              className="h-9 w-full rounded-xl border border-white/[0.08] bg-white/[0.03] px-3 text-sm text-slate-200 focus:border-primary/40 focus:outline-none transition-colors"
              value={activeTenantId}
              onChange={(event) => setActiveTenantId(event.target.value)}
              disabled={isLoadingTenants}
            >
              {tenants.map((tenant) => (
                <option key={tenant.id} value={tenant.id}>{tenant.name}</option>
              ))}
            </select>
            {tenantError ? <p className="text-xs text-destructive">{tenantError}</p> : null}
          </div>

          {canCreateTenant && (
            <Button type="button" variant="ghost" className="w-full justify-start text-slate-400 hover:text-slate-200" onClick={() => setShowCreateTenant((v) => !v)}>
              <Plus className="h-4 w-4 mr-2" />
              New workspace
            </Button>
          )}

          {showCreateTenant && (
            <form className="space-y-2 rounded-xl border border-white/[0.06] bg-white/[0.02] p-3" onSubmit={onCreateTenant}>
              <Input
                placeholder="Workspace name"
                value={tenantNameInput}
                onChange={(event) => setTenantNameInput(event.target.value)}
                disabled={tenantActionLoading}
              />
              <Input
                placeholder="Optional workspace id"
                value={tenantIdInput}
                onChange={(event) => setTenantIdInput(event.target.value)}
                disabled={tenantActionLoading}
              />
              {tenantActionError ? <p className="text-xs text-destructive">{tenantActionError}</p> : null}
              <div className="flex gap-2">
                <Button type="submit" size="sm" disabled={tenantActionLoading}>{tenantActionLoading ? 'Creating...' : 'Create'}</Button>
                <Button type="button" variant="secondary" size="sm" onClick={() => setShowCreateTenant(false)} disabled={tenantActionLoading}>Cancel</Button>
              </div>
            </form>
          )}
        </div>

        {/* Navigation */}
        <div className="flex-1 min-h-0 overflow-y-auto px-3 py-4">
          <nav className="space-y-0.5">
            {navItems.map((item) => {
              const Icon = item.icon;
              const isActive = Array.isArray(item.matches) && item.matches.includes(currentPageName);

              return (
                <Link
                  key={item.page}
                  to={item.to}
                  onClick={() => setSidebarOpen(false)}
                  className={`
                    flex items-center gap-3 px-3 py-2 rounded-xl text-sm font-medium
                    transition-all duration-150
                    ${isActive
                      ? 'bg-primary/10 text-primary border border-primary/20'
                      : 'text-slate-400 hover:text-slate-200 hover:bg-white/[0.03] border border-transparent'
                    }
                  `}
                >
                  <Icon className={`h-4 w-4 ${isActive ? 'text-primary' : ''}`} />
                  {item.name}
                  {isActive && (
                    <ChevronRight className="h-3.5 w-3.5 ml-auto text-primary/60" />
                  )}
                </Link>
              );
            })}
          </nav>
        </div>

        {/* User */}
        {currentUser && (
          <div className="p-4 border-t border-white/[0.06] space-y-2 shrink-0">
            <div className="flex items-center gap-3 px-3 py-2.5 rounded-xl border border-white/[0.06] bg-white/[0.02]">
              <div className="w-8 h-8 rounded-full bg-primary/10 border border-primary/20 flex items-center justify-center text-primary text-sm font-medium">
                {currentUser.email?.charAt(0)?.toUpperCase() || 'U'}
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-slate-200 truncate">{currentUser.email || 'User'}</p>
                <p className="text-[11px] text-slate-500 truncate">{roleLabel}</p>
              </div>
            </div>
            <Button type="button" variant="ghost" className="w-full justify-start text-slate-400 hover:text-slate-200" onClick={logout}>
              <LogOut className="h-4 w-4 mr-2" />
              Sign out
            </Button>
          </div>
        )}
      </aside>

      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/40 z-30 lg:hidden backdrop-blur-sm"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      <main className="lg:pl-72 pt-16 lg:pt-0 min-h-screen">
        <div className="p-6 lg:p-8">
          <div className="mb-6 flex flex-wrap items-center justify-end gap-2 rounded-xl border border-white/[0.06] bg-white/[0.02] px-3 py-2">
            <Button
              type="button"
              variant="ghost"
              className="justify-start text-slate-300 hover:text-slate-100"
              onClick={() => setGlobalSearchOpen(true)}
            >
              <Search className="mr-2 h-4 w-4" />
              Global Search
              <span className="ml-2 rounded border border-white/[0.12] px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-slate-400">
                ⌘/Ctrl K
              </span>
            </Button>

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button type="button" variant="ghost" className="text-slate-300 hover:text-slate-100">
                  <Plus className="mr-2 h-4 w-4" />
                  Quick Add
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-48">
                <DropdownMenuItem onClick={() => navigate('/workbench/alerts?quickAdd=1')}>
                  Add Alert
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => navigate('/competitors?quickAdd=1')}>
                  Add Competitor
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>

            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="text-slate-300 hover:text-slate-100"
              onClick={() => setNotificationsOpen(true)}
            >
              <Bell className="h-4 w-4" />
            </Button>
          </div>

          {children}
        </div>
      </main>

      <Dialog open={globalSearchOpen} onOpenChange={setGlobalSearchOpen}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Global Search</DialogTitle>
            <DialogDescription>
              Search tenders from anywhere. Use keywords, buyer names, or title fragments.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <Input
              value={globalSearchQuery}
              onChange={(event) => setGlobalSearchQuery(event.target.value)}
              placeholder="Search tenders..."
              autoFocus
            />

            {globalSearchLoading ? (
              <div className="flex items-center gap-2 text-sm text-slate-400">
                <Loader2 className="h-4 w-4 animate-spin" />
                Searching...
              </div>
            ) : null}

            {globalSearchError ? (
              <p className="text-sm text-destructive">{globalSearchError}</p>
            ) : null}

            {!globalSearchLoading && !globalSearchError && globalSearchQuery.trim() && globalSearchResults.length === 0 ? (
              <p className="text-sm text-slate-400">No matching tenders found.</p>
            ) : null}

            <div className="max-h-80 space-y-2 overflow-y-auto">
              {globalSearchResults.map((item, index) => {
                const tenderId = item?.id || item?.tender_id || item?.canonical_id;
                const title = item?.title || item?.tender_name || 'Untitled tender';
                const buyer = item?.buyer_name || item?.buyer || 'Unknown buyer';
                const key = tenderId || `${title}-${index}`;

                return (
                  <button
                    type="button"
                    key={key}
                    className="w-full rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2 text-left hover:bg-white/[0.04]"
                    onClick={() => {
                      if (!tenderId) return;
                      setGlobalSearchOpen(false);
                      navigate(createPageUrl(`TenderDetail?id=${encodeURIComponent(tenderId)}`));
                    }}
                  >
                    <p className="truncate text-sm font-medium text-slate-200">{title}</p>
                    <p className="truncate text-xs text-slate-400">{buyer}</p>
                  </button>
                );
              })}
            </div>

            <div className="flex justify-end">
              <Button
                type="button"
                variant="secondary"
                onClick={() => {
                  setGlobalSearchOpen(false);
                  navigate('/workbench/search');
                }}
              >
                Open advanced search
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={notificationsOpen} onOpenChange={setNotificationsOpen}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Notifications</DialogTitle>
            <DialogDescription>Recent alert activity and quick alert actions.</DialogDescription>
          </DialogHeader>

          <div className="mt-2 space-y-5">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-semibold text-slate-200">Your Alerts</h3>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => {
                  setNotificationsOpen(false);
                  navigate('/workbench/alerts?quickAdd=1');
                }}
              >
                <Plus className="mr-1 h-3.5 w-3.5" />
                Add Alert
              </Button>
            </div>

            {alertsPreviewLoading ? (
              <div className="flex items-center gap-2 text-sm text-slate-400">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading alerts...
              </div>
            ) : null}

            {alertsPreviewError ? (
              <p className="text-sm text-destructive">{alertsPreviewError}</p>
            ) : null}

            {!alertsPreviewLoading && !alertsPreviewError && alertsPreview.length === 0 ? (
              <p className="text-sm text-slate-400">No alerts configured yet.</p>
            ) : null}

            <div className="max-h-36 space-y-2 overflow-y-auto">
              {alertsPreview.map((alert) => (
                <div key={alert.id} className="rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2">
                  <p className="truncate text-sm text-slate-200">{alert.alert_name || 'Untitled alert'}</p>
                  <p className="truncate text-xs text-slate-400">{alert.active === false ? 'Paused' : 'Active'}</p>
                </div>
              ))}
            </div>

            <div className="pt-2">
              <h3 className="mb-2 text-sm font-semibold text-slate-200">Recent Matches</h3>
              <div className="max-h-36 space-y-2 overflow-y-auto">
                {alertEventsPreview.length === 0 ? (
                  <p className="text-sm text-slate-400">No recent alert events.</p>
                ) : alertEventsPreview.map((event) => (
                  <div key={event.id} className="rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2">
                    <p className="truncate text-sm text-slate-200">{event?.tender_title || 'Tender match'}</p>
                    <p className="truncate text-xs text-slate-400">{event?.matched_at || ''}</p>
                  </div>
                ))}
              </div>
            </div>

            <div className="flex justify-end">
              <Button
                type="button"
                variant="secondary"
                onClick={() => {
                  setNotificationsOpen(false);
                  navigate('/workbench/alerts');
                }}
              >
                Open alerts workspace
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
