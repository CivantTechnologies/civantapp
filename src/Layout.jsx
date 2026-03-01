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
  ChevronDown,
  Zap,
  Network,
  LogOut,
  Plus,
  Loader2,
  Check,
  UserRound,
  FileText
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
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger
} from '@/components/ui/dropdown-menu';

export default function Layout({ children, currentPageName }) {
  const { session, currentUser, roles, profileStatus, logout, authWarning } = useAuth();
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
  const [commandCursor, setCommandCursor] = useState(0);
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [alertsPreviewLoading, setAlertsPreviewLoading] = useState(false);
  const [alertsPreviewError, setAlertsPreviewError] = useState('');
  const [alertsPreview, setAlertsPreview] = useState([]);
  const [alertEventsPreview, setAlertEventsPreview] = useState([]);

  const selectedTenant = useMemo(
    () => tenants.find((tenant) => tenant.id === activeTenantId) || null,
    [tenants, activeTenantId]
  );

  const canAccessSystem = profileStatus === 'ready' && Array.isArray(roles) && (roles.includes('admin') || roles.includes('creator') || roles.includes('super_admin'));
  const isPlatformTenant = selectedTenant?.is_platform_admin === true;

  /* ── Derived user info for avatar dropdown ── */

  const tenantCount = Array.isArray(tenants) ? tenants.length : 0;
  const canSwitchWorkspace = tenantCount > 1;
  const userMetadata = session?.user?.user_metadata || currentUser?.user_metadata || {};
  const userDisplayName = String(
    userMetadata.full_name
    || userMetadata.name
    || currentUser?.email?.split('@')?.[0]
    || 'User'
  );
  const userAvatarUrl = userMetadata.avatar_url || userMetadata.picture || userMetadata.photo_url || '';
  const avatarInitial = String(userDisplayName || currentUser?.email || 'U').trim().charAt(0).toUpperCase() || 'U';

  /* ── Sidebar nav items (current RBAC gating) ── */

  const navItems = useMemo(() => {
    const base = [
      { name: 'Panorama', page: 'Home', icon: LayoutDashboard },
      { name: 'Forecast', page: 'Forecast', icon: BarChart3 },
      { name: 'Finder', page: 'Search', icon: Search },
      { name: 'Competitors', page: 'Competitors', icon: Radar },
      { name: 'Reports', page: 'Reports', icon: FileText },
      { name: 'Company', page: 'Company', icon: Building2 },
    ];

    if (isPlatformTenant) {
      base.push(
        { name: 'Integrations', page: 'Integrations', icon: Settings },
        { name: 'Connectors', page: 'Connectors', icon: Zap },
        { name: 'Pipeline', page: 'PipelineAdmin', icon: Network },
        { name: 'Architecture', page: 'Architecture', icon: Network },
      );
    }

    if (canAccessSystem && isPlatformTenant) {
      base.push({ name: 'System', page: 'System', icon: Settings });
    }

    return base;
  }, [isPlatformTenant, canAccessSystem]);

  /* ── Global search (Cmd+K) command palette ── */

  const commandGroups = useMemo(() => {
    const keyword = globalSearchQuery.trim();

    const tenderItems = keyword
      ? globalSearchResults.slice(0, 8).map((item, index) => {
        const tenderId = item?.id || item?.tender_id || item?.canonical_id;
        const title = item?.title || item?.tender_name || 'Untitled tender';
        const buyer = item?.buyer_name || item?.buyer || 'Unknown buyer';

        return {
          id: `tender-${tenderId || index}`,
          label: title,
          subtitle: buyer,
          onSelect: () => {
            if (!tenderId) return;
            setGlobalSearchOpen(false);
            navigate(createPageUrl(`TenderDetail?id=${encodeURIComponent(tenderId)}`));
          }
        };
      })
      : [];

    const buyerSet = new Set();
    const buyerItems = keyword
      ? globalSearchResults
        .map((item) => item?.buyer_name || item?.buyer || '')
        .filter((buyerName) => {
          const normalized = String(buyerName || '').trim().toLowerCase();
          if (!normalized || buyerSet.has(normalized)) return false;
          buyerSet.add(normalized);
          return true;
        })
        .slice(0, 6)
        .map((buyerName) => ({
          id: `buyer-${buyerName}`,
          label: buyerName,
          subtitle: 'Open tenders by this buyer',
          onSelect: () => {
            setGlobalSearchOpen(false);
            navigate(`/workbench/search?buyer=${encodeURIComponent(buyerName)}`);
          }
        }))
      : [];

    const competitorItems = [
      {
        id: 'competitors-open',
        label: 'Open competitor intelligence',
        subtitle: 'View tracked competitors and analysis',
        onSelect: () => {
          setGlobalSearchOpen(false);
          navigate('/competitors');
        }
      },
      {
        id: 'competitors-add',
        label: 'Add competitor',
        subtitle: 'Create a new competitor profile',
        onSelect: () => {
          setGlobalSearchOpen(false);
          navigate('/competitors?quickAdd=1');
        }
      }
    ];

    const alertItems = [
      {
        id: 'alerts-open',
        label: 'Open alerts workspace',
        subtitle: 'Review and manage your alert rules',
        onSelect: () => {
          setGlobalSearchOpen(false);
          navigate('/workbench/alerts');
        }
      },
      {
        id: 'alerts-add',
        label: 'Add alert',
        subtitle: 'Create a new monitoring alert',
        onSelect: () => {
          setGlobalSearchOpen(false);
          navigate('/workbench/alerts?quickAdd=1');
        }
      }
    ];

    return [
      { key: 'tenders', label: 'Tenders', items: tenderItems },
      { key: 'buyers', label: 'Buyers', items: buyerItems },
      { key: 'competitors', label: 'Competitors', items: competitorItems },
      { key: 'alerts', label: 'Alerts', items: alertItems }
    ];
  }, [globalSearchQuery, globalSearchResults, navigate]);

  const flatCommandItems = useMemo(
    () => commandGroups.flatMap((group) => group.items),
    [commandGroups]
  );

  /* ── Keyboard shortcut: Cmd/Ctrl+K ── */

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

  /* ── URL param listeners (openSearch, openNotifications) ── */

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

  /* ── Global search debounced query ── */

  useEffect(() => {
    if (!globalSearchOpen) return undefined;
    const keyword = globalSearchQuery.trim();
    if (!keyword) {
      setGlobalSearchResults([]);
      setGlobalSearchError('');
      setGlobalSearchLoading(false);
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

  /* ── Notifications loader ── */

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

  /* ── Command cursor sync ── */

  useEffect(() => {
    if (!globalSearchOpen) {
      setCommandCursor(0);
      return;
    }
    setCommandCursor((previous) => {
      if (flatCommandItems.length === 0) return 0;
      return Math.min(previous, flatCommandItems.length - 1);
    });
  }, [globalSearchOpen, flatCommandItems.length]);

  /* ── Handlers ── */

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

  const onCommandInputKeyDown = (event) => {
    if (!flatCommandItems.length) return;

    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setCommandCursor((cursor) => (cursor + 1) % flatCommandItems.length);
      return;
    }

    if (event.key === 'ArrowUp') {
      event.preventDefault();
      setCommandCursor((cursor) => (cursor - 1 + flatCommandItems.length) % flatCommandItems.length);
      return;
    }

    if (event.key === 'Enter') {
      event.preventDefault();
      const selected = flatCommandItems[commandCursor];
      selected?.onSelect?.();
    }
  };

  let runningCommandIndex = 0;

  /* ── Render ── */

  return (
    <div className="min-h-screen bg-background text-foreground">
      {authWarning ? (
        <div className="mx-4 mt-4 rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
          {authWarning}
        </div>
      ) : null}

      {/* ── Top bar (sleek version) ── */}
      <header className="civant-topbar fixed inset-x-0 top-0 z-50 h-14 border-b border-white/[0.06] bg-background/80 backdrop-blur-md">
        <div className="flex h-full items-center gap-4 px-4 lg:px-8">
          {/* Left: hamburger + logo + stacked brand/tenant */}
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <Button
              variant="ghost"
              size="icon"
              className="h-9 w-9 text-slate-300 lg:hidden"
              onClick={() => setSidebarOpen((open) => !open)}
            >
              {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
            </Button>

            <div className="flex items-center gap-3">
              <Link to="/home" className="text-slate-100">
                <div className="h-9 w-9 overflow-hidden rounded-lg ring-1 ring-primary/30">
                  <img src="/apple-touch-icon.png" alt="Civant mark" className="h-full w-full object-cover" />
                </div>
              </Link>
              <div className="hidden min-w-0 flex-col sm:flex">
                <Link to="/home" className="text-base font-semibold leading-tight tracking-tight text-slate-100">
                  Civant
                </Link>
                <span className="civant-tenant-label truncate text-[11px] leading-tight text-slate-500">
                  {selectedTenant?.name || 'Workspace'}
                </span>
              </div>
            </div>
          </div>

          {/* Right: search, quick-add, avatar dropdown */}
          <div className="flex items-center justify-end gap-4">
            <div className="flex items-center gap-2">
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="civant-icon-button h-9 w-9 text-slate-300"
                onClick={() => setGlobalSearchOpen(true)}
                aria-label="Open global search"
                title="Search (Cmd/Ctrl+K)"
              >
                <Search className="h-5 w-5" />
              </Button>
            </div>

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="civant-icon-button h-9 w-9 text-slate-300"
                  aria-label="Quick add"
                >
                  <Plus className="h-5 w-5" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-52">
                <DropdownMenuItem onClick={() => navigate('/workbench/alerts?quickAdd=1')}>
                  Add Alert
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => navigate('/competitors?quickAdd=1')}>
                  Add Competitor
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>

            {/* Avatar dropdown with workspace switcher, profile, sign out */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button
                  type="button"
                  className="civant-icon-button flex h-9 w-9 items-center justify-center overflow-hidden rounded-full border border-white/[0.08] bg-white/[0.03] text-sm font-medium text-slate-200"
                  aria-label="Open account menu"
                >
                  {userAvatarUrl ? (
                    <img src={userAvatarUrl} alt={userDisplayName} className="h-full w-full object-cover" />
                  ) : (
                    avatarInitial
                  )}
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent
                align="end"
                className="w-[300px] rounded-2xl border-white/[0.08] bg-slate-950/95 p-1.5 shadow-xl backdrop-blur-xl duration-150 ease-out"
              >
                <DropdownMenuLabel className="px-3 py-2.5">
                  <p className="truncate text-sm font-medium text-slate-100">{userDisplayName}</p>
                </DropdownMenuLabel>
                <DropdownMenuSeparator className="bg-white/[0.06]" />

                {canSwitchWorkspace ? (
                  <DropdownMenuSub>
                    <DropdownMenuSubTrigger className="rounded-lg px-3 py-2 text-sm text-slate-300 focus:bg-white/[0.05] focus:text-slate-100 data-[state=open]:bg-white/[0.05] data-[state=open]:text-slate-100">
                      Switch workspace
                    </DropdownMenuSubTrigger>
                    <DropdownMenuSubContent className="w-56 rounded-xl border-white/[0.08] bg-slate-950/95 p-1.5 shadow-xl">
                      {tenants.map((tenant) => {
                        const isSelected = tenant.id === activeTenantId;
                        return (
                          <DropdownMenuItem
                            key={tenant.id}
                            className="rounded-lg px-3 py-2 text-sm text-slate-300 focus:bg-white/[0.05] focus:text-slate-100"
                            onClick={() => setActiveTenantId(tenant.id)}
                          >
                            <span className="truncate">{tenant.name}</span>
                            {isSelected ? <Check className="ml-auto h-4 w-4 text-primary/80" /> : null}
                          </DropdownMenuItem>
                        );
                      })}
                    </DropdownMenuSubContent>
                  </DropdownMenuSub>
                ) : null}

                <DropdownMenuItem
                  className="rounded-lg px-3 py-2 text-sm text-slate-300 focus:bg-white/[0.05] focus:text-slate-100"
                  onClick={() => navigate('/company/profile')}
                >
                  Profile / Account
                </DropdownMenuItem>
                <DropdownMenuSeparator className="bg-white/[0.06]" />
                <DropdownMenuItem
                  className="rounded-lg px-3 py-2 text-sm text-slate-300 focus:bg-white/[0.05] focus:text-slate-100"
                  onClick={logout}
                >
                  <LogOut className="h-4 w-4" />
                  Sign out
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>
        {tenantError ? (
          <p className="px-4 pb-1 text-[11px] text-destructive lg:px-8">{tenantError}</p>
        ) : null}
      </header>

      {/* ── Sidebar (below top bar, current RBAC flat nav) ── */}
      <aside className={`
        civant-sidebar-shell fixed left-0 top-14 z-40 h-[calc(100vh-56px)] w-60 border-r border-white/[0.02] bg-background/68 backdrop-blur-md
        civant-motion-standard transition-transform lg:translate-x-0
        ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}
      `}>
        <div className="flex h-full flex-col overflow-hidden">
          {/* Mobile workspace switcher (hidden on desktop, workspace is in avatar dropdown) */}
          <div className="space-y-1 px-2 pt-4 sm:hidden">
            <label className="text-[10px] uppercase tracking-[0.14em] text-slate-500">Workspace</label>
            <div className="relative">
              <select
                className="h-9 w-full appearance-none rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 pr-8 text-xs text-slate-200 transition-colors focus:border-primary/40 focus:outline-none"
                value={activeTenantId}
                onChange={(event) => setActiveTenantId(event.target.value)}
                disabled={isLoadingTenants}
              >
                {tenants.map((tenant) => (
                  <option key={tenant.id} value={tenant.id}>{tenant.name}</option>
                ))}
              </select>
              <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500" />
            </div>
          </div>

          {/* Navigation */}
          <div className="flex-1 min-h-0 overflow-y-auto px-2 py-4">
            <nav className="space-y-0.5">
              {navItems.map((item) => {
                const Icon = item.icon;
                const isActive = currentPageName === item.page;

                return (
                  <Link
                    key={item.page}
                    to={createPageUrl(item.page)}
                    onClick={() => setSidebarOpen(false)}
                    className={`
                      civant-nav-item flex items-center gap-3 rounded-xl border px-3 py-2 text-sm font-medium transition-all duration-150
                      ${isActive
                        ? 'border-primary/20 bg-primary/8 text-slate-100'
                        : 'border-transparent text-slate-500 hover:border-white/[0.05] hover:bg-white/[0.02] hover:text-slate-300'}
                    `}
                  >
                    <Icon className={`h-5 w-5 ${isActive ? 'text-primary/80' : 'text-slate-500'}`} />
                    {item.name}
                  </Link>
                );
              })}
            </nav>
          </div>

          {/* Bottom: create tenant (platform admin only) */}
          {(canCreateTenant && isPlatformTenant) || showCreateTenant ? (
            <div className="shrink-0 border-t border-white/[0.05] p-4 space-y-3">
              {canCreateTenant && isPlatformTenant ? (
                <Button
                  type="button"
                  variant="ghost"
                  className="w-full justify-start text-slate-500 hover:text-slate-300"
                  onClick={() => setShowCreateTenant((v) => !v)}
                >
                  <Plus className="mr-2 h-4 w-4" />
                  New workspace
                </Button>
              ) : null}

              {showCreateTenant ? (
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
              ) : null}
            </div>
          ) : null}
        </div>
      </aside>

      {/* Mobile overlay */}
      {sidebarOpen ? (
        <div
          className="fixed inset-0 top-14 z-30 bg-black/40 backdrop-blur-sm lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      ) : null}

      {/* Main content */}
      <main className="min-h-screen pt-14 lg:pl-60">
        <div className="civant-main-shell">
          {children}
        </div>
      </main>

      {/* ── Global Search Dialog (Cmd+K) ── */}
      <Dialog open={globalSearchOpen} onOpenChange={setGlobalSearchOpen}>
        <DialogContent className="w-[min(92vw,680px)] max-w-[680px] gap-0 rounded-2xl border border-white/[0.1] bg-background/95 p-0 shadow-2xl backdrop-blur-xl data-[state=open]:duration-150 data-[state=closed]:duration-150 data-[state=open]:zoom-in-[98%] data-[state=closed]:zoom-out-[98%]">
          <DialogHeader className="sr-only">
            <DialogTitle>Global Search</DialogTitle>
            <DialogDescription>Search tenders, buyers, competitors, and alerts.</DialogDescription>
          </DialogHeader>

          <div className="border-b border-white/[0.06] px-5 py-4">
            <div className="flex items-center gap-3 rounded-xl border border-white/[0.1] bg-white/[0.03] px-3 py-2.5">
              <Search className="h-5 w-5 text-slate-400" />
              <Input
                value={globalSearchQuery}
                onChange={(event) => setGlobalSearchQuery(event.target.value)}
                onKeyDown={onCommandInputKeyDown}
                placeholder="Search tenders, buyers, competitors, alerts"
                autoFocus
                className="h-auto border-0 bg-transparent p-0 text-base text-slate-100 placeholder:text-slate-500 focus-visible:ring-0"
              />
            </div>
          </div>

          <div className="max-h-[420px] space-y-4 overflow-y-auto px-5 py-4">
            {globalSearchLoading ? (
              <div className="flex items-center gap-2 text-sm text-slate-400">
                <Loader2 className="h-4 w-4 animate-spin" />
                Searching...
              </div>
            ) : null}

            {globalSearchError ? (
              <p className="text-sm text-destructive">{globalSearchError}</p>
            ) : null}

            {!globalSearchLoading && !globalSearchError
              ? commandGroups.map((group) => {
                if (group.items.length === 0) {
                  if (group.key === 'tenders' && globalSearchQuery.trim()) {
                    return (
                      <div key={group.key} className="space-y-2">
                        <p className="text-[10px] uppercase tracking-[0.14em] text-slate-500">{group.label}</p>
                        <p className="rounded-xl border border-white/[0.06] bg-white/[0.02] px-3 py-2 text-sm text-slate-500">
                          No matching tenders.
                        </p>
                      </div>
                    );
                  }
                  return null;
                }

                return (
                  <div key={group.key} className="space-y-2">
                    <p className="text-[10px] uppercase tracking-[0.14em] text-slate-500">{group.label}</p>
                    <div className="space-y-1">
                      {group.items.map((item) => {
                        const itemIndex = runningCommandIndex;
                        runningCommandIndex += 1;
                        const isActive = itemIndex === commandCursor;

                        return (
                          <button
                            key={item.id}
                            type="button"
                            className={`
                              flex w-full items-start justify-between rounded-xl border px-3 py-2 text-left transition-all duration-150
                              ${isActive
                                ? 'border-primary/40 bg-primary/10 text-slate-100'
                                : 'border-transparent bg-white/[0.02] text-slate-200 hover:border-white/[0.08] hover:bg-white/[0.04]'}
                            `}
                            onMouseEnter={() => setCommandCursor(itemIndex)}
                            onClick={() => item.onSelect()}
                          >
                            <span className="min-w-0">
                              <span className="block truncate text-sm font-medium">{item.label}</span>
                              {item.subtitle ? <span className="block truncate text-xs text-slate-500">{item.subtitle}</span> : null}
                            </span>
                            <ChevronRight className="mt-0.5 h-4 w-4 shrink-0 text-slate-500" />
                          </button>
                        );
                      })}
                    </div>
                  </div>
                );
              })
              : null}
          </div>

          <div className="flex items-center justify-between border-t border-white/[0.06] px-5 py-3">
            <p className="text-xs text-slate-500">Use Up/Down and Enter. Press Esc to close.</p>
            <Button
              type="button"
              variant="ghost"
              className="text-slate-300 hover:text-slate-100"
              onClick={() => {
                setGlobalSearchOpen(false);
                navigate('/workbench/search');
              }}
            >
              Advanced search
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* ── Notifications Dialog ── */}
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
