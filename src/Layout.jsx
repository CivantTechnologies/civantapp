import React, { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from './utils';
import { useAuth } from '@/lib/auth';
import { useTenant } from '@/lib/tenant';
import {
  LayoutDashboard,
  Search,
  Bell,
  BarChart3,
  Settings,
  Menu,
  X,
  Radar,
  ChevronRight,
  Zap,
  Network,
  LogOut,
  Plus,
  UserRound,
  FileText,
  Building2
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

export default function Layout({ children, currentPageName }) {
  const { currentUser, roles, profileStatus, logout, authWarning } = useAuth();
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

  const selectedTenant = useMemo(
    () => tenants.find((tenant) => tenant.id === activeTenantId) || null,
    [tenants, activeTenantId]
  );

  const canAccessSystem = profileStatus === 'ready' && Array.isArray(roles) && (roles.includes('admin') || roles.includes('creator'));
  const roleLabel = Array.isArray(roles) && roles.length ? roles.join(', ') : (profileStatus === 'ready' ? 'user' : 'Checking permissions...');

  const isPlatformTenant = selectedTenant?.is_platform_admin === true;

  const navItems = [
    { name: 'Panorama', page: 'Home', icon: LayoutDashboard },
    { name: 'Forecast', page: 'Forecast', icon: BarChart3 },
    { name: 'Finder', page: 'Search', icon: Search },
    { name: 'Competitors', page: 'Competitors', icon: Radar },
    { name: 'Reports', page: 'Reports', icon: FileText },
    { name: 'Company', page: 'Company', icon: Building2 },
  ];

  if (isPlatformTenant) {
    navItems.push(
      { name: 'Profile', page: 'Profile', icon: UserRound },
      { name: 'Alerts', page: 'Alerts', icon: Bell },
      { name: 'Insights', page: 'Insights', icon: BarChart3 },
      { name: 'Integrations', page: 'Integrations', icon: Settings },
      { name: 'Connectors', page: 'Connectors', icon: Zap },
      { name: 'Pipeline', page: 'PipelineAdmin', icon: Network },
      { name: 'Architecture', page: 'Architecture', icon: Network },
    );
  }

  if (canAccessSystem && isPlatformTenant) {
    navItems.push({ name: 'System', page: 'System', icon: Settings });
  }

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

          {canCreateTenant && isPlatformTenant && (
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
              const isActive = currentPageName === item.page;

              return (
                <Link
                  key={item.page}
                  to={createPageUrl(item.page)}
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
          {children}
        </div>
      </main>
    </div>
  );
}
