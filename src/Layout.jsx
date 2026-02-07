import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from './utils';
import { useAuth } from '@/lib/auth';
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
  LogOut
} from 'lucide-react';
import { Button } from '@/components/ui/button';

export default function Layout({ children, currentPageName }) {
  const { currentUser, roles, logout } = useAuth();
  const tenantName = 'Civant';
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const canAccessSystem = Array.isArray(roles) && (roles.includes('admin') || roles.includes('creator'));

  const navItems = [
    { name: 'Home', page: 'Home', icon: LayoutDashboard },
    { name: 'Predictions', page: 'Predictions', icon: BarChart3 },
    { name: 'Search', page: 'Search', icon: Search },
    { name: 'Competitors', page: 'Competitors', icon: Radar },
    { name: 'Alerts', page: 'Alerts', icon: Bell },
    { name: 'Insights', page: 'Insights', icon: BarChart3 },
    { name: 'Integrations', page: 'Integrations', icon: Settings },
    { name: 'Connectors', page: 'Connectors', icon: Zap },
    { name: 'Pipeline', page: 'PipelineAdmin', icon: Network },
    { name: 'Architecture', page: 'Architecture', icon: Network }
  ];

  if (canAccessSystem) {
    navItems.push({ name: 'System', page: 'System', icon: Settings });
  }

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="lg:hidden fixed top-0 left-0 right-0 h-16 bg-card/95 border-b border-border z-50 px-4 flex items-center justify-between backdrop-blur-sm">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="icon" onClick={() => setSidebarOpen(!sidebarOpen)}>
            {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
          </Button>
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-primary/20 border border-primary/30 flex items-center justify-center">
              <Radar className="h-4 w-4 text-primary" />
            </div>
            <span className="font-semibold text-card-foreground">{tenantName}</span>
          </div>
        </div>
        <Button variant="primary" size="sm" asChild>
          <Link to={createPageUrl('Home')}>Dashboard</Link>
        </Button>
      </header>

      <aside className={`
        fixed top-0 left-0 h-full w-72 bg-card border-r border-border z-40
        transform transition-transform duration-200 ease-in-out
        lg:translate-x-0
        ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}
      `}>
        <div className="h-16 px-6 flex items-center gap-3 border-b border-border">
          <div className="w-9 h-9 rounded-xl bg-primary/20 border border-primary/30 flex items-center justify-center">
            <Radar className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h1 className="font-bold text-card-foreground tracking-tight">{tenantName}</h1>
            <p className="text-xs text-muted-foreground">IE & FR Procurement</p>
          </div>
        </div>

        <nav className="p-4 space-y-1.5">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = currentPageName === item.page;

            return (
              <Link
                key={item.page}
                to={createPageUrl(item.page)}
                onClick={() => setSidebarOpen(false)}
                className={`
                  flex items-center gap-3 px-4 py-2.5 rounded-xl text-sm font-medium border
                  transition-all duration-150
                  ${isActive
      ? 'border-primary/30 bg-primary/20 text-card-foreground'
      : 'border-transparent text-muted-foreground hover:border-border hover:bg-muted/60 hover:text-card-foreground'
    }
                `}
              >
                <Icon className={`h-4 w-4 ${isActive ? 'text-primary' : ''}`} />
                {item.name}
                {isActive && (
                  <ChevronRight className="h-4 w-4 ml-auto text-primary" />
                )}
              </Link>
            );
          })}
        </nav>

        {currentUser && (
          <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-border space-y-3">
            <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-border bg-muted/40">
              <div className="w-8 h-8 rounded-full bg-primary/20 border border-primary/30 flex items-center justify-center text-primary text-sm font-medium">
                {currentUser.email?.charAt(0) || 'U'}
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-card-foreground truncate">{currentUser.email || 'User'}</p>
                <p className="text-xs text-muted-foreground truncate">{roles.join(', ') || 'user'}</p>
              </div>
            </div>
            <Button type="button" variant="ghost" className="w-full justify-start" onClick={logout}>
              <LogOut className="h-4 w-4 mr-2" />
              Sign out
            </Button>
          </div>
        )}
      </aside>

      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/40 z-30 lg:hidden"
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
