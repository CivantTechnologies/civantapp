import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from './utils';
import { useAuth } from '@/lib/AuthProvider';
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
    Network
} from 'lucide-react';
import { Button } from '@/components/ui/button';

export default function Layout({ children, currentPageName }) {
    const { user, capabilities, tenantInfo, isLoadingCapabilities } = useAuth();
    const tenantName = tenantInfo?.name || 'Civant';
    const [sidebarOpen, setSidebarOpen] = useState(false);
    const isAdmin = Boolean(capabilities?.isAdmin);
    
    const navItems = [
        { name: 'Home', page: 'Home', icon: LayoutDashboard },
        { name: 'Predictions', page: 'Predictions', icon: BarChart3 },
        { name: 'Search', page: 'Search', icon: Search },
        { name: 'Competitors', page: 'Competitors', icon: Radar },
        { name: 'Alerts', page: 'Alerts', icon: Bell },
        { name: 'Insights', page: 'Insights', icon: BarChart3 },
        { name: 'Integrations', page: 'Integrations', icon: Settings },
    ];

    if (!isLoadingCapabilities && isAdmin) {
        navItems.push(
            { name: 'Connectors', page: 'Connectors', icon: Zap },
            { name: 'System', page: 'System', icon: Settings },
            { name: 'Pipeline', page: 'PipelineAdmin', icon: Network },
            { name: 'Architecture', page: 'Architecture', icon: Network }
        );
    }
    
    return (
        <div className="min-h-screen bg-background text-foreground">
            {/* Mobile header */}
            <header className="lg:hidden fixed top-0 left-0 right-0 h-16 bg-card/95 border-b border-border z-50 px-4 flex items-center justify-between backdrop-blur-sm">
                <div className="flex items-center gap-3">
                    <Button 
                        variant="ghost" 
                        size="icon"
                        onClick={() => setSidebarOpen(!sidebarOpen)}
                    >
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
            
            {/* Sidebar */}
            <aside className={`
                fixed top-0 left-0 h-full w-72 bg-card border-r border-border z-40
                transform transition-transform duration-200 ease-in-out
                lg:translate-x-0
                ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}
            `}>
                {/* Logo */}
                <div className="h-16 px-6 flex items-center gap-3 border-b border-border">
                    <div className="w-9 h-9 rounded-xl bg-primary/20 border border-primary/30 flex items-center justify-center">
                        <Radar className="h-5 w-5 text-primary" />
                    </div>
                    <div>
                        <h1 className="font-bold text-card-foreground tracking-tight">{tenantName}</h1>
                        <p className="text-xs text-muted-foreground">IE & FR Procurement</p>
                    </div>
                </div>
                
                {/* Navigation */}
                <nav className="p-4 space-y-1.5">
                    {navItems.map(item => {
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
                
                {/* User info */}
                {user && (
                    <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-border">
                        <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-border bg-muted/40">
                            <div className="w-8 h-8 rounded-full bg-primary/20 border border-primary/30 flex items-center justify-center text-primary text-sm font-medium">
                                {user.full_name?.charAt(0) || user.email?.charAt(0) || 'U'}
                            </div>
                            <div className="flex-1 min-w-0">
                                <p className="text-sm font-medium text-card-foreground truncate">
                                    {user.full_name || 'User'}
                                </p>
                                <p className="text-xs text-muted-foreground truncate">{user.email}</p>
                            </div>
                            {isAdmin && (
                                <span className="text-xs px-2 py-0.5 rounded-full bg-primary/20 text-primary font-semibold border border-primary/30">
                                    Admin
                                </span>
                            )}
                        </div>
                    </div>
                )}
            </aside>
            
            {/* Mobile overlay */}
            {sidebarOpen && (
                <div 
                    className="fixed inset-0 bg-black/40 z-30 lg:hidden"
                    onClick={() => setSidebarOpen(false)}
                />
            )}
            
            {/* Main content */}
            <main className="lg:pl-72 pt-16 lg:pt-0 min-h-screen">
                <div className="p-6 lg:p-8">
                    {children}
                </div>
            </main>
        </div>
    );
}
