import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from './utils';
import { civant } from '@/api/civantClient';
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
    const [user, setUser] = useState(null);
    const [sidebarOpen, setSidebarOpen] = useState(false);
    
    useEffect(() => {
        civant.auth.me().then(setUser).catch(() => {});
    }, []);
    
    const isAdmin = user?.role === 'admin';
    
    const navItems = [
        { name: 'Home', page: 'Home', icon: LayoutDashboard },
        { name: 'Predictions', page: 'Predictions', icon: BarChart3 },
        { name: 'Search', page: 'Search', icon: Search },
        { name: 'Competitors', page: 'Competitors', icon: Radar },
        { name: 'Alerts', page: 'Alerts', icon: Bell },
        { name: 'Insights', page: 'Insights', icon: BarChart3 },
        { name: 'Integrations', page: 'Integrations', icon: Settings },
    ];

    if (isAdmin) {
        navItems.push(
            { name: 'Connectors', page: 'Connectors', icon: Zap },
            { name: 'System', page: 'System', icon: Settings },
            { name: 'Pipeline', page: 'PipelineAdmin', icon: Network },
            { name: 'Architecture', page: 'Architecture', icon: Network }
        );
    }
    
    return (
        <div className="min-h-screen bg-slate-50">
            {/* Mobile header */}
            <header className="lg:hidden fixed top-0 left-0 right-0 h-16 bg-white border-b border-slate-200 z-50 px-4 flex items-center justify-between">
                <div className="flex items-center gap-3">
                    <Button 
                        variant="ghost" 
                        size="icon"
                        onClick={() => setSidebarOpen(!sidebarOpen)}
                    >
                        {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
                    </Button>
                    <div className="flex items-center gap-2">
                        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center">
                            <Radar className="h-4 w-4 text-white" />
                        </div>
                        <span className="font-semibold text-slate-900">Civant</span>
                    </div>
                </div>
            </header>
            
            {/* Sidebar */}
            <aside className={`
                fixed top-0 left-0 h-full w-64 bg-white border-r border-slate-200 z-40
                transform transition-transform duration-200 ease-in-out
                lg:translate-x-0
                ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}
            `}>
                {/* Logo */}
                <div className="h-16 px-6 flex items-center gap-3 border-b border-slate-100">
                    <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center shadow-lg shadow-indigo-500/20">
                        <Radar className="h-5 w-5 text-white" />
                    </div>
                    <div>
                        <h1 className="font-bold text-slate-900 tracking-tight">Civant</h1>
                        <p className="text-xs text-slate-500">IE & FR Procurement</p>
                    </div>
                </div>
                
                {/* Navigation */}
                <nav className="p-4 space-y-1">
                    {navItems.map(item => {
                        const Icon = item.icon;
                        const isActive = currentPageName === item.page;
                        
                        return (
                            <Link
                                key={item.page}
                                to={createPageUrl(item.page)}
                                onClick={() => setSidebarOpen(false)}
                                className={`
                                    flex items-center gap-3 px-4 py-2.5 rounded-xl text-sm font-medium
                                    transition-all duration-150
                                    ${isActive 
                                        ? 'bg-indigo-50 text-indigo-700' 
                                        : 'text-slate-600 hover:bg-slate-50 hover:text-slate-900'
                                    }
                                `}
                            >
                                <Icon className={`h-4 w-4 ${isActive ? 'text-indigo-600' : ''}`} />
                                {item.name}
                                {isActive && (
                                    <ChevronRight className="h-4 w-4 ml-auto text-indigo-400" />
                                )}
                            </Link>
                        );
                    })}
                </nav>
                
                {/* User info */}
                {user && (
                    <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-slate-100">
                        <div className="flex items-center gap-3 px-4 py-3 rounded-xl bg-slate-50">
                            <div className="w-8 h-8 rounded-full bg-gradient-to-br from-slate-400 to-slate-600 flex items-center justify-center text-white text-sm font-medium">
                                {user.full_name?.charAt(0) || user.email?.charAt(0) || 'U'}
                            </div>
                            <div className="flex-1 min-w-0">
                                <p className="text-sm font-medium text-slate-900 truncate">
                                    {user.full_name || 'User'}
                                </p>
                                <p className="text-xs text-slate-500 truncate">{user.email}</p>
                            </div>
                            {isAdmin && (
                                <span className="text-xs px-2 py-0.5 rounded-full bg-indigo-100 text-indigo-700 font-medium">
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
                    className="fixed inset-0 bg-black/20 z-30 lg:hidden"
                    onClick={() => setSidebarOpen(false)}
                />
            )}
            
            {/* Main content */}
            <main className="lg:pl-64 pt-16 lg:pt-0 min-h-screen">
                <div className="p-6 lg:p-8">
                    {children}
                </div>
            </main>
        </div>
    );
}
