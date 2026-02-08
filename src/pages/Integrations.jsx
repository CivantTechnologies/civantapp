import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { 
    Calendar,
    MessageSquare,
    HardDrive,
    CheckCircle2,
    Loader2,
    Settings
} from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

export default function Integrations() {
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        loadUser();
    }, []);

    const loadUser = async () => {
        try {
            await civant.auth.me();
        } catch (error) {
            console.error('Error loading user:', error);
        } finally {
            setLoading(false);
        }
    };

    const integrations = [
        {
            id: 'google-calendar',
            name: 'Google Calendar',
            description: 'Automatically add tender deadlines to your calendar',
            icon: Calendar,
            color: 'text-blue-200',
            bgColor: 'bg-blue-500/15 border border-blue-400/40',
            features: [
                'Add tender deadlines to calendar',
                'Sync important dates automatically',
                'Get calendar reminders'
            ],
            available: true
        },
        {
            id: 'slack',
            name: 'Slack',
            description: 'Send alert notifications to Slack channels',
            icon: MessageSquare,
            color: 'text-violet-200',
            bgColor: 'bg-violet-500/15 border border-violet-400/40',
            features: [
                'Post tender alerts to channels',
                'Get real-time notifications',
                'Share tenders with team'
            ],
            available: true
        },
        {
            id: 'google-drive',
            name: 'Google Drive',
            description: 'Export and save tender reports to your Drive',
            icon: HardDrive,
            color: 'text-emerald-200',
            bgColor: 'bg-emerald-500/15 border border-emerald-400/40',
            features: [
                'Export tender data to Drive',
                'Save reports automatically',
                'Organize tender documents'
            ],
            available: true
        }
    ];

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-civant-teal" />
            </div>
        );
    }

    return (
        <div className="space-y-6 max-w-5xl mx-auto">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-bold text-slate-100">Integrations</h1>
                <p className="text-slate-400 mt-1">
                    Connect third-party tools to streamline your tender tracking workflow
                </p>
            </div>

            {/* Integration Cards */}
            <div className="grid gap-6">
                {integrations.map(integration => {
                    const Icon = integration.icon;
                    
                    return (
                        <Card key={integration.id} className="border border-civant-border bg-civant-navy/55 shadow-none">
                            <CardContent className="p-6">
                                <div className="flex items-start gap-4">
                                    {/* Icon */}
                                    <div className={`p-4 rounded-xl ${integration.bgColor}`}>
                                        <Icon className={`h-6 w-6 ${integration.color}`} />
                                    </div>

                                    {/* Content */}
                                    <div className="flex-1">
                                        <div className="flex items-center gap-3 mb-2">
                                            <h3 className="text-lg font-semibold text-slate-100">
                                                {integration.name}
                                            </h3>
                                            {integration.available ? (
                                                <Badge className="bg-emerald-500/15 text-emerald-200 border border-emerald-400/40">
                                                    Available
                                                </Badge>
                                            ) : (
                                                <Badge variant="outline" className="text-slate-300 border-slate-600">
                                                    Coming Soon
                                                </Badge>
                                            )}
                                        </div>
                                        
                                        <p className="text-slate-300 mb-4">
                                            {integration.description}
                                        </p>

                                        {/* Features */}
                                        <ul className="space-y-2 mb-4">
                                            {integration.features.map((feature) => (
                                                <li key={feature} className="flex items-center gap-2 text-sm text-slate-300">
                                                    <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                                                    {feature}
                                                </li>
                                            ))}
                                        </ul>

                                        <p className="text-sm text-slate-200 bg-civant-navy/70 border border-civant-border rounded-lg p-3">
                                            <strong className="text-civant-teal">Admin Setup Required:</strong> Contact your administrator to authorize this integration for the organization.
                                        </p>
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    );
                })}
            </div>

            {/* Info Box */}
            <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                <CardContent className="p-6">
                    <div className="flex gap-4">
                        <Settings className="h-5 w-5 text-civant-teal mt-0.5" />
                        <div>
                            <h3 className="font-semibold text-slate-100 mb-2">
                                How Integrations Work
                            </h3>
                            <ul className="space-y-2 text-sm text-slate-300">
                                <li>• Integrations are set up once by an admin for the entire organization</li>
                                <li>• Once connected, integration features appear throughout the app</li>
                                <li>• Look for "Add to Calendar", "Send to Slack", and "Export to Drive" buttons</li>
                                <li>• All data is securely synced with your connected accounts</li>
                            </ul>
                        </div>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
