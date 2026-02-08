import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { 
    Bell, 
    Plus, 
    Edit2,
    Trash2,
    Play,
    Pause,
    Loader2,
    Search,
    Filter,
    Mail,
    Clock,
    CalendarX
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
} from "@/components/ui/dialog";
import { format, formatDistanceToNow } from 'date-fns';

export default function Alerts() {
    const [user, setUser] = useState(null);
    const [alerts, setAlerts] = useState([]);
    const [alertEvents, setAlertEvents] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showForm, setShowForm] = useState(false);
    const [editingAlert, setEditingAlert] = useState(null);
    const [saving, setSaving] = useState(false);
    const [saveError, setSaveError] = useState('');
    
    // Form state
    const [formData, setFormData] = useState({
        alert_name: '',
        country: '',
        keywords: '',
        buyer_contains: '',
        cpv_contains: '',
        deadline_within_days: '',
        notification_frequency: 'immediate',
        expiry_date: '',
        active: true
    });
    
    // Check URL params for prefilled data
    useEffect(() => {
        const urlParams = new URLSearchParams(window.location.search);
        const buyer = urlParams.get('buyer');
        const keyword = urlParams.get('keyword');
        
        if (buyer || keyword) {
            setFormData(prev => ({
                ...prev,
                buyer_contains: buyer || '',
                keywords: keyword || '',
                alert_name: `Alert for ${buyer || keyword || 'New Tender'}`
            }));
            setShowForm(true);
        }
    }, []);
    
    useEffect(() => {
        loadData();
    }, []);
    
    const loadData = async () => {
        try {
            const userData = await civant.auth.me();
            setUser(userData);
            
            // Load user's alerts
            const alertsData = await civant.entities.Alerts.filter({
                user_email: userData.email
            });
            setAlerts(alertsData);
            
            // Load alert events
            const eventsData = await civant.entities.AlertEvents.list('-matched_at', 100);
            setAlertEvents(eventsData);
            
        } catch (error) {
            console.error('Error loading data:', error);
        } finally {
            setLoading(false);
        }
    };
    
    const handleSubmit = async (e) => {
        e.preventDefault();
        setSaving(true);
        setSaveError('');
        
        try {
            const alertData = {
                ...formData,
                user_email: user.email,
                deadline_within_days: formData.deadline_within_days ? parseInt(formData.deadline_within_days) : null,
                country: formData.country || null
            };
            
            if (editingAlert) {
                await civant.entities.Alerts.update(editingAlert.id, alertData);
            } else {
                await civant.entities.Alerts.create(alertData);
            }
            
            setShowForm(false);
            setEditingAlert(null);
            resetForm();
            await loadData();
            
        } catch (error) {
            console.error('Error saving alert:', error);
            setSaveError(error instanceof Error ? error.message : 'Unable to save alert. Please try again.');
        } finally {
            setSaving(false);
        }
    };
    
    const resetForm = () => {
        setFormData({
            alert_name: '',
            country: '',
            keywords: '',
            buyer_contains: '',
            cpv_contains: '',
            deadline_within_days: '',
            notification_frequency: 'immediate',
            expiry_date: '',
            active: true
        });
    };
    
    const handleEdit = (alert) => {
        setEditingAlert(alert);
        setFormData({
            alert_name: alert.alert_name || '',
            country: alert.country || '',
            keywords: alert.keywords || '',
            buyer_contains: alert.buyer_contains || '',
            cpv_contains: alert.cpv_contains || '',
            deadline_within_days: alert.deadline_within_days?.toString() || '',
            notification_frequency: alert.notification_frequency || 'immediate',
            expiry_date: alert.expiry_date || '',
            active: alert.active !== false
        });
        setShowForm(true);
    };
    
    const handleDelete = async (alertId) => {
        if (!confirm('Are you sure you want to delete this alert?')) return;
        
        try {
            await civant.entities.Alerts.delete(alertId);
            await loadData();
        } catch (error) {
            console.error('Error deleting alert:', error);
        }
    };
    
    const toggleActive = async (alert) => {
        try {
            await civant.entities.Alerts.update(alert.id, {
                active: !alert.active
            });
            await loadData();
        } catch (error) {
            console.error('Error toggling alert:', error);
        }
    };
    
    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-civant-teal" />
            </div>
        );
    }
    
    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-slate-100">Alerts</h1>
                    <p className="text-slate-400 mt-1">Get notified when matching tenders are published</p>
                </div>
                <Button 
                    className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90"
                    onClick={() => {
                        setEditingAlert(null);
                        resetForm();
                        setShowForm(true);
                    }}
                >
                    <Plus className="h-4 w-4 mr-2" />
                    New Alert
                </Button>
            </div>
            
            {/* Alerts List */}
            <div className="grid gap-4">
                {alerts.length === 0 ? (
                    <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                        <CardContent className="py-12 text-center">
                            <Bell className="h-12 w-12 mx-auto text-slate-300 mb-4" />
                            <h3 className="text-lg font-semibold text-slate-100 mb-2">No alerts yet</h3>
                            <p className="text-slate-400 mb-4">Create an alert to get notified about matching tenders</p>
                            <Button onClick={() => setShowForm(true)}>
                                <Plus className="h-4 w-4 mr-2" />
                                Create Your First Alert
                            </Button>
                        </CardContent>
                    </Card>
                ) : (
                    alerts.map(alert => {
                        const matchCount = alertEvents.filter(e => e.alert_id === alert.id).length;
                        
                        return (
                            <Card key={alert.id} className="border border-civant-border bg-civant-navy/55 shadow-none">
                                <CardContent className="p-6">
                                    <div className="flex items-start justify-between">
                                        <div className="flex items-start gap-4">
                                            <div className={`p-3 rounded-xl ${
                                                alert.active 
                                                    ? 'bg-indigo-50' 
                                                    : 'bg-slate-100'
                                            }`}>
                                                <Bell className={`h-5 w-5 ${
                                                    alert.active 
                                                        ? 'text-civant-teal' 
                                                        : 'text-slate-400'
                                                }`} />
                                            </div>
                                            <div>
                                                <div className="flex items-center gap-2 mb-1">
                                                    <h3 className="font-semibold text-slate-100">
                                                        {alert.alert_name}
                                                    </h3>
                                                    {alert.active ? (
                                                        <Badge className="bg-emerald-50 text-emerald-700 border-emerald-200">
                                                            Active
                                                        </Badge>
                                                    ) : (
                                                        <Badge variant="outline" className="text-slate-400">
                                                            Paused
                                                        </Badge>
                                                    )}
                                                </div>
                                                
                                                {/* Filters */}
                                                <div className="flex flex-wrap gap-2 mt-2">
                                                    {alert.country && (
                                                        <Badge variant="outline" className="text-xs">
                                                            {alert.country === 'FR' ? '🇫🇷 France' : '🇮🇪 Ireland'}
                                                        </Badge>
                                                    )}
                                                    {alert.keywords && (
                                                        <Badge variant="outline" className="text-xs">
                                                            <Search className="h-3 w-3 mr-1" />
                                                            {alert.keywords}
                                                        </Badge>
                                                    )}
                                                    {alert.buyer_contains && (
                                                        <Badge variant="outline" className="text-xs">
                                                            Buyer: {alert.buyer_contains}
                                                        </Badge>
                                                    )}
                                                    {alert.cpv_contains && (
                                                        <Badge variant="outline" className="text-xs">
                                                            CPV: {alert.cpv_contains}
                                                        </Badge>
                                                    )}
                                                    {alert.deadline_within_days && (
                                                       <Badge variant="outline" className="text-xs">
                                                           Deadline: {alert.deadline_within_days} days
                                                       </Badge>
                                                    )}
                                                    {alert.notification_frequency && (
                                                       <Badge variant="outline" className="text-xs">
                                                           <Clock className="h-3 w-3 mr-1" />
                                                           {alert.notification_frequency === 'immediate' ? 'Immediate' : 'Daily Digest'}
                                                       </Badge>
                                                    )}
                                                    {alert.expiry_date && (
                                                       <Badge variant="outline" className="text-xs">
                                                           <CalendarX className="h-3 w-3 mr-1" />
                                                           Expires: {format(new Date(alert.expiry_date), 'MMM d, yyyy')}
                                                       </Badge>
                                                    )}
                                                    </div>

                                                    {/* Stats */}
                                                    <div className="flex items-center gap-4 mt-3 text-sm text-slate-400">
                                                    <span>{matchCount} matches</span>
                                                    {alert.last_checked_at && (
                                                       <span>
                                                           Last checked {formatDistanceToNow(new Date(alert.last_checked_at), { addSuffix: true })}
                                                       </span>
                                                    )}
                                                    </div>
                                            </div>
                                        </div>
                                        
                                        {/* Actions */}
                                        <div className="flex items-center gap-2">
                                            <Button 
                                                variant="ghost" 
                                                size="icon"
                                                onClick={() => toggleActive(alert)}
                                            >
                                                {alert.active ? (
                                                    <Pause className="h-4 w-4 text-slate-400" />
                                                ) : (
                                                    <Play className="h-4 w-4 text-slate-400" />
                                                )}
                                            </Button>
                                            <Button 
                                                variant="ghost" 
                                                size="icon"
                                                onClick={() => handleEdit(alert)}
                                            >
                                                <Edit2 className="h-4 w-4 text-slate-400" />
                                            </Button>
                                            <Button 
                                                variant="ghost" 
                                                size="icon"
                                                onClick={() => handleDelete(alert.id)}
                                            >
                                                <Trash2 className="h-4 w-4 text-red-500" />
                                            </Button>
                                        </div>
                                    </div>
                                </CardContent>
                            </Card>
                        );
                    })
                )}
            </div>
            
            {/* Recent Matches */}
            {alertEvents.length > 0 && (
                <Card className="border border-civant-border bg-civant-navy/55 shadow-none">
                    <CardHeader>
                        <CardTitle className="text-lg font-semibold">Recent Matches</CardTitle>
                    </CardHeader>
                    <CardContent className="p-0">
                        <div className="divide-y divide-slate-800">
                            {alertEvents.slice(0, 10).map(event => {
                                const alert = alerts.find(a => a.id === event.alert_id);
                                return (
                                    <div key={event.id} className="p-4 flex items-center justify-between">
                                        <div>
                                            <p className="font-medium text-slate-100">
                                                {alert?.alert_name || 'Alert'}
                                            </p>
                                            <p className="text-sm text-slate-400">
                                                Matched tender: {event.tender_uid}
                                            </p>
                                        </div>
                                        <div className="flex items-center gap-3">
                                            <span className="text-sm text-slate-400">
                                                {event.matched_at && formatDistanceToNow(new Date(event.matched_at), { addSuffix: true })}
                                            </span>
                                            {event.sent ? (
                                                <Badge className="bg-emerald-50 text-emerald-700">
                                                    <Mail className="h-3 w-3 mr-1" />
                                                    Sent
                                                </Badge>
                                            ) : (
                                                <Badge variant="outline">Pending</Badge>
                                            )}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </CardContent>
                </Card>
            )}
            
            {/* Alert Form Dialog */}
            <Dialog open={showForm} onOpenChange={setShowForm}>
                <DialogContent className="sm:max-w-lg">
                    <DialogHeader>
                        <DialogTitle>
                            {editingAlert ? 'Edit Alert' : 'Create New Alert'}
                        </DialogTitle>
                        <DialogDescription>
                            Define criteria to get notified about matching tenders
                        </DialogDescription>
                    </DialogHeader>
                    
                    <form onSubmit={handleSubmit} className="space-y-5">
                        <div>
                            <Label htmlFor="alert_name">Alert Name *</Label>
                            <Input
                                id="alert_name"
                                value={formData.alert_name}
                                onChange={(e) => setFormData({ ...formData, alert_name: e.target.value })}
                                placeholder="e.g. IT Services Tenders"
                                required
                            />
                        </div>
                        
                        {/* Filters Section */}
                        <div className="space-y-4 pt-2 border-t">
                            <h4 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
                                <Filter className="h-4 w-4" />
                                Filter Criteria
                            </h4>
                            
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <Label htmlFor="country">Country</Label>
                                    <Select 
                                        value={formData.country || 'all'} 
                                        onValueChange={(value) => setFormData({ ...formData, country: value === 'all' ? '' : value })}
                                    >
                                        <SelectTrigger>
                                            <SelectValue placeholder="All countries" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">All Countries</SelectItem>
                                            <SelectItem value="FR">🇫🇷 France</SelectItem>
                                            <SelectItem value="IE">🇮🇪 Ireland</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                                
                                <div>
                                    <Label htmlFor="deadline_within_days">Deadline Within (days)</Label>
                                    <Input
                                        id="deadline_within_days"
                                        type="number"
                                        value={formData.deadline_within_days}
                                        onChange={(e) => setFormData({ ...formData, deadline_within_days: e.target.value })}
                                        placeholder="e.g. 30"
                                    />
                                </div>
                            </div>
                            
                            <div>
                                <Label htmlFor="keywords">Keywords (comma-separated)</Label>
                                <Input
                                    id="keywords"
                                    value={formData.keywords}
                                    onChange={(e) => setFormData({ ...formData, keywords: e.target.value })}
                                    placeholder="e.g. software, cloud, IT"
                                />
                                <p className="text-xs text-slate-400 mt-1">Match any of these keywords in title or description</p>
                            </div>
                            
                            <div>
                                <Label htmlFor="buyer_contains">Buyer Name Contains</Label>
                                <Input
                                    id="buyer_contains"
                                    value={formData.buyer_contains}
                                    onChange={(e) => setFormData({ ...formData, buyer_contains: e.target.value })}
                                    placeholder="e.g. Ministry, Council, Health"
                                />
                            </div>
                            
                            <div>
                                <Label htmlFor="cpv_contains">CPV Code Contains</Label>
                                <Input
                                    id="cpv_contains"
                                    value={formData.cpv_contains}
                                    onChange={(e) => setFormData({ ...formData, cpv_contains: e.target.value })}
                                    placeholder="e.g. 72000000, 45"
                                />
                                <p className="text-xs text-slate-400 mt-1">Partial CPV code matching</p>
                            </div>
                        </div>
                        
                        {/* Notification Settings Section */}
                        <div className="space-y-4 pt-2 border-t">
                            <h4 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
                                <Bell className="h-4 w-4" />
                                Notification Settings
                            </h4>
                            
                            <div>
                                <Label htmlFor="notification_frequency">Notification Frequency</Label>
                                <Select 
                                    value={formData.notification_frequency} 
                                    onValueChange={(value) => setFormData({ ...formData, notification_frequency: value })}
                                >
                                    <SelectTrigger>
                                        <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="immediate">
                                            <div className="flex flex-col">
                                                <span>Immediate</span>
                                                <span className="text-xs text-slate-400">Notify as soon as a match is found</span>
                                            </div>
                                        </SelectItem>
                                        <SelectItem value="daily_digest">
                                            <div className="flex flex-col">
                                                <span>Daily Digest</span>
                                                <span className="text-xs text-slate-400">Receive a summary once per day</span>
                                            </div>
                                        </SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>
                            
                            <div>
                                <Label htmlFor="expiry_date">Expiry Date (optional)</Label>
                                <Input
                                    id="expiry_date"
                                    type="date"
                                    value={formData.expiry_date}
                                    onChange={(e) => setFormData({ ...formData, expiry_date: e.target.value })}
                                    min={new Date().toISOString().split('T')[0]}
                                />
                                <p className="text-xs text-slate-400 mt-1">Alert will automatically deactivate after this date</p>
                            </div>
                        </div>
                        
                        <div className="flex items-center justify-between pt-2 border-t">
                            <div className="flex flex-col">
                                <Label htmlFor="active">Alert Active</Label>
                                <p className="text-xs text-slate-400">Enable or disable this alert</p>
                            </div>
                            <Switch
                                id="active"
                                checked={formData.active}
                                onCheckedChange={(checked) => setFormData({ ...formData, active: checked })}
                            />
                        </div>

                        {saveError && (
                            <p className="text-sm text-red-400">{saveError}</p>
                        )}
                        
                        <DialogFooter>
                            <Button 
                                type="button" 
                                variant="outline" 
                                onClick={() => {
                                    setShowForm(false);
                                    setEditingAlert(null);
                                    resetForm();
                                }}
                            >
                                Cancel
                            </Button>
                            <Button 
                                type="submit" 
                                className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90"
                                disabled={saving}
                            >
                                {saving ? (
                                    <>
                                        <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                        Saving...
                                    </>
                                ) : (
                                    editingAlert ? 'Update Alert' : 'Create Alert'
                                )}
                            </Button>
                        </DialogFooter>
                    </form>
                </DialogContent>
            </Dialog>
        </div>
    );
}
