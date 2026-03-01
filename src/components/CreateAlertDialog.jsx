import React, { useState, useEffect } from 'react';
import { civant } from '@/api/civantClient';
import { useAuth } from '@/lib/auth';
import { Bell, Filter, Loader2 } from 'lucide-react';
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';

const EMPTY_FORM = {
    alert_name: '',
    country: '',
    keywords: '',
    buyer_contains: '',
    cpv_contains: '',
    deadline_within_days: '',
    notification_frequency: 'immediate',
    expiry_date: '',
    active: true
};

export default function CreateAlertDialog({ open, onOpenChange, prefill, onCreated }) {
    const { currentUser } = useAuth();
    const [formData, setFormData] = useState(EMPTY_FORM);
    const [saving, setSaving] = useState(false);
    const [saveError, setSaveError] = useState('');

    useEffect(() => {
        if (open && prefill) {
            setFormData({
                ...EMPTY_FORM,
                alert_name: prefill.alert_name || '',
                buyer_contains: prefill.buyer_contains || '',
                keywords: prefill.keywords || '',
                cpv_contains: prefill.cpv_contains || '',
                country: prefill.country || '',
            });
        }
        if (!open) {
            setSaveError('');
        }
    }, [open, prefill]);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setSaving(true);
        setSaveError('');

        try {
            const alertData = {
                ...formData,
                user_email: currentUser?.email,
                deadline_within_days: formData.deadline_within_days ? parseInt(formData.deadline_within_days) : null,
                country: formData.country || null
            };

            await civant.entities.Alerts.create(alertData);
            setFormData(EMPTY_FORM);
            onOpenChange(false);
            if (onCreated) onCreated();
        } catch (error) {
            console.error('Error saving alert:', error);
            setSaveError(error instanceof Error ? error.message : 'Unable to save alert. Please try again.');
        } finally {
            setSaving(false);
        }
    };

    return (
        <Dialog open={open} onOpenChange={onOpenChange}>
            <DialogContent className="w-[calc(100vw-2rem)] max-w-lg max-h-[90vh] overflow-y-auto">
                <DialogHeader>
                    <DialogTitle>Create New Alert</DialogTitle>
                    <DialogDescription>
                        Define criteria to get notified about matching tenders
                    </DialogDescription>
                </DialogHeader>

                <form onSubmit={handleSubmit} className="space-y-5">
                    <div>
                        <Label htmlFor="ca_alert_name">Alert Name *</Label>
                        <Input
                            id="ca_alert_name"
                            value={formData.alert_name}
                            onChange={(e) => setFormData({ ...formData, alert_name: e.target.value })}
                            placeholder="e.g. IT Services Tenders"
                            required
                        />
                    </div>

                    <div className="space-y-4 pt-2 border-t border-white/[0.06]">
                        <h4 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
                            <Filter className="h-4 w-4" />
                            Filter Criteria
                        </h4>

                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div>
                                <Label htmlFor="ca_country">Country</Label>
                                <Select
                                    value={formData.country || 'all'}
                                    onValueChange={(value) => setFormData({ ...formData, country: value === 'all' ? '' : value })}
                                >
                                    <SelectTrigger>
                                        <SelectValue placeholder="All countries" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="all">All Countries</SelectItem>
                                        <SelectItem value="FR">France</SelectItem>
                                        <SelectItem value="IE">Ireland</SelectItem>
                                        <SelectItem value="ES">Spain</SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>

                            <div>
                                <Label htmlFor="ca_deadline">Deadline Within (days)</Label>
                                <Input
                                    id="ca_deadline"
                                    type="number"
                                    value={formData.deadline_within_days}
                                    onChange={(e) => setFormData({ ...formData, deadline_within_days: e.target.value })}
                                    placeholder="e.g. 30"
                                />
                            </div>
                        </div>

                        <div>
                            <Label htmlFor="ca_keywords">Keywords (comma-separated)</Label>
                            <Input
                                id="ca_keywords"
                                value={formData.keywords}
                                onChange={(e) => setFormData({ ...formData, keywords: e.target.value })}
                                placeholder="e.g. software, cloud, IT"
                            />
                            <p className="text-xs text-slate-400 mt-1">Match any of these keywords in title or description</p>
                        </div>

                        <div>
                            <Label htmlFor="ca_buyer">Buyer Name Contains</Label>
                            <Input
                                id="ca_buyer"
                                value={formData.buyer_contains}
                                onChange={(e) => setFormData({ ...formData, buyer_contains: e.target.value })}
                                placeholder="e.g. Ministry, Council, Health"
                            />
                        </div>

                        <div>
                            <Label htmlFor="ca_cpv">CPV Code Contains</Label>
                            <Input
                                id="ca_cpv"
                                value={formData.cpv_contains}
                                onChange={(e) => setFormData({ ...formData, cpv_contains: e.target.value })}
                                placeholder="e.g. 72000000, 45"
                            />
                            <p className="text-xs text-slate-400 mt-1">Partial CPV code matching</p>
                        </div>
                    </div>

                    <div className="space-y-4 pt-2 border-t border-white/[0.06]">
                        <h4 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
                            <Bell className="h-4 w-4" />
                            Notification Settings
                        </h4>

                        <div>
                            <Label htmlFor="ca_frequency">Notification Frequency</Label>
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
                            <Label htmlFor="ca_expiry">Expiry Date (optional)</Label>
                            <Input
                                id="ca_expiry"
                                type="date"
                                value={formData.expiry_date}
                                onChange={(e) => setFormData({ ...formData, expiry_date: e.target.value })}
                                min={new Date().toISOString().split('T')[0]}
                            />
                            <p className="text-xs text-slate-400 mt-1">Alert will automatically deactivate after this date</p>
                        </div>
                    </div>

                    <div className="flex items-center justify-between pt-2 border-t border-white/[0.06]">
                        <div className="flex flex-col">
                            <Label htmlFor="ca_active">Alert Active</Label>
                            <p className="text-xs text-slate-400">Enable or disable this alert</p>
                        </div>
                        <Switch
                            id="ca_active"
                            checked={formData.active}
                            onCheckedChange={(checked) => setFormData({ ...formData, active: checked })}
                        />
                    </div>

                    {saveError && (
                        <p className="text-sm text-red-400">{saveError}</p>
                    )}

                    <DialogFooter className="flex-col sm:flex-row gap-2">
                        <Button
                            type="button"
                            variant="outline"
                            onClick={() => onOpenChange(false)}
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
                                'Create Alert'
                            )}
                        </Button>
                    </DialogFooter>
                </form>
            </DialogContent>
        </Dialog>
    );
}
