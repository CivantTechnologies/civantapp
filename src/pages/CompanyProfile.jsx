import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { civant } from '@/api/civantClient';
import { useTenant } from '@/lib/tenant';
import { useAuth } from '@/lib/auth';
import { useLocation } from 'react-router-dom';
import {
    Building2, CreditCard, Loader2, Save, ChevronRight, ChevronLeft,
    Check, Tag, Target
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { Textarea } from '@/components/ui/textarea';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useOnboarding } from "@/lib/OnboardingGate";
import SupplierAutocomplete from '@/components/SupplierAutocomplete';

const BUYER_TYPES = [
    { value: 'education', label: 'Education', desc: 'Universities, schools, training bodies' },
    { value: 'health', label: 'Healthcare', desc: 'Hospitals, HSE, health agencies' },
    { value: 'local_authority', label: 'Local Government', desc: 'Councils, communes, municipalities' },
    { value: 'central_government', label: 'Central Government', desc: 'Ministries, departments, agencies' },
    { value: 'transport', label: 'Transport & Infrastructure', desc: 'Roads, rail, airports, ports' },
    { value: 'defence', label: 'Defence & Security', desc: 'Military, police, emergency services' },
    { value: 'utilities', label: 'Utilities & Energy', desc: 'Water, electricity, gas, telecoms' },
    { value: 'other', label: 'Other', desc: 'NGOs, international bodies, other public' },
];

const CPV_CLUSTERS = [
    { value: 'cluster_it_software', label: 'IT & Software' },
    { value: 'cluster_construction', label: 'Construction' },
    { value: 'cluster_consulting', label: 'Consulting & Professional Services' },
    { value: 'cluster_health_medical', label: 'Healthcare & Medical' },
    { value: 'cluster_education_training', label: 'Education & Training' },
    { value: 'cluster_transport', label: 'Transport & Logistics' },
    { value: 'cluster_food_catering', label: 'Food & Catering' },
    { value: 'cluster_energy_environment', label: 'Energy & Environment' },
    { value: 'cluster_facilities_maintenance', label: 'Facilities & Maintenance' },
    { value: 'cluster_communications_media', label: 'Communications & Media' },
    { value: 'cluster_financial_legal', label: 'Financial & Legal Services' },
    { value: 'cluster_manufacturing', label: 'Manufacturing & Industrial' },
    { value: 'cluster_defence_security', label: 'Defence & Security' },
    { value: 'cluster_research', label: 'Research & Development' },
    { value: 'cluster_other', label: 'Other' },
];

const COUNTRIES = [
    { value: 'IE', label: 'Ireland', flag: '🇮🇪' },
    { value: 'FR', label: 'France', flag: '🇫🇷' },
    { value: 'ES', label: 'Spain', flag: '🇪🇸' },
];

const SIZE_OPTIONS = [
    { value: 'micro', label: 'Micro (1-9 employees)' },
    { value: 'small', label: 'Small (10-49 employees)' },
    { value: 'medium', label: 'Medium (50-249 employees)' },
    { value: 'large', label: 'Large (250+ employees)' },
];

const VOLUME_OPTIONS = [
    { value: 'few', label: '1-5 bids per year' },
    { value: 'moderate', label: '6-20 bids per year' },
    { value: 'heavy', label: '20+ bids per year' },
];

const CONTRACT_SIZE_OPTIONS = [
    { value: 0, label: 'Any' },
    { value: 10000, label: '€10K' },
    { value: 50000, label: '€50K' },
    { value: 100000, label: '€100K' },
    { value: 250000, label: '€250K' },
    { value: 500000, label: '€500K' },
    { value: 1000000, label: '€1M' },
    { value: 5000000, label: '€5M' },
    { value: 10000000, label: '€10M+' },
];

function MultiChipSelect({ options, selected, onChange, renderOption = null }) {
    return (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {options.map((opt) => {
                const isSelected = selected.includes(opt.value);
                return (
                    <button
                        key={opt.value}
                        type="button"
                        onClick={() => {
                            if (isSelected) onChange(selected.filter(v => v !== opt.value));
                            else onChange([...selected, opt.value]);
                        }}
                        className={`text-left px-3 py-2.5 rounded-xl border transition-all duration-150 ${
                            isSelected
                                ? 'border-civant-teal/40 bg-civant-teal/10 text-slate-200'
                                : 'border-white/[0.06] bg-white/[0.02] text-slate-400 hover:border-white/[0.12] hover:text-slate-300'
                        }`}
                    >
                        <div className="flex items-center justify-between">
                            <div>
                                {renderOption ? renderOption(opt) : (
                                    <>
                                        <p className="text-sm font-medium">{opt.label}</p>
                                        {opt.desc && <p className="text-[11px] text-slate-500 mt-0.5">{opt.desc}</p>}
                                    </>
                                )}
                            </div>
                            {isSelected && <Check className="h-4 w-4 text-civant-teal shrink-0" />}
                        </div>
                    </button>
                );
            })}
        </div>
    );
}

function TagInput({ value, onChange, placeholder }) {
    const [input, setInput] = useState('');
    const add = () => {
        const trimmed = input.trim();
        if (trimmed && !value.includes(trimmed)) {
            onChange([...value, trimmed]);
        }
        setInput('');
    };
    return (
        <div className="space-y-2">
            <div className="flex gap-2">
                <Input
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); add(); } }}
                    placeholder={placeholder}
                    className="flex-1"
                />
                <Button type="button" variant="outline" size="sm" onClick={add} disabled={!input.trim()}>Add</Button>
            </div>
            {value.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                    {value.map((tag, i) => (
                        <Badge key={i} className="bg-civant-teal/10 text-civant-teal border border-civant-teal/30 text-xs cursor-pointer hover:bg-red-500/10 hover:text-red-400 hover:border-red-400/30 transition-colors"
                            onClick={() => onChange(value.filter((_, j) => j !== i))}
                        >
                            {tag} ×
                        </Badge>
                    ))}
                </div>
            )}
        </div>
    );
}

function CompetitorInput({ value, onChange }) {
    const [adding, setAdding] = useState(false);
    return (
        <div className="space-y-2">
            {value.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                    {value.map((name, i) => (
                        <Badge key={i} className="bg-white/[0.04] text-slate-300 border border-white/[0.08] text-xs cursor-pointer hover:bg-red-500/10 hover:text-red-400 hover:border-red-400/30 transition-colors"
                            onClick={() => onChange(value.filter((_, j) => j !== i))}
                        >
                            {name} ×
                        </Badge>
                    ))}
                </div>
            )}
            {adding ? (
                <SupplierAutocomplete
                    value=""
                    onChange={() => {}}
                    onSelect={(v) => {
                        if (v && !value.includes(v)) onChange([...value, v]);
                        setAdding(false);
                    }}
                    placeholder="Search supplier name..."
                />
            ) : (
                <Button type="button" variant="outline" size="sm" onClick={() => setAdding(true)}>
                    + Add competitor
                </Button>
            )}
        </div>
    );
}

// ===== ONBOARDING WIZARD =====

function OnboardingWizard({ profile, onSave, saving }) {
    const [step, setStep] = useState(profile.onboarding_step || 0);
    const [form, setForm] = useState(profile);
    const set = (key, val) => setForm(prev => ({ ...prev, [key]: val }));

    const steps = [
        { title: 'Your Company', icon: Building2, desc: 'Basic information about your organisation' },
        { title: 'Who You Sell To', icon: Target, desc: 'Your target buyers and markets' },
        { title: 'What You Offer', icon: Tag, desc: 'Products, services and procurement experience' },
    ];

    const canProceed = () => {
        if (step === 0) return form.company_name?.trim();
        if (step === 1) return form.target_buyer_types?.length > 0;
        return true;
    };

    const next = () => {
        if (step < 2) {
            const nextStep = step + 1;
            setStep(nextStep);
            const updated = { ...form, onboarding_step: nextStep };
            setForm(updated);
            onSave(updated);
        }
    };
    const prev = () => { if (step > 0) setStep(step - 1); };
    const finish = () => {
        onSave({ ...form, onboarding_completed: true, onboarding_step: 3 });
    };

    return (
        <div className="max-w-2xl mx-auto space-y-6">
            {/* Progress */}
            <div className="flex items-center justify-between px-2">
                {steps.map((s, i) => {
                    const Icon = s.icon;
                    const isActive = i === step;
                    const isDone = i < step;
                    return (
                        <React.Fragment key={i}>
                            <div className="flex items-center gap-2">
                                <div className={`w-8 h-8 rounded-full flex items-center justify-center transition-colors ${
                                    isActive ? 'bg-civant-teal/20 border border-civant-teal/40 text-civant-teal' :
                                    isDone ? 'bg-civant-teal/10 border border-civant-teal/30 text-civant-teal' :
                                    'bg-white/[0.03] border border-white/[0.08] text-slate-500'
                                }`}>
                                    {isDone ? <Check className="h-4 w-4" /> : <Icon className="h-4 w-4" />}
                                </div>
                                <span className={`text-sm hidden sm:inline ${isActive ? 'text-slate-200 font-medium' : 'text-slate-500'}`}>
                                    {s.title}
                                </span>
                            </div>
                            {i < steps.length - 1 && (
                                <div className={`flex-1 h-px mx-3 ${i < step ? 'bg-civant-teal/30' : 'bg-white/[0.06]'}`} />
                            )}
                        </React.Fragment>
                    );
                })}
            </div>

            {/* Step Content */}
            <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                <CardHeader>
                    <CardTitle className="text-lg">{steps[step].title}</CardTitle>
                    <p className="text-sm text-slate-500">{steps[step].desc}</p>
                </CardHeader>
                <CardContent className="space-y-4">
                    {step === 0 && (
                        <>
                            <div>
                                <Label>Company Name *</Label>
                                <Input value={form.company_name || ''} onChange={(e) => set('company_name', e.target.value)} placeholder="Your company name" />
                            </div>
                            <div>
                                <Label>What does your company do?</Label>
                                <Textarea value={form.company_description || ''} onChange={(e) => set('company_description', e.target.value)} placeholder="Brief description of your products or services..." rows={3} />
                            </div>
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                                <div>
                                    <Label>Company Size</Label>
                                    <Select value={form.company_size || ''} onValueChange={(v) => set('company_size', v)}>
                                        <SelectTrigger><SelectValue placeholder="Select size" /></SelectTrigger>
                                        <SelectContent>{SIZE_OPTIONS.map(o => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                                <div>
                                    <Label>Headquarters</Label>
                                    <Select value={form.country_hq || ''} onValueChange={(v) => set('country_hq', v)}>
                                        <SelectTrigger><SelectValue placeholder="Select country" /></SelectTrigger>
                                        <SelectContent>{COUNTRIES.map(c => <SelectItem key={c.value} value={c.value}>{c.flag} {c.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                            </div>
                            <div>
                                <Label>Website</Label>
                                <Input value={form.website || ''} onChange={(e) => set('website', e.target.value)} placeholder="https://yourcompany.com" />
                            </div>
                        </>
                    )}

                    {step === 1 && (
                        <>
                            <div>
                                <Label className="mb-2 block">Which buyer types do you typically sell to?</Label>
                                <MultiChipSelect
                                    options={BUYER_TYPES}
                                    selected={form.target_buyer_types || []}
                                    onChange={(v) => set('target_buyer_types', v)}
                                />
                            </div>
                            <div>
                                <Label className="mb-2 block">Which countries do you bid in?</Label>
                                <MultiChipSelect
                                    options={COUNTRIES.map(c => ({ value: c.value, label: `${c.flag} ${c.label}` }))}
                                    selected={form.target_countries || []}
                                    onChange={(v) => set('target_countries', v)}
                                />
                            </div>
                        </>
                    )}

                    {step === 2 && (
                        <>
                            <div>
                                <Label className="mb-2 block">Your key products or services</Label>
                                <TagInput
                                    value={form.key_products_services || []}
                                    onChange={(v) => set('key_products_services', v)}
                                    placeholder="e.g. lecture capture, video platform..."
                                />
                            </div>
                            <div>
                                <Label className="mb-2 block">Industry categories you operate in</Label>
                                <MultiChipSelect
                                    options={CPV_CLUSTERS}
                                    selected={form.target_cpv_clusters || []}
                                    onChange={(v) => set('target_cpv_clusters', v)}
                                />
                            </div>
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                                <div>
                                    <Label>Minimum contract value</Label>
                                    <Select value={String(form.contract_size_min_eur || 0)} onValueChange={(v) => set('contract_size_min_eur', Number(v))}>
                                        <SelectTrigger><SelectValue /></SelectTrigger>
                                        <SelectContent>{CONTRACT_SIZE_OPTIONS.map(o => <SelectItem key={o.value} value={String(o.value)}>{o.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                                <div>
                                    <Label>Maximum contract value</Label>
                                    <Select value={String(form.contract_size_max_eur || 0)} onValueChange={(v) => set('contract_size_max_eur', Number(v))}>
                                        <SelectTrigger><SelectValue /></SelectTrigger>
                                        <SelectContent>{CONTRACT_SIZE_OPTIONS.map(o => <SelectItem key={o.value} value={String(o.value)}>{o.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                            </div>
                            <div>
                                <Label>Annual bid volume</Label>
                                <Select value={form.annual_bid_volume || ''} onValueChange={(v) => set('annual_bid_volume', v)}>
                                    <SelectTrigger><SelectValue placeholder="How many bids per year?" /></SelectTrigger>
                                    <SelectContent>{VOLUME_OPTIONS.map(o => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
                                </Select>
                            </div>
                            <div>
                                <Label className="mb-2 block">Known competitors</Label>
                                <CompetitorInput
                                    value={form.known_competitors || []}
                                    onChange={(v) => set('known_competitors', v)}
                                />
                            </div>
                        </>
                    )}
                </CardContent>
            </Card>

            {/* Navigation */}
            <div className="flex items-center justify-between">
                <Button type="button" variant="ghost" onClick={prev} disabled={step === 0}>
                    <ChevronLeft className="h-4 w-4 mr-1" /> Back
                </Button>
                <div className="flex items-center gap-2">
                    <span className="text-xs text-slate-500">Step {step + 1} of {steps.length}</span>
                    {step < 2 ? (
                        <Button type="button" onClick={next} disabled={!canProceed()} className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90">
                            Next <ChevronRight className="h-4 w-4 ml-1" />
                        </Button>
                    ) : (
                        <Button type="button" onClick={finish} disabled={saving} className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90">
                            {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Check className="h-4 w-4 mr-2" />}
                            Complete Setup
                        </Button>
                    )}
                </div>
            </div>
        </div>
    );
}

// ===== MAIN PAGE (TABBED PROFILE) =====

function ProfileTabs({ profile, onSave, saving, isOrgAdmin, initialTab = 'company' }) {
    const [form, setForm] = useState(profile);
    const [activeTab, setActiveTab] = useState(initialTab);
    useEffect(() => {
        setForm(profile);
    }, [profile]);
    useEffect(() => {
        setActiveTab(initialTab);
    }, [initialTab]);
    const set = (key, val) => setForm(prev => ({ ...prev, [key]: val }));
    const isDirty = useMemo(
        () => JSON.stringify(form) !== JSON.stringify(profile),
        [form, profile]
    );
    const save = () => onSave(form);

    return (
        <div className="space-y-4">
            <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="space-y-1.5">
                    <h1 className="text-[clamp(1.7rem,2.2vw,2rem)] font-semibold tracking-tight text-slate-100">Company</h1>
                    <p className="text-sm text-slate-400">Company profile and targeting preferences.</p>
                    {form.company_name ? (
                        <p className="text-xs text-slate-500">{form.company_name}</p>
                    ) : null}
                </div>
                <div className="shrink-0 pt-0.5">
                    <Button
                        onClick={save}
                        disabled={saving || !isDirty}
                        className="bg-civant-teal text-slate-950 hover:bg-civant-teal/90 disabled:cursor-not-allowed disabled:opacity-50"
                    >
                        {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Save className="h-4 w-4 mr-2" />}
                        Save Changes
                    </Button>
                </div>
            </div>

            <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
                <TabsList className="bg-slate-900/70 border border-white/[0.06]">
                    <TabsTrigger value="company" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">
                        <Building2 className="h-3.5 w-3.5 mr-1.5" />Company Information
                    </TabsTrigger>
                    <TabsTrigger value="personalization" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">
                        <Target className="h-3.5 w-3.5 mr-1.5" />Civant Personalization
                    </TabsTrigger>
                    <TabsTrigger value="billing" className="data-[state=active]:bg-civant-teal/15 data-[state=active]:text-civant-teal">
                        <CreditCard className="h-3.5 w-3.5 mr-1.5" />Billing
                    </TabsTrigger>
                </TabsList>

                {/* ===== COMPANY INFO TAB ===== */}
                <TabsContent value="company" className="mt-3 space-y-4">
                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardContent className="p-6 space-y-4">
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                                <div className="sm:col-span-2">
                                    <Label>Company Name</Label>
                                    <Input value={form.company_name || ''} onChange={(e) => set('company_name', e.target.value)} />
                                </div>
                                <div className="sm:col-span-2">
                                    <Label>Description</Label>
                                    <Textarea value={form.company_description || ''} onChange={(e) => set('company_description', e.target.value)} rows={3} />
                                </div>
                                <div>
                                    <Label>Company Size</Label>
                                    <Select value={form.company_size || ''} onValueChange={(v) => set('company_size', v)}>
                                        <SelectTrigger><SelectValue placeholder="Select" /></SelectTrigger>
                                        <SelectContent>{SIZE_OPTIONS.map(o => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                                <div>
                                    <Label>Headquarters</Label>
                                    <Select value={form.country_hq || ''} onValueChange={(v) => set('country_hq', v)}>
                                        <SelectTrigger><SelectValue placeholder="Select" /></SelectTrigger>
                                        <SelectContent>{COUNTRIES.map(c => <SelectItem key={c.value} value={c.value}>{c.flag} {c.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                                <div>
                                    <Label>Website</Label>
                                    <Input value={form.website || ''} onChange={(e) => set('website', e.target.value)} />
                                </div>
                                <div>
                                    <Label>Year Established</Label>
                                    <Input type="number" value={form.year_established || ''} onChange={(e) => set('year_established', e.target.value ? Number(e.target.value) : null)} placeholder="e.g. 2015" />
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>

                {/* ===== PERSONALIZATION TAB ===== */}
                <TabsContent value="personalization" className="mt-3 space-y-4">
                    {isOrgAdmin ? (
                        <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                            <CardHeader><CardTitle className="text-base">Scope Behavior</CardTitle></CardHeader>
                            <CardContent className="flex flex-wrap items-start justify-between gap-4">
                                <div className="space-y-1">
                                    <p className="text-sm font-medium text-slate-200">Use Company Scope to filter Forecast and Search</p>
                                    {form.company_scope_filter_enabled !== false ? (
                                        <p className="text-xs text-slate-400">ON: Results are filtered to your Company scope.</p>
                                    ) : (
                                        <p className="text-xs text-slate-400">OFF: Results show the full market. Scope is used to prioritize and highlight.</p>
                                    )}
                                </div>
                                <Switch
                                    checked={form.company_scope_filter_enabled !== false}
                                    onCheckedChange={(checked) => set('company_scope_filter_enabled', checked)}
                                />
                            </CardContent>
                        </Card>
                    ) : null}

                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardHeader><CardTitle className="text-base">Target Buyers</CardTitle></CardHeader>
                        <CardContent>
                            <MultiChipSelect
                                options={BUYER_TYPES}
                                selected={form.target_buyer_types || []}
                                onChange={(v) => set('target_buyer_types', v)}
                            />
                        </CardContent>
                    </Card>

                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardHeader><CardTitle className="text-base">Target Countries</CardTitle></CardHeader>
                        <CardContent>
                            <MultiChipSelect
                                options={COUNTRIES.map(c => ({ value: c.value, label: `${c.flag} ${c.label}` }))}
                                selected={form.target_countries || []}
                                onChange={(v) => set('target_countries', v)}
                            />
                        </CardContent>
                    </Card>

                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardHeader><CardTitle className="text-base">Industry Categories</CardTitle></CardHeader>
                        <CardContent>
                            <MultiChipSelect
                                options={CPV_CLUSTERS}
                                selected={form.target_cpv_clusters || []}
                                onChange={(v) => set('target_cpv_clusters', v)}
                            />
                        </CardContent>
                    </Card>

                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardHeader><CardTitle className="text-base">Products & Services</CardTitle></CardHeader>
                        <CardContent>
                            <TagInput
                                value={form.key_products_services || []}
                                onChange={(v) => set('key_products_services', v)}
                                placeholder="e.g. lecture capture, consulting..."
                            />
                        </CardContent>
                    </Card>

                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardHeader><CardTitle className="text-base">Contract Preferences</CardTitle></CardHeader>
                        <CardContent className="space-y-4">
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                                <div>
                                    <Label>Min contract value</Label>
                                    <Select value={String(form.contract_size_min_eur || 0)} onValueChange={(v) => set('contract_size_min_eur', Number(v))}>
                                        <SelectTrigger><SelectValue /></SelectTrigger>
                                        <SelectContent>{CONTRACT_SIZE_OPTIONS.map(o => <SelectItem key={o.value} value={String(o.value)}>{o.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                                <div>
                                    <Label>Max contract value</Label>
                                    <Select value={String(form.contract_size_max_eur || 0)} onValueChange={(v) => set('contract_size_max_eur', Number(v))}>
                                        <SelectTrigger><SelectValue /></SelectTrigger>
                                        <SelectContent>{CONTRACT_SIZE_OPTIONS.map(o => <SelectItem key={o.value} value={String(o.value)}>{o.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                                <div>
                                    <Label>Annual bid volume</Label>
                                    <Select value={form.annual_bid_volume || ''} onValueChange={(v) => set('annual_bid_volume', v)}>
                                        <SelectTrigger><SelectValue placeholder="Select" /></SelectTrigger>
                                        <SelectContent>{VOLUME_OPTIONS.map(o => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                                <div>
                                    <Label>Frameworks</Label>
                                    <Select value={form.does_frameworks ? 'yes' : 'no'} onValueChange={(v) => set('does_frameworks', v === 'yes')}>
                                        <SelectTrigger><SelectValue /></SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="yes">Yes, we bid on frameworks</SelectItem>
                                            <SelectItem value="no">No</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                            </div>
                        </CardContent>
                    </Card>

                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardHeader><CardTitle className="text-base">Known Competitors</CardTitle></CardHeader>
                        <CardContent>
                            <CompetitorInput
                                value={form.known_competitors || []}
                                onChange={(v) => set('known_competitors', v)}
                            />
                        </CardContent>
                    </Card>
                </TabsContent>

                {/* ===== BILLING TAB ===== */}
                <TabsContent value="billing" className="mt-3 space-y-4">
                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardHeader><CardTitle className="text-base">Current Plan</CardTitle></CardHeader>
                        <CardContent>
                            <div className="flex items-center gap-3">
                                <Badge className="bg-civant-teal/15 text-civant-teal border border-civant-teal/40 text-sm px-3 py-1">
                                    {(form.plan_type || 'free').charAt(0).toUpperCase() + (form.plan_type || 'free').slice(1)}
                                </Badge>
                                <span className="text-sm text-slate-500">Upgrade options coming soon</span>
                            </div>
                        </CardContent>
                    </Card>

                    <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                        <CardHeader><CardTitle className="text-base">Billing Details</CardTitle></CardHeader>
                        <CardContent className="space-y-4">
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                                <div className="sm:col-span-2">
                                    <Label>Billing Company Name</Label>
                                    <Input value={form.billing_company_name || ''} onChange={(e) => set('billing_company_name', e.target.value)} placeholder="Legal company name for invoices" />
                                </div>
                                <div>
                                    <Label>VAT Number</Label>
                                    <Input value={form.billing_vat_number || ''} onChange={(e) => set('billing_vat_number', e.target.value)} placeholder="e.g. IE1234567T" />
                                </div>
                                <div>
                                    <Label>Billing Email</Label>
                                    <Input type="email" value={form.billing_email || ''} onChange={(e) => set('billing_email', e.target.value)} placeholder="accounts@company.com" />
                                </div>
                                <div className="sm:col-span-2">
                                    <Label>Address Line 1</Label>
                                    <Input value={form.billing_address_line1 || ''} onChange={(e) => set('billing_address_line1', e.target.value)} />
                                </div>
                                <div className="sm:col-span-2">
                                    <Label>Address Line 2</Label>
                                    <Input value={form.billing_address_line2 || ''} onChange={(e) => set('billing_address_line2', e.target.value)} />
                                </div>
                                <div>
                                    <Label>City</Label>
                                    <Input value={form.billing_city || ''} onChange={(e) => set('billing_city', e.target.value)} />
                                </div>
                                <div>
                                    <Label>Postcode</Label>
                                    <Input value={form.billing_postcode || ''} onChange={(e) => set('billing_postcode', e.target.value)} />
                                </div>
                                <div>
                                    <Label>Country</Label>
                                    <Select value={form.billing_country || ''} onValueChange={(v) => set('billing_country', v)}>
                                        <SelectTrigger><SelectValue placeholder="Select" /></SelectTrigger>
                                        <SelectContent>{COUNTRIES.map(c => <SelectItem key={c.value} value={c.value}>{c.flag} {c.label}</SelectItem>)}</SelectContent>
                                    </Select>
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>
            </Tabs>
        </div>
    );
}

// ===== PAGE EXPORT =====

export default function CompanyProfile() {
    const { activeTenantId, isLoadingTenants, refreshCompanyProfile } = useTenant();
    const { roles } = useAuth();
    const location = useLocation();
    const { refreshOnboarding } = useOnboarding();
    const [profile, setProfile] = useState(null);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [saveMsg, setSaveMsg] = useState('');
    const isOrgAdmin = Array.isArray(roles) && (roles.includes('admin') || roles.includes('creator'));
    const initialTab = useMemo(() => {
        const tab = new URLSearchParams(location.search).get('tab');
        if (tab === 'personalization' || tab === 'billing' || tab === 'company') return tab;
        return 'company';
    }, [location.search]);

    const loadProfile = useCallback(async () => {
        if (!activeTenantId) return;
        setLoading(true);
        try {
            const rows = await civant.entities.company_profiles.filter(
                { tenant_id: activeTenantId },
                '-updated_at',
                1
            );
            const data = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
            const hydrated = data
                ? { ...data, company_scope_filter_enabled: data.company_scope_filter_enabled !== false }
                : { tenant_id: activeTenantId, company_name: '', onboarding_completed: false, company_scope_filter_enabled: true };
            setProfile(hydrated);
        } catch (e) {
            console.error('Failed to load company profile:', e);
            setProfile({ tenant_id: activeTenantId, company_name: '', onboarding_completed: false, company_scope_filter_enabled: true });
        } finally {
            setLoading(false);
        }
    }, [activeTenantId]);

    useEffect(() => {
        if (!isLoadingTenants && activeTenantId) loadProfile();
    }, [activeTenantId, isLoadingTenants, loadProfile]);

    const saveProfile = async (form) => {
        setSaving(true);
        setSaveMsg('');
        try {
            const payload = { ...form, tenant_id: activeTenantId, updated_at: new Date().toISOString() };
            const saved = await civant.entities.company_profiles.create(payload);
            setProfile(saved && typeof saved === 'object' ? saved : payload);
            if (payload.onboarding_completed) refreshOnboarding();
            refreshCompanyProfile(activeTenantId);
            setSaveMsg('Saved successfully');
            setTimeout(() => setSaveMsg(''), 3000);
        } catch (e) {
            console.error('Save failed:', e);
            setSaveMsg('Failed to save: ' + e.message);
        } finally {
            setSaving(false);
        }
    };

    if (loading || isLoadingTenants) {
        return <div className="flex items-center justify-center h-64"><Loader2 className="h-8 w-8 animate-spin text-civant-teal" /></div>;
    }

    if (!activeTenantId) {
        return (
            <div className="flex items-center justify-center h-64">
                <p className="text-slate-400">Select a workspace to manage your company profile.</p>
            </div>
        );
    }

    return (
        <div className="mx-auto w-full max-w-[1040px] space-y-6">
            {!profile?.onboarding_completed ? (
                <>
                    <div className="civant-hero mx-auto flex min-h-[60vh] max-w-4xl flex-col justify-center gap-5 text-center">
                        <h1 className="text-4xl font-semibold tracking-tight text-slate-100 md:text-5xl">Set your company baseline.</h1>
                        <p className="mx-auto max-w-3xl text-base text-slate-400 md:text-lg">
                            Complete your profile once so forecasting, targeting, and alerting align to your real bidding strategy.
                        </p>
                    </div>
                    <OnboardingWizard profile={profile} onSave={saveProfile} saving={saving} />
                </>
            ) : (
                <ProfileTabs
                    profile={profile}
                    onSave={saveProfile}
                    saving={saving}
                    isOrgAdmin={isOrgAdmin}
                    initialTab={initialTab}
                />
            )}
            {saveMsg && (
                <div className={`text-center text-sm ${saveMsg.includes('Failed') ? 'text-red-400' : 'text-emerald-400'}`}>
                    {saveMsg}
                </div>
            )}
        </div>
    );
}
