import React, { useEffect, useMemo, useState } from 'react';
import { UserRound, Save, Loader2, Upload } from 'lucide-react';
import { civant } from '@/api/civantClient';
import { useAuth } from '@/lib/auth';
import { Page, PageHeader, PageTitle, PageDescription, PageBody, Card, CardHeader, CardTitle, CardContent, Button, Input, Badge } from '@/components/ui';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Checkbox } from '@/components/ui/checkbox';

const tenderTypeOptions = [
  'IT & Software',
  'Construction & Infrastructure',
  'Healthcare & Medical',
  'Transport & Logistics',
  'Energy & Utilities',
  'Consulting & Professional Services',
  'Education & Training',
  'Facilities & Maintenance'
];

const noticeTypeOptions = ['Tender', 'Award', 'Corrigendum', 'PIN', 'Contract notice'];
const contractTypeOptions = ['Supplies', 'Services', 'Works', 'Framework', 'Concession'];
const regionOptions = ['Ireland', 'France', 'EU-wide', 'Nordics', 'DACH', 'Benelux', 'UK'];

function ensureArray(value) {
  if (Array.isArray(value)) return value.filter(Boolean).map((v) => String(v));
  if (!value) return [];
  return String(value)
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

const defaultForm = {
  first_name: '',
  last_name: '',
  birth_date: '',
  email: '',
  phone_number: '',
  country: '',
  company: '',
  industry: '',
  job_title: '',
  role_focus: '',
  tender_interest_types: [],
  procurement_regions: [],
  cpv_interest_codes: '',
  preferred_notice_types: [],
  preferred_contract_types: [],
  budget_range: '',
  notification_frequency: 'daily',
  language: 'en',
  timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'Europe/Dublin',
  avatar_url: '',
  linkedin_url: '',
  website_url: '',
  bio: ''
};

export default function Profile() {
  const { currentUser } = useAuth();
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState('');
  const [saveNotice, setSaveNotice] = useState('');
  const [profileId, setProfileId] = useState('');
  const [userId, setUserId] = useState('');
  const [form, setForm] = useState(defaultForm);

  const displayName = useMemo(() => [form.first_name, form.last_name].filter(Boolean).join(' ').trim(), [form.first_name, form.last_name]);

  useEffect(() => {
    let mounted = true;

    const load = async () => {
      setLoading(true);
      setSaveError('');

      try {
        const me = await civant.auth.me();
        const effectiveUserId = String(me?.userId || me?.user_id || '').trim();
        const effectiveEmail = String(currentUser?.email || me?.email || '').trim().toLowerCase();
        if (!mounted) return;

        setUserId(effectiveUserId);

        const query = effectiveUserId ? { user_id: effectiveUserId } : { email: effectiveEmail };
        const rows = await civant.entities.UserProfile.filter(query, '-created_at', 1);
        const row = Array.isArray(rows) && rows.length ? rows[0] : null;

        if (!mounted) return;

        if (!row) {
          setForm((prev) => ({
            ...prev,
            email: effectiveEmail
          }));
          setProfileId('');
        } else {
          setProfileId(String(row.id || ''));
          setForm({
            first_name: row.first_name || '',
            last_name: row.last_name || '',
            birth_date: row.birth_date || '',
            email: row.email || effectiveEmail,
            phone_number: row.phone_number || '',
            country: row.country || '',
            company: row.company || '',
            industry: row.industry || '',
            job_title: row.job_title || '',
            role_focus: row.role_focus || '',
            tender_interest_types: ensureArray(row.tender_interest_types),
            procurement_regions: ensureArray(row.procurement_regions),
            cpv_interest_codes: ensureArray(row.cpv_interest_codes).join(', '),
            preferred_notice_types: ensureArray(row.preferred_notice_types),
            preferred_contract_types: ensureArray(row.preferred_contract_types),
            budget_range: row.budget_range || '',
            notification_frequency: row.notification_frequency || 'daily',
            language: row.language || 'en',
            timezone: row.timezone || defaultForm.timezone,
            avatar_url: row.avatar_url || '',
            linkedin_url: row.linkedin_url || '',
            website_url: row.website_url || '',
            bio: row.bio || ''
          });
        }
      } catch (error) {
        if (!mounted) return;
        setSaveError(error?.message || 'Failed to load your profile.');
      } finally {
        if (mounted) setLoading(false);
      }
    };

    load();
    return () => {
      mounted = false;
    };
  }, [currentUser?.email]);

  const setField = (key, value) => setForm((prev) => ({ ...prev, [key]: value }));

  const toggleOption = (key, value) => {
    setForm((prev) => {
      const list = ensureArray(prev[key]);
      const next = list.includes(value) ? list.filter((item) => item !== value) : [...list, value];
      return { ...prev, [key]: next };
    });
  };

  const onSave = async (event) => {
    event.preventDefault();
    setSaving(true);
    setSaveError('');
    setSaveNotice('');

    try {
      const payload = {
        user_id: userId,
        email: String(form.email || currentUser?.email || '').trim().toLowerCase(),
        first_name: form.first_name || null,
        last_name: form.last_name || null,
        birth_date: form.birth_date || null,
        phone_number: form.phone_number || null,
        country: form.country || null,
        company: form.company || null,
        industry: form.industry || null,
        job_title: form.job_title || null,
        role_focus: form.role_focus || null,
        tender_interest_types: ensureArray(form.tender_interest_types),
        procurement_regions: ensureArray(form.procurement_regions),
        cpv_interest_codes: ensureArray(form.cpv_interest_codes),
        preferred_notice_types: ensureArray(form.preferred_notice_types),
        preferred_contract_types: ensureArray(form.preferred_contract_types),
        budget_range: form.budget_range || null,
        notification_frequency: form.notification_frequency || null,
        language: form.language || null,
        timezone: form.timezone || null,
        avatar_url: form.avatar_url || null,
        linkedin_url: form.linkedin_url || null,
        website_url: form.website_url || null,
        bio: form.bio || null,
        updated_at: new Date().toISOString()
      };

      if (profileId) {
        await civant.entities.UserProfile.update(profileId, payload);
      } else {
        const created = await civant.entities.UserProfile.create(payload);
        setProfileId(String(created?.id || ''));
      }
      setSaveNotice('Profile saved.');
    } catch (error) {
      setSaveError(error?.message || 'Failed to save profile.');
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-7 w-7 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <Page className="space-y-6">
      <PageHeader>
        <PageTitle className="flex items-center gap-2">
          <UserRound className="h-6 w-6 text-primary" />
          Profile
        </PageTitle>
        <PageDescription>Complete your information and tender preferences to personalize Civant Intelligence.</PageDescription>
      </PageHeader>

      <PageBody>
        <form onSubmit={onSave} className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Identity</CardTitle>
            </CardHeader>
            <CardContent className="space-y-5">
              <div className="flex items-center gap-4">
                <div className="h-16 w-16 rounded-full border border-border bg-muted/50 overflow-hidden flex items-center justify-center">
                  {form.avatar_url ? (
                    <img src={form.avatar_url} alt="Profile avatar" className="h-full w-full object-cover" />
                  ) : (
                    <UserRound className="h-8 w-8 text-muted-foreground" />
                  )}
                </div>
                <div className="flex-1">
                  <Label htmlFor="avatar_url">Profile Picture URL</Label>
                  <div className="mt-1 flex gap-2">
                    <Input
                      id="avatar_url"
                      value={form.avatar_url}
                      onChange={(event) => setField('avatar_url', event.target.value)}
                      placeholder="https://..."
                    />
                    <Button type="button" variant="secondary">
                      <Upload className="h-4 w-4 mr-1" />
                      Link
                    </Button>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="first_name">First Name</Label>
                  <Input id="first_name" value={form.first_name} onChange={(e) => setField('first_name', e.target.value)} />
                </div>
                <div>
                  <Label htmlFor="last_name">Last Name</Label>
                  <Input id="last_name" value={form.last_name} onChange={(e) => setField('last_name', e.target.value)} />
                </div>
                <div>
                  <Label htmlFor="birth_date">Birth Date</Label>
                  <Input id="birth_date" type="date" value={form.birth_date} onChange={(e) => setField('birth_date', e.target.value)} />
                </div>
                <div>
                  <Label htmlFor="email">Email</Label>
                  <Input id="email" type="email" value={form.email} onChange={(e) => setField('email', e.target.value)} />
                </div>
                <div>
                  <Label htmlFor="phone_number">Phone Number</Label>
                  <Input id="phone_number" value={form.phone_number} onChange={(e) => setField('phone_number', e.target.value)} />
                </div>
                <div>
                  <Label htmlFor="country">Country</Label>
                  <Input id="country" value={form.country} onChange={(e) => setField('country', e.target.value)} placeholder="e.g. Ireland" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Professional Information</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <Label htmlFor="company">Company</Label>
                <Input id="company" value={form.company} onChange={(e) => setField('company', e.target.value)} />
              </div>
              <div>
                <Label htmlFor="industry">Industry</Label>
                <Input id="industry" value={form.industry} onChange={(e) => setField('industry', e.target.value)} />
              </div>
              <div>
                <Label htmlFor="job_title">Job Title</Label>
                <Input id="job_title" value={form.job_title} onChange={(e) => setField('job_title', e.target.value)} />
              </div>
              <div>
                <Label htmlFor="role_focus">Role Focus</Label>
                <Input id="role_focus" value={form.role_focus} onChange={(e) => setField('role_focus', e.target.value)} placeholder="e.g. Procurement manager" />
              </div>
              <div>
                <Label htmlFor="budget_range">Typical Budget Range</Label>
                <Input id="budget_range" value={form.budget_range} onChange={(e) => setField('budget_range', e.target.value)} placeholder="e.g. €100k - €2M" />
              </div>
              <div>
                <Label htmlFor="notification_frequency">Notification Frequency</Label>
                <Input id="notification_frequency" value={form.notification_frequency} onChange={(e) => setField('notification_frequency', e.target.value)} placeholder="immediate, daily, weekly" />
              </div>
              <div>
                <Label htmlFor="language">Language</Label>
                <Input id="language" value={form.language} onChange={(e) => setField('language', e.target.value)} />
              </div>
              <div>
                <Label htmlFor="timezone">Timezone</Label>
                <Input id="timezone" value={form.timezone} onChange={(e) => setField('timezone', e.target.value)} />
              </div>
              <div>
                <Label htmlFor="linkedin_url">LinkedIn URL</Label>
                <Input id="linkedin_url" value={form.linkedin_url} onChange={(e) => setField('linkedin_url', e.target.value)} />
              </div>
              <div>
                <Label htmlFor="website_url">Website</Label>
                <Input id="website_url" value={form.website_url} onChange={(e) => setField('website_url', e.target.value)} />
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Tender Preferences</CardTitle>
            </CardHeader>
            <CardContent className="space-y-5">
              <div>
                <Label>Type of Tenders Interested In</Label>
                <div className="mt-2 grid grid-cols-1 sm:grid-cols-2 gap-2">
                  {tenderTypeOptions.map((option) => (
                    <label key={option} className="flex items-center gap-2 rounded-md border border-border px-3 py-2 cursor-pointer">
                      <Checkbox
                        checked={form.tender_interest_types.includes(option)}
                        onCheckedChange={() => toggleOption('tender_interest_types', option)}
                      />
                      <span className="text-sm text-card-foreground">{option}</span>
                    </label>
                  ))}
                </div>
              </div>

              <div>
                <Label>Preferred Procurement Regions</Label>
                <div className="mt-2 flex flex-wrap gap-2">
                  {regionOptions.map((option) => (
                    <button
                      key={option}
                      type="button"
                      onClick={() => toggleOption('procurement_regions', option)}
                      className={`rounded-full border px-3 py-1 text-sm ${
                        form.procurement_regions.includes(option)
                          ? 'border-primary/50 bg-primary/20 text-primary'
                          : 'border-border text-muted-foreground'
                      }`}
                    >
                      {option}
                    </button>
                  ))}
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label>Preferred Notice Types</Label>
                  <div className="mt-2 space-y-2">
                    {noticeTypeOptions.map((option) => (
                      <label key={option} className="flex items-center gap-2">
                        <Checkbox
                          checked={form.preferred_notice_types.includes(option)}
                          onCheckedChange={() => toggleOption('preferred_notice_types', option)}
                        />
                        <span className="text-sm text-card-foreground">{option}</span>
                      </label>
                    ))}
                  </div>
                </div>
                <div>
                  <Label>Preferred Contract Types</Label>
                  <div className="mt-2 space-y-2">
                    {contractTypeOptions.map((option) => (
                      <label key={option} className="flex items-center gap-2">
                        <Checkbox
                          checked={form.preferred_contract_types.includes(option)}
                          onCheckedChange={() => toggleOption('preferred_contract_types', option)}
                        />
                        <span className="text-sm text-card-foreground">{option}</span>
                      </label>
                    ))}
                  </div>
                </div>
              </div>

              <div>
                <Label htmlFor="cpv_interest_codes">CPV Codes of Interest</Label>
                <Input
                  id="cpv_interest_codes"
                  value={form.cpv_interest_codes}
                  onChange={(e) => setField('cpv_interest_codes', e.target.value)}
                  placeholder="e.g. 72000000, 72250000, 80500000"
                />
                <p className="mt-1 text-xs text-muted-foreground">Comma-separated CPV codes for watchlists and alert scoring.</p>
              </div>

              <div>
                <Label htmlFor="bio">Additional Notes</Label>
                <Textarea id="bio" value={form.bio} onChange={(e) => setField('bio', e.target.value)} rows={4} />
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Recommended Extra Fields</CardTitle>
            </CardHeader>
            <CardContent className="flex flex-wrap gap-2">
              {['Bid role', 'Preferred deal size', 'Target regions', 'CPV watchlist', 'Notification cadence', 'Language/timezone'].map((item) => (
                <Badge key={item} variant="outline">{item}</Badge>
              ))}
            </CardContent>
          </Card>

          {(saveError || saveNotice) && (
            <p className={`text-sm ${saveError ? 'text-red-400' : 'text-emerald-400'}`}>{saveError || saveNotice}</p>
          )}

          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground">
              Logged in as <span className="text-card-foreground">{displayName || currentUser?.email || 'User'}</span>
            </p>
            <Button type="submit" disabled={saving}>
              {saving ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Save className="h-4 w-4 mr-2" />}
              Save Profile
            </Button>
          </div>
        </form>
      </PageBody>
    </Page>
  );
}
