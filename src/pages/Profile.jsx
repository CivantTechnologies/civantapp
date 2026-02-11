import React, { useEffect, useMemo, useRef, useState } from 'react';
import { UserRound, Save, Loader2, Upload, X } from 'lucide-react';
import { civant } from '@/api/civantClient';
import { useAuth } from '@/lib/auth';
import Cropper from 'react-easy-crop';
import {
  Page,
  PageHeader,
  PageTitle,
  PageDescription,
  PageBody,
  Card,
  CardHeader,
  CardTitle,
  CardContent,
  Button,
  Input,
  Badge
} from '@/components/ui';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Checkbox } from '@/components/ui/checkbox';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Slider } from '@/components/ui/slider';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import 'react-easy-crop/react-easy-crop.css';

const PROFILE_DRAFT_VERSION = 1;
const PROFILE_DRAFT_PREFIX = 'civant_profile_draft_v1';
const MAX_AVATAR_SIZE_BYTES = 10 * 1024 * 1024;
const AVATAR_OUTPUT_SIZE = 512;
const AVATAR_CROP_VIEWPORT = 300;
const AVATAR_ZOOM_MIN = 1;
const AVATAR_ZOOM_MAX = 4;

const phoneCodeOptions = [
  { code: '+353', label: 'Ireland (+353)' },
  { code: '+33', label: 'France (+33)' },
  { code: '+34', label: 'Spain (+34)' },
  { code: '+44', label: 'United Kingdom (+44)' },
  { code: '+49', label: 'Germany (+49)' },
  { code: '+39', label: 'Italy (+39)' },
  { code: '+31', label: 'Netherlands (+31)' },
  { code: '+32', label: 'Belgium (+32)' },
  { code: '+351', label: 'Portugal (+351)' },
  { code: '+45', label: 'Denmark (+45)' },
  { code: '+46', label: 'Sweden (+46)' },
  { code: '+47', label: 'Norway (+47)' },
  { code: '+1', label: 'US/Canada (+1)' }
];

const countryOptions = [
  'Ireland',
  'France',
  'Spain',
  'United Kingdom',
  'Germany',
  'Italy',
  'Portugal',
  'Belgium',
  'Netherlands',
  'Luxembourg',
  'Denmark',
  'Sweden',
  'Norway',
  'Finland',
  'Austria',
  'Poland',
  'Czech Republic',
  'Romania',
  'United States',
  'Other'
];

const tenderTypeOptions = [
  'IT & Software',
  'AI, Data & Analytics',
  'Cybersecurity',
  'Construction & Infrastructure',
  'Healthcare & Medical',
  'Transport & Logistics',
  'Energy & Utilities',
  'Consulting & Professional Services',
  'Education & Training',
  'Facilities & Maintenance',
  'Telecoms',
  'Food & Catering',
  'Environmental Services',
  'Security Services',
  'Legal Services',
  'Manufacturing & Industrial Equipment'
];

const noticeTypeOptions = ['Tender', 'Award', 'Corrigendum', 'PIN', 'Contract notice'];
const contractTypeOptions = ['Supplies', 'Services', 'Works', 'Framework', 'Concession'];
const regionOptions = [
  'Ireland',
  'France',
  'Spain',
  'EU-wide',
  'United Kingdom',
  'Nordics',
  'DACH',
  'Benelux',
  'Iberia',
  'Italy',
  'Central Europe'
];

function ensureArray(value) {
  if (Array.isArray(value)) return value.filter(Boolean).map((v) => String(v));
  if (!value) return [];
  return String(value)
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

function getProfileDraftKey(email) {
  const cleanEmail = String(email || '').trim().toLowerCase();
  if (!cleanEmail) return '';
  return `${PROFILE_DRAFT_PREFIX}_${cleanEmail}`;
}

function readProfileDraft(storageKey) {
  if (typeof window === 'undefined' || !window.localStorage || !storageKey) return null;
  try {
    const raw = window.localStorage.getItem(storageKey);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || parsed.version !== PROFILE_DRAFT_VERSION || !parsed.form) return null;
    return parsed.form;
  } catch {
    return null;
  }
}

function writeProfileDraft(storageKey, form) {
  if (typeof window === 'undefined' || !window.localStorage || !storageKey) return;
  try {
    window.localStorage.setItem(
      storageKey,
      JSON.stringify({
        version: PROFILE_DRAFT_VERSION,
        updated_at: new Date().toISOString(),
        form
      })
    );
  } catch {
    // ignore localStorage quota errors to avoid blocking profile edits
  }
}

function clearProfileDraft(storageKey) {
  if (typeof window === 'undefined' || !window.localStorage || !storageKey) return;
  try {
    window.localStorage.removeItem(storageKey);
  } catch {
    // no-op
  }
}

function splitPhoneNumber(value) {
  const source = String(value || '').trim();
  if (!source) {
    return {
      phone_country_code: '+353',
      phone_number_local: ''
    };
  }
  const matchedCode = phoneCodeOptions
    .map((option) => option.code)
    .sort((a, b) => b.length - a.length)
    .find((code) => source.startsWith(code));

  if (!matchedCode) {
    return {
      phone_country_code: '+353',
      phone_number_local: source
    };
  }

  return {
    phone_country_code: matchedCode,
    phone_number_local: source.slice(matchedCode.length).trim().replace(/^\s+/, '')
  };
}

function composePhoneNumber(code, number) {
  const cleanCode = String(code || '').trim();
  const cleanNumber = String(number || '').trim();
  if (!cleanCode && !cleanNumber) return '';
  if (!cleanCode) return cleanNumber;
  if (!cleanNumber) return cleanCode;
  return `${cleanCode} ${cleanNumber}`;
}

const defaultForm = {
  first_name: '',
  last_name: '',
  birth_date: '',
  email: '',
  phone_country_code: '+353',
  phone_number_local: '',
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
  notification_frequency: 'daily',
  language: 'en',
  timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'Europe/Dublin',
  avatar_url: '',
  linkedin_url: '',
  website_url: '',
  bio: ''
};

function normalizeForm(value) {
  const input = value || {};
  return {
    ...defaultForm,
    ...input,
    tender_interest_types: ensureArray(input.tender_interest_types),
    procurement_regions: ensureArray(input.procurement_regions),
    preferred_notice_types: ensureArray(input.preferred_notice_types),
    preferred_contract_types: ensureArray(input.preferred_contract_types),
    cpv_interest_codes: Array.isArray(input.cpv_interest_codes)
      ? ensureArray(input.cpv_interest_codes).join(', ')
      : String(input.cpv_interest_codes || '')
  };
}

function addUniqueOption(list, value) {
  const current = ensureArray(list);
  if (current.includes(value)) return current;
  return [...current, value];
}

function removeOption(list, value) {
  return ensureArray(list).filter((item) => item !== value);
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function loadImageFromDataUrl(dataUrl) {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error('Could not load this image.'));
    image.src = dataUrl;
  });
}

function renderCroppedAvatar(imageSource, cropPixels) {
  return loadImageFromDataUrl(imageSource).then((image) => {
    const sourceWidth = Number(image.naturalWidth || image.width || 0);
    const sourceHeight = Number(image.naturalHeight || image.height || 0);
    if (!sourceWidth || !sourceHeight || !cropPixels) {
      throw new Error('Could not read crop area.');
    }

    const sx = clamp(Math.round(Number(cropPixels.x || 0)), 0, Math.max(0, sourceWidth - 1));
    const sy = clamp(Math.round(Number(cropPixels.y || 0)), 0, Math.max(0, sourceHeight - 1));
    const sw = clamp(Math.round(Number(cropPixels.width || sourceWidth)), 1, sourceWidth - sx);
    const sh = clamp(Math.round(Number(cropPixels.height || sourceHeight)), 1, sourceHeight - sy);

    const canvas = document.createElement('canvas');
    canvas.width = AVATAR_OUTPUT_SIZE;
    canvas.height = AVATAR_OUTPUT_SIZE;
    const context = canvas.getContext('2d');
    if (!context) throw new Error('Could not prepare avatar crop canvas.');

    context.clearRect(0, 0, AVATAR_OUTPUT_SIZE, AVATAR_OUTPUT_SIZE);
    context.imageSmoothingEnabled = true;
    context.imageSmoothingQuality = 'high';
    context.drawImage(image, sx, sy, sw, sh, 0, 0, AVATAR_OUTPUT_SIZE, AVATAR_OUTPUT_SIZE);
    return canvas.toDataURL('image/jpeg', 0.88);
  });
}

export default function Profile() {
  const { currentUser } = useAuth();
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState('');
  const [saveNotice, setSaveNotice] = useState('');
  const [avatarError, setAvatarError] = useState('');
  const [avatarCropOpen, setAvatarCropOpen] = useState(false);
  const [avatarCropSource, setAvatarCropSource] = useState('');
  const [avatarCrop, setAvatarCrop] = useState({ x: 0, y: 0 });
  const [avatarZoom, setAvatarZoom] = useState(1);
  const [avatarCroppedAreaPixels, setAvatarCroppedAreaPixels] = useState(null);
  const [avatarApplying, setAvatarApplying] = useState(false);
  const [profileId, setProfileId] = useState('');
  const [userId, setUserId] = useState('');
  const [draftStorageKey, setDraftStorageKey] = useState('');
  const [form, setForm] = useState(defaultForm);
  const avatarObjectUrlRef = useRef('');

  const displayName = useMemo(() => [form.first_name, form.last_name].filter(Boolean).join(' ').trim(), [form.first_name, form.last_name]);

  useEffect(() => {
    let mounted = true;

    const load = async () => {
      setLoading(true);
      setSaveError('');
      setSaveNotice('');
      setAvatarError('');

      try {
        const me = await civant.auth.me();
        const effectiveUserId = String(me?.userId || me?.user_id || '').trim();
        const effectiveEmail = String(currentUser?.email || me?.email || '').trim().toLowerCase();
        const nextDraftStorageKey = getProfileDraftKey(effectiveEmail);
        if (!mounted) return;

        setUserId(effectiveUserId);
        setDraftStorageKey(nextDraftStorageKey);

        const query = effectiveUserId ? { user_id: effectiveUserId } : { email: effectiveEmail };
        const rows = await civant.entities.UserProfile.filter(query, '-created_at', 1);
        const row = Array.isArray(rows) && rows.length ? rows[0] : null;

        if (!mounted) return;

        const existingPhone = splitPhoneNumber(row?.phone_number || '');
        const baseForm = normalizeForm({
          first_name: row?.first_name || '',
          last_name: row?.last_name || '',
          birth_date: row?.birth_date || '',
          email: row?.email || effectiveEmail,
          phone_country_code: existingPhone.phone_country_code,
          phone_number_local: existingPhone.phone_number_local,
          country: row?.country || '',
          company: row?.company || '',
          industry: row?.industry || '',
          job_title: row?.job_title || '',
          role_focus: row?.role_focus || '',
          tender_interest_types: ensureArray(row?.tender_interest_types),
          procurement_regions: ensureArray(row?.procurement_regions),
          cpv_interest_codes: ensureArray(row?.cpv_interest_codes).join(', '),
          preferred_notice_types: ensureArray(row?.preferred_notice_types),
          preferred_contract_types: ensureArray(row?.preferred_contract_types),
          notification_frequency: row?.notification_frequency || 'daily',
          language: row?.language || 'en',
          timezone: row?.timezone || defaultForm.timezone,
          avatar_url: row?.avatar_url || '',
          linkedin_url: row?.linkedin_url || '',
          website_url: row?.website_url || '',
          bio: row?.bio || ''
        });

        const draft = readProfileDraft(nextDraftStorageKey);
        if (draft) {
          const restored = normalizeForm({
            ...baseForm,
            ...draft,
            email: String(draft.email || baseForm.email || effectiveEmail).trim().toLowerCase()
          });
          setForm(restored);
          setSaveNotice('Unsaved draft restored.');
        } else {
          setForm(baseForm);
        }
        setProfileId(String(row?.id || ''));
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

  useEffect(() => {
    if (loading || !draftStorageKey) return;
    const timeout = window.setTimeout(() => {
      writeProfileDraft(draftStorageKey, form);
    }, 150);
    return () => {
      window.clearTimeout(timeout);
    };
  }, [draftStorageKey, form, loading]);

  const setField = (key, value) => setForm((prev) => ({ ...prev, [key]: value }));

  const releaseAvatarObjectUrl = () => {
    if (!avatarObjectUrlRef.current) return;
    try {
      URL.revokeObjectURL(avatarObjectUrlRef.current);
    } catch {
      // no-op
    }
    avatarObjectUrlRef.current = '';
  };

  useEffect(() => {
    return () => {
      releaseAvatarObjectUrl();
    };
  }, []);

  const toggleOption = (key, value) => {
    setForm((prev) => {
      const list = ensureArray(prev[key]);
      const next = list.includes(value) ? list.filter((item) => item !== value) : [...list, value];
      return { ...prev, [key]: next };
    });
  };

  const addOption = (key, value) => {
    setForm((prev) => ({
      ...prev,
      [key]: addUniqueOption(prev[key], value)
    }));
  };

  const removeSelectedOption = (key, value) => {
    setForm((prev) => ({
      ...prev,
      [key]: removeOption(prev[key], value)
    }));
  };

  const openAvatarCropper = (source) => {
    if (!source) return;
    setAvatarCropSource(source);
    setAvatarCrop({ x: 0, y: 0 });
    setAvatarZoom(1);
    setAvatarCroppedAreaPixels(null);
    setAvatarCropOpen(true);
  };

  const closeAvatarCropper = () => {
    setAvatarCropOpen(false);
    setAvatarCropSource('');
    setAvatarCrop({ x: 0, y: 0 });
    setAvatarZoom(1);
    setAvatarCroppedAreaPixels(null);
    setAvatarApplying(false);
    releaseAvatarObjectUrl();
  };

  const updateAvatarZoom = (nextZoom) => {
    setAvatarZoom(clamp(nextZoom, AVATAR_ZOOM_MIN, AVATAR_ZOOM_MAX));
  };

  const onAvatarFileChange = (event) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setAvatarError('');
    if (!file.type.startsWith('image/')) {
      setAvatarError('Please upload an image file (PNG, JPG, WEBP).');
      event.target.value = '';
      return;
    }
    if (file.size > MAX_AVATAR_SIZE_BYTES) {
      setAvatarError('Profile picture must be 10 MB or smaller.');
      event.target.value = '';
      return;
    }

    releaseAvatarObjectUrl();
    try {
      const objectUrl = URL.createObjectURL(file);
      avatarObjectUrlRef.current = objectUrl;
      openAvatarCropper(objectUrl);
    } catch {
      setAvatarError('Could not prepare this image. Try another file.');
    }
    event.target.value = '';
  };

  const onAvatarCropComplete = (_croppedArea, croppedAreaPixels) => {
    setAvatarCroppedAreaPixels(croppedAreaPixels);
  };

  const applyAvatarCrop = async () => {
    if (!avatarCropSource) return;
    if (!avatarCroppedAreaPixels) {
      setAvatarError('Wait a moment for the preview to load, then try again.');
      return;
    }
    setAvatarApplying(true);
    setAvatarError('');
    try {
      const cropped = await renderCroppedAvatar(avatarCropSource, avatarCroppedAreaPixels);
      setField('avatar_url', cropped);
      setSaveNotice('Profile picture updated.');
      closeAvatarCropper();
    } catch (error) {
      setAvatarError(error?.message || 'Could not crop this image. Try another file.');
      setAvatarApplying(false);
    }
  };

  const triggerAvatarUpload = () => {
    const element = document.getElementById('avatar_upload');
    element?.click();
  };

  const startEditingCurrentAvatar = () => {
    if (!form.avatar_url) return;
    setAvatarError('');
    releaseAvatarObjectUrl();
    openAvatarCropper(form.avatar_url);
  };

  const onRemoveAvatar = () => {
    setField('avatar_url', '');
    setSaveNotice('Profile picture removed.');
    if (avatarCropOpen) {
      closeAvatarCropper();
    }
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
        phone_number: composePhoneNumber(form.phone_country_code, form.phone_number_local) || null,
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
        budget_range: null,
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

      clearProfileDraft(draftStorageKey || getProfileDraftKey(payload.email));
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
              <div className="flex items-start gap-4">
                <div className="h-16 w-16 rounded-full border border-border bg-muted/50 overflow-hidden flex items-center justify-center shrink-0">
                  {form.avatar_url ? (
                    <img src={form.avatar_url} alt="Profile avatar" className="h-full w-full object-cover" />
                  ) : (
                    <UserRound className="h-8 w-8 text-muted-foreground" />
                  )}
                </div>
                <div className="flex-1">
                  <Label htmlFor="avatar_upload">Profile Picture</Label>
                  <div className="mt-1 flex flex-wrap gap-2">
                    <input
                      id="avatar_upload"
                      type="file"
                      accept="image/png,image/jpeg,image/webp,image/gif"
                      className="hidden"
                      onChange={onAvatarFileChange}
                    />
                    <Button type="button" variant="secondary" onClick={triggerAvatarUpload}>
                      <Upload className="h-4 w-4 mr-1" />
                      Upload image
                    </Button>
                    <Button
                      type="button"
                      variant="secondary"
                      onClick={startEditingCurrentAvatar}
                      disabled={!form.avatar_url}
                      title={form.avatar_url ? 'Edit current picture' : 'Upload a picture first'}
                    >
                      Edit picture
                    </Button>
                    <Button type="button" variant="ghost" onClick={onRemoveAvatar} disabled={!form.avatar_url}>
                      <X className="h-4 w-4 mr-1" />
                      Remove
                    </Button>
                  </div>
                  <p className="mt-1 text-xs text-muted-foreground">Max file size: 10 MB.</p>
                  {avatarError && <p className="mt-1 text-xs text-red-400">{avatarError}</p>}
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
                  <Label>Phone Number</Label>
                  <div className="mt-1 grid grid-cols-[220px_1fr] gap-2">
                    <Select value={form.phone_country_code} onValueChange={(value) => setField('phone_country_code', value)}>
                      <SelectTrigger>
                        <SelectValue placeholder="Code" />
                      </SelectTrigger>
                      <SelectContent>
                        {phoneCodeOptions.map((option) => (
                          <SelectItem key={option.code} value={option.code}>
                            {option.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <Input
                      value={form.phone_number_local}
                      onChange={(e) => setField('phone_number_local', e.target.value)}
                      placeholder="871234567"
                    />
                  </div>
                </div>
                <div>
                  <Label>Country</Label>
                  <Select value={form.country || undefined} onValueChange={(value) => setField('country', value)}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select country" />
                    </SelectTrigger>
                    <SelectContent>
                      {countryOptions.map((option) => (
                        <SelectItem key={option} value={option}>
                          {option}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
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
                <div className="mt-2 grid grid-cols-1 lg:grid-cols-2 gap-3">
                  <div className="rounded-lg border border-border/70 bg-muted/20 p-3">
                    <p className="text-xs uppercase tracking-wide text-muted-foreground">Available categories</p>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {tenderTypeOptions.map((option) => {
                        const selected = form.tender_interest_types.includes(option);
                        return (
                          <button
                            key={option}
                            type="button"
                            disabled={selected}
                            onClick={() => addOption('tender_interest_types', option)}
                            className={`rounded-full border px-3 py-1.5 text-xs transition ${
                              selected
                                ? 'border-primary/40 bg-primary/20 text-primary cursor-not-allowed'
                                : 'border-border text-muted-foreground hover:border-primary/50 hover:text-primary'
                            }`}
                          >
                            {selected ? 'Selected' : 'Add'} {option}
                          </button>
                        );
                      })}
                    </div>
                  </div>
                  <div className="rounded-lg border border-border/70 bg-muted/20 p-3">
                    <p className="text-xs uppercase tracking-wide text-muted-foreground">Selected categories</p>
                    {form.tender_interest_types.length ? (
                      <div className="mt-2 flex flex-wrap gap-2">
                        {form.tender_interest_types.map((option) => (
                          <Badge key={option} variant="outline" className="gap-1 py-1">
                            {option}
                            <button type="button" onClick={() => removeSelectedOption('tender_interest_types', option)} aria-label={`Remove ${option}`}>
                              <X className="h-3 w-3" />
                            </button>
                          </Badge>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-2 text-sm text-muted-foreground">Select categories from the left panel.</p>
                    )}
                  </div>
                </div>
              </div>

              <div>
                <Label>Preferred Procurement Regions</Label>
                <div className="mt-2 grid grid-cols-1 lg:grid-cols-2 gap-3">
                  <div className="rounded-lg border border-border/70 bg-muted/20 p-3">
                    <p className="text-xs uppercase tracking-wide text-muted-foreground">Available regions</p>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {regionOptions.map((option) => {
                        const selected = form.procurement_regions.includes(option);
                        return (
                          <button
                            key={option}
                            type="button"
                            disabled={selected}
                            onClick={() => addOption('procurement_regions', option)}
                            className={`rounded-full border px-3 py-1.5 text-xs transition ${
                              selected
                                ? 'border-primary/40 bg-primary/20 text-primary cursor-not-allowed'
                                : 'border-border text-muted-foreground hover:border-primary/50 hover:text-primary'
                            }`}
                          >
                            {selected ? 'Selected' : 'Add'} {option}
                          </button>
                        );
                      })}
                    </div>
                  </div>
                  <div className="rounded-lg border border-border/70 bg-muted/20 p-3">
                    <p className="text-xs uppercase tracking-wide text-muted-foreground">Selected regions</p>
                    {form.procurement_regions.length ? (
                      <div className="mt-2 flex flex-wrap gap-2">
                        {form.procurement_regions.map((option) => (
                          <Badge key={option} variant="outline" className="gap-1 py-1">
                            {option}
                            <button type="button" onClick={() => removeSelectedOption('procurement_regions', option)} aria-label={`Remove ${option}`}>
                              <X className="h-3 w-3" />
                            </button>
                          </Badge>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-2 text-sm text-muted-foreground">Select regions from the left panel.</p>
                    )}
                  </div>
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

        <Dialog open={avatarCropOpen} onOpenChange={(open) => (!open ? closeAvatarCropper() : setAvatarCropOpen(true))}>
          <DialogContent className="max-w-lg">
            <DialogHeader>
              <DialogTitle>Crop profile picture</DialogTitle>
              <DialogDescription>Drag to reposition and use trackpad pinch, mouse wheel, or slider to zoom.</DialogDescription>
            </DialogHeader>

            <div className="space-y-4">
              <div
                className="mx-auto w-full max-w-[300px] overflow-hidden rounded-2xl border border-border bg-muted/20 aspect-square"
                style={{ maxWidth: `${AVATAR_CROP_VIEWPORT}px` }}
              >
                {avatarCropSource ? (
                  <Cropper
                    image={avatarCropSource}
                    crop={avatarCrop}
                    zoom={avatarZoom}
                    onCropChange={setAvatarCrop}
                    onZoomChange={updateAvatarZoom}
                    onCropComplete={onAvatarCropComplete}
                    cropShape="round"
                    showGrid={false}
                    aspect={1}
                    zoomWithScroll
                    restrictPosition
                    objectFit="cover"
                  />
                ) : null}
              </div>

              <div className="space-y-3">
                <div>
                  <div className="mb-1 flex items-center justify-between text-xs text-muted-foreground">
                    <span>Zoom</span>
                    <span>{avatarZoom.toFixed(2)}x</span>
                  </div>
                  <Slider
                    min={AVATAR_ZOOM_MIN}
                    max={AVATAR_ZOOM_MAX}
                    step={0.01}
                    value={[avatarZoom]}
                    onValueChange={(value) => updateAvatarZoom(value[0] || 1)}
                  />
                </div>
                <p className="text-xs text-muted-foreground">Tip: drag the photo to center your face. Use wheel/pinch for fine zoom.</p>
              </div>
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="ghost"
                onClick={closeAvatarCropper}
                disabled={avatarApplying}
              >
                Cancel
              </Button>
              <Button type="button" onClick={applyAvatarCrop} disabled={avatarApplying || !avatarCropSource}>
                {avatarApplying ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
                Save photo
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </PageBody>
    </Page>
  );
}
