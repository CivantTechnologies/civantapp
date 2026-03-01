import React, { useEffect, useMemo, useState } from 'react';
import { civant } from '@/api/civantClient';
import { supabase } from '@/lib/supabaseClient';
import { useAuth } from '@/lib/auth';
import { useTenant } from '@/lib/tenant';
import {
  AlertCircle, Loader2, RefreshCw, Shield, Building2, Users,
  PlugZap, UserPlus, UserMinus, Copy, Mail, Calendar, MessageSquare,
  HardDrive, CheckCircle2, Settings
} from 'lucide-react';
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
  Badge,
  Button,
  Input
} from '@/components/ui';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { formatDistanceToNow } from 'date-fns';

function unwrapResponse(response) {
  return response?.data ?? response ?? null;
}

function formatDateRelative(value) {
  if (!value) return 'No data yet';
  try {
    return formatDistanceToNow(new Date(value), { addSuffix: true });
  } catch {
    return 'No data yet';
  }
}

function formatDateTime(value) {
  if (!value) return 'No data yet';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'No data yet';
  return `${date.toLocaleString()} (${formatDateRelative(value)})`;
}

function StatusBadge({ status }) {
  const normalized = String(status || 'no_data').toLowerCase();
  if (normalized === 'success') return <Badge variant="primary">Healthy</Badge>;
  if (normalized === 'partial') return <Badge variant="secondary">Partial</Badge>;
  if (normalized === 'fail') return <Badge variant="destructive">Failed</Badge>;
  if (normalized === 'disabled') return <Badge variant="ghost">Disabled</Badge>;
  return <Badge variant="ghost">No data yet</Badge>;
}

function roleBadgeVariant(role) {
  if (role === 'owner') return 'primary';
  if (role === 'admin') return 'secondary';
  return 'ghost';
}

export default function System() {
  const { roles, currentUser } = useAuth();
  const { tenants, activeTenantId, refreshTenants } = useTenant();
  const isSystemAllowed = Array.isArray(roles) && (roles.includes('admin') || roles.includes('creator'));

  const currentTenantRole = useMemo(() => {
    const t = Array.isArray(tenants) ? tenants.find((t) => t.id === activeTenantId) : null;
    return t?.role || 'member';
  }, [tenants, activeTenantId]);

  // Override with direct DB role since backend may return stale 'member'
  const [dbRole, setDbRole] = useState(null);
  useEffect(() => {
    if (!activeTenantId) return;
    (async () => {
      const { data: { session } } = await supabase.auth.getSession();
      if (!session?.user?.id) return;
      const { data } = await supabase
        .from('tenant_users')
        .select('role')
        .eq('tenant_id', activeTenantId)
        .eq('user_id', session.user.id)
        .single();
      if (data?.role) setDbRole(data.role);
    })();
  }, [activeTenantId]);

  const effectiveRole = dbRole || currentTenantRole;
  const canInvite = ['owner', 'admin'].includes(effectiveRole);
  const canManageRoles = ['owner', 'admin'].includes(effectiveRole);

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [denied, setDenied] = useState(false);
  const [error, setError] = useState('');

  const [tenant, setTenant] = useState(null);
  const [tenantUsers, setTenantUsers] = useState([]);
  const [connectors, setConnectors] = useState([]);

  // Invitation state
  const [invitations, setInvitations] = useState([]);
  const [inviteEmail, setInviteEmail] = useState('');
  const [inviteRole, setInviteRole] = useState('member');
  const [inviting, setInviting] = useState(false);
  const [inviteLink, setInviteLink] = useState(null);
  const [teamMessage, setTeamMessage] = useState('');
  const [teamError, setTeamError] = useState('');
  const [copied, setCopied] = useState(false);

  // Member action state
  const [memberActionLoading, setMemberActionLoading] = useState('');

  // Support state
  const [supportStatus, setSupportStatus] = useState(null);
  const [supportAudit, setSupportAudit] = useState([]);
  const [supportLoading, setSupportLoading] = useState(true);
  const [supportActionLoading, setSupportActionLoading] = useState('');
  const [supportMessage, setSupportMessage] = useState('');
  const [supportError, setSupportError] = useState('');
  const [enableDurationMinutes, setEnableDurationMinutes] = useState('60');
  const [enableReason, setEnableReason] = useState('');
  const [revokeReason, setRevokeReason] = useState('');

  const loadInvitations = async () => {
    try {
      const payload = unwrapResponse(await civant.system.listInvitations());
      setInvitations(Array.isArray(payload) ? payload : []);
    } catch {
      // Non-fatal — invitations section just stays empty
    }
  };

  const loadSupportSection = async () => {
    setSupportLoading(true);
    setSupportError('');
    try {
      const [statusPayload, auditPayload] = await Promise.all([
        civant.system.getSupportAccessStatus(),
        civant.system.listSupportAccessAudit({ limit: 10 })
      ]);
      setSupportStatus(unwrapResponse(statusPayload));
      setSupportAudit(Array.isArray(unwrapResponse(auditPayload)) ? unwrapResponse(auditPayload) : []);
    } catch (err) {
      setSupportError(err?.message || 'Failed to load support access');
    } finally {
      setSupportLoading(false);
    }
  };

  const loadSystemData = async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    setError('');

    try {
      if (!activeTenantId) {
        throw new Error('Select a tenant');
      }

      const tenantPayload = unwrapResponse(await civant.system.getTenant());
      setTenant(tenantPayload || null);

      const [usersPayload, connectorsPayload] = await Promise.all([
        civant.system.listTenantUsers(),
        civant.system.getConnectorStatus()
      ]);

      setTenantUsers(Array.isArray(unwrapResponse(usersPayload)) ? unwrapResponse(usersPayload) : []);
      setConnectors(Array.isArray(unwrapResponse(connectorsPayload)) ? unwrapResponse(connectorsPayload) : []);

      // Enrich with correct roles from tenant_users
      const { data: tuRows } = await supabase
        .from('tenant_users')
        .select('user_id, role')
        .eq('tenant_id', activeTenantId);
      if (tuRows?.length) {
        const roleMap = Object.fromEntries(tuRows.map(r => [r.user_id, r.role]));
        setTenantUsers(prev => prev.map(u => ({
          ...u,
          role: roleMap[u.userId] || u.role || 'member'
        })));
      }

      await Promise.all([loadInvitations(), loadSupportSection()]);
      setDenied(false);
    } catch (err) {
      const status = err?.status || err?.response?.status;
      if (status === 403) {
        setDenied(true);
      } else {
        setError(err?.message || 'Failed to load system settings');
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    if (!isSystemAllowed) {
      setLoading(false);
      return;
    }
    loadSystemData(false);
  }, [activeTenantId, isSystemAllowed]);

  const handleInvite = async () => {
    const email = inviteEmail.trim().toLowerCase();
    if (!email || !email.includes('@')) {
      setTeamError('Enter a valid email address.');
      return;
    }
    setTeamError('');
    setTeamMessage('');
    setInviteLink(null);
    setInviting(true);
    try {
      const result = unwrapResponse(await civant.system.inviteUser({ email, role: inviteRole }));
      setInviteEmail('');
      setInviteRole('member');
      setInviteLink(result?.inviteUrl || null);
      await loadInvitations();

      // Provision auth account so the invited user can log in
      try {
        const { data: { session } } = await supabase.auth.getSession();
        const provRes = await fetch(
          `${import.meta.env.VITE_SUPABASE_URL}/functions/v1/provision-user`,
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${session?.access_token}`,
            },
            body: JSON.stringify({ email }),
          }
        );
        const provData = await provRes.json();
        if (provData.status === 'created') {
          setTeamMessage(`Invitation created for ${email}. A password setup email has been sent.`);
        } else if (provData.status === 'existing_user') {
          setTeamMessage(`Invitation created for ${email}. They already have an account.`);
        } else {
          setTeamMessage(`Invitation created for ${email}. Note: could not auto-provision account.`);
        }
      } catch {
        setTeamMessage(`Invitation created for ${email}. Ask them to contact you if they cannot log in.`);
      }
    } catch (err) {
      setTeamError(err?.message || 'Failed to create invitation.');
    } finally {
      setInviting(false);
    }
  };

  const handleCopyLink = () => {
    if (!inviteLink) return;
    navigator.clipboard.writeText(inviteLink).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  const handleRevokeInvitation = async (token) => {
    setTeamError('');
    setTeamMessage('');
    try {
      await civant.system.revokeInvitation({ token });
      setTeamMessage('Invitation revoked.');
      await loadInvitations();
    } catch (err) {
      setTeamError(err?.message || 'Failed to revoke invitation.');
    }
  };

  const handleChangeRole = async (userId, newRole) => {
    setMemberActionLoading(userId);
    setTeamError('');
    setTeamMessage('');
    try {
      await civant.system.updateTenantUserRole({ userId, newRole });
      setTeamMessage('Role updated.');
      const usersPayload = unwrapResponse(await civant.system.listTenantUsers());
      setTenantUsers(Array.isArray(usersPayload) ? usersPayload : []);
    } catch (err) {
      setTeamError(err?.message || 'Failed to update role.');
    } finally {
      setMemberActionLoading('');
    }
  };

  const handleRemoveMember = async (userId) => {
    setMemberActionLoading(userId);
    setTeamError('');
    setTeamMessage('');
    try {
      await civant.system.removeTenantUser({ userId });
      setTeamMessage('Member removed.');
      const usersPayload = unwrapResponse(await civant.system.listTenantUsers());
      setTenantUsers(Array.isArray(usersPayload) ? usersPayload : []);
    } catch (err) {
      setTeamError(err?.message || 'Failed to remove member.');
    } finally {
      setMemberActionLoading('');
    }
  };

  const handleEnableSupportAccess = async () => {
    if (!enableReason.trim()) {
      setSupportError('Enable reason is required.');
      return;
    }
    setSupportError('');
    setSupportMessage('');
    setSupportActionLoading('enable');
    try {
      await civant.system.enableSupportAccess({
        durationMinutes: Number(enableDurationMinutes),
        reason: enableReason.trim()
      });
      setSupportMessage('Support access enabled.');
      setEnableReason('');
      await loadSupportSection();
    } catch (err) {
      setSupportError(err?.message || 'Failed to enable support access.');
    } finally {
      setSupportActionLoading('');
    }
  };

  const handleRevokeSupportAccess = async () => {
    if (!revokeReason.trim()) {
      setSupportError('Revoke reason is required.');
      return;
    }
    setSupportError('');
    setSupportMessage('');
    setSupportActionLoading('revoke');
    try {
      await civant.system.revokeSupportAccess({ reason: revokeReason.trim() });
      setSupportMessage('Support access revoked.');
      setRevokeReason('');
      await loadSupportSection();
    } catch (err) {
      setSupportError(err?.message || 'Failed to revoke support access.');
    } finally {
      setSupportActionLoading('');
    }
  };

  const regionText = useMemo(() => {
    if (!tenant?.regions || !tenant.regions.length) return 'Not configured';
    return tenant.regions.join(', ');
  }, [tenant]);

  if (!isSystemAllowed) {
    return (
      <Page>
        <Card>
          <CardContent className="p-8 text-center space-y-3">
            <AlertCircle className="h-10 w-10 mx-auto text-destructive" />
            <h2 className="text-xl font-semibold text-card-foreground">Not authorised</h2>
            <p className="text-muted-foreground">You do not have permission to access System settings.</p>
          </CardContent>
        </Card>
      </Page>
    );
  }

  if (!activeTenantId) {
    return (
      <Page>
        <Card>
          <CardContent className="p-8 text-center space-y-3">
            <AlertCircle className="h-10 w-10 mx-auto text-destructive" />
            <h2 className="text-xl font-semibold text-card-foreground">Select a tenant</h2>
            <p className="text-muted-foreground">Choose a tenant from the sidebar to access System settings.</p>
            <Button variant="secondary" onClick={() => refreshTenants()}>Reload tenants</Button>
          </CardContent>
        </Card>
      </Page>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (denied) {
    return (
      <Page>
        <Card>
          <CardContent className="p-8 text-center space-y-3">
            <AlertCircle className="h-10 w-10 mx-auto text-destructive" />
            <h2 className="text-xl font-semibold text-card-foreground">Access denied</h2>
            <p className="text-muted-foreground">You don't have access to System settings.</p>
          </CardContent>
        </Card>
      </Page>
    );
  }

  return (
    <Page>
      <PageHeader>
        <div className="flex items-start justify-between gap-4">
          <div>
            <PageTitle>System / Admin</PageTitle>
            <PageDescription>Tenant context, roles, connector health, and platform controls.</PageDescription>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant="secondary">Tenant: {activeTenantId}</Badge>
            <Button variant="secondary" onClick={() => loadSystemData(true)} disabled={refreshing}>
              {refreshing ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Refresh
            </Button>
          </div>
        </div>
      </PageHeader>

      <PageBody>
        <Tabs defaultValue="overview" className="space-y-6">
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="integrations">Integrations</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="space-y-6">
        {error && (
          <Card>
            <CardContent className="p-4">
              <p className="text-sm text-destructive">{error}</p>
            </CardContent>
          </Card>
        )}

        {/* ── Team Management ───────────────────────────────── */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Users className="h-4 w-4" /> Team</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">

            {teamMessage && <p className="text-sm text-primary">{teamMessage}</p>}
            {teamError && <p className="text-sm text-destructive">{teamError}</p>}

            {/* Invite form */}
            {canInvite && (
              <div className="rounded-xl border border-border p-4 space-y-3">
                <h3 className="text-sm font-semibold text-card-foreground flex items-center gap-2">
                  <UserPlus className="h-4 w-4" /> Invite a new member
                </h3>
                <div className="flex flex-col md:flex-row gap-3">
                  <Input
                    type="email"
                    placeholder="colleague@company.com"
                    value={inviteEmail}
                    onChange={(e) => setInviteEmail(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleInvite()}
                    className="flex-1"
                  />
                  <select
                    value={inviteRole}
                    onChange={(e) => setInviteRole(e.target.value)}
                    className="h-10 rounded-xl border border-input bg-background px-3 py-2 text-sm text-foreground"
                  >
                    <option value="member">Member</option>
                    <option value="admin">Admin</option>
                  </select>
                  <Button onClick={handleInvite} disabled={inviting}>
                    {inviting ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Mail className="h-4 w-4 mr-2" />}
                    Send invite
                  </Button>
                </div>
                {inviteLink && (
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Share this invite link manually:</p>
                    <div className="flex gap-2">
                      <Input readOnly value={inviteLink} className="flex-1 text-xs font-mono" />
                      <Button variant="secondary" onClick={handleCopyLink}>
                        <Copy className="h-4 w-4 mr-1" />
                        {copied ? 'Copied!' : 'Copy'}
                      </Button>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Members list */}
            <div className="space-y-2">
              <h3 className="text-sm font-semibold text-card-foreground">
                Members ({tenantUsers.length})
              </h3>
              {tenantUsers.length === 0 ? (
                <p className="text-sm text-muted-foreground">No members yet</p>
              ) : (
                tenantUsers.map((row) => {
                  const isCurrentUser = (row.email || '').toLowerCase() === (currentUser?.email || '').toLowerCase();
                  const isActioning = memberActionLoading === row.userId;
                  return (
                    <div key={row.userId} className="flex flex-col md:flex-row md:items-center md:justify-between gap-2 rounded-xl border border-border p-3">
                      <div>
                        <p className="text-sm font-medium text-card-foreground">
                          {row.email || 'Not configured'}
                          {isCurrentUser && <span className="ml-2 text-xs text-muted-foreground">(you)</span>}
                        </p>
                        <p className="text-xs text-muted-foreground">{row.userId}</p>
                      </div>
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant={roleBadgeVariant(row.role || (row.roles || [])[0])}>
                          {(row.role || (row.roles || [])[0] || 'member').charAt(0).toUpperCase() + (row.role || (row.roles || [])[0] || 'member').slice(1)}
                        </Badge>
                        {canManageRoles && !isCurrentUser && (
                          <>
                            <select
                              value={row.role || (row.roles || [])[0] || 'member'}
                              onChange={(e) => handleChangeRole(row.userId, e.target.value)}
                              disabled={isActioning}
                              className="h-8 rounded-lg border border-input bg-background px-2 text-xs text-foreground"
                            >
                              <option value="admin">Admin</option>
                              <option value="member">Member</option>
                            </select>
                            <Button
                              variant="secondary"
                              onClick={() => handleRemoveMember(row.userId)}
                              disabled={isActioning}
                              className="h-8 px-2 text-xs"
                            >
                              {isActioning ? <Loader2 className="h-3 w-3 animate-spin" /> : <UserMinus className="h-3 w-3" />}
                            </Button>
                          </>
                        )}
                      </div>
                    </div>
                  );
                })
              )}
            </div>

            {/* Pending invitations */}
            {canInvite && (
              <div className="space-y-2">
                <h3 className="text-sm font-semibold text-card-foreground">
                  Pending invitations ({invitations.length})
                </h3>
                {invitations.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No pending invitations</p>
                ) : (
                  invitations.map((inv) => (
                    <div key={inv.token} className="flex flex-col md:flex-row md:items-center md:justify-between gap-2 rounded-xl border border-border p-3">
                      <div>
                        <p className="text-sm font-medium text-card-foreground">{inv.email}</p>
                        <p className="text-xs text-muted-foreground">
                          Role: {inv.role} · Expires {formatDateRelative(inv.expires_at)}
                        </p>
                      </div>
                      <Button
                        variant="secondary"
                        onClick={() => handleRevokeInvitation(inv.token)}
                        className="h-8 px-3 text-xs"
                      >
                        Revoke
                      </Button>
                    </div>
                  ))
                )}
              </div>
            )}
          </CardContent>
        </Card>

        {/* ── Organisation ──────────────────────────────────── */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Building2 className="h-4 w-4" /> Organisation</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-3">
            <div>
              <p className="text-xs uppercase tracking-wide text-muted-foreground mb-1">Name</p>
              <Input readOnly value={tenant?.name || 'Not configured'} />
            </div>
            <div>
              <p className="text-xs uppercase tracking-wide text-muted-foreground mb-1">Tenant ID</p>
              <Input readOnly value={tenant?.tenantId || activeTenantId || 'Not configured'} />
            </div>
            <div>
              <p className="text-xs uppercase tracking-wide text-muted-foreground mb-1">Regions</p>
              <Input readOnly value={regionText} />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Plan & Limits</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Plan tier:</span>
              <Badge variant="primary">{tenant?.planTier || 'Not configured'}</Badge>
            </div>
            {tenant?.planLimits && typeof tenant.planLimits === 'object' ? (
              <div className="grid gap-2 md:grid-cols-2">
                {Object.entries(tenant.planLimits).map(([key, value]) => (
                  <div key={key} className="rounded-xl border border-border p-3">
                    <p className="text-xs uppercase tracking-wide text-muted-foreground">{key}</p>
                    <p className="text-sm text-card-foreground mt-1">{String(value)}</p>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">Not configured</p>
            )}
          </CardContent>
        </Card>

        {/* ── Connectors ────────────────────────────────────── */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><PlugZap className="h-4 w-4" /> Connectors</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {connectors.length === 0 ? (
              <p className="text-sm text-muted-foreground">No data yet</p>
            ) : (
              connectors.map((connector) => (
                <div key={connector.key} className="rounded-xl border border-border p-3 space-y-2">
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-sm font-semibold text-card-foreground">{connector.displayName}</p>
                    <StatusBadge status={connector.status} />
                  </div>
                  <div className="grid gap-2 md:grid-cols-2 text-xs text-muted-foreground">
                    <p>Last run: {formatDateRelative(connector.lastRunAt)}</p>
                    <p>Last success: {formatDateRelative(connector.lastSuccessAt)}</p>
                    <p>Schedule: {connector.schedule || 'Not configured'}</p>
                    <p>Last error: {connector.lastError || 'No data yet'}</p>
                  </div>
                </div>
              ))
            )}
          </CardContent>
        </Card>

        {/* ── Support Access ────────────────────────────────── */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Shield className="h-4 w-4" /> Support Access</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-muted-foreground">
              Audited, consent-based support access. Support access is time-limited and logged for compliance.
            </p>

            {supportLoading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading support access status...
              </div>
            ) : (
              <div className="rounded-xl border border-border p-4 space-y-2">
                <div className="flex items-center gap-2">
                  <span className="text-sm text-muted-foreground">Current status:</span>
                  <Badge variant={supportStatus?.enabled ? 'primary' : 'ghost'}>
                    {supportStatus?.enabled ? 'Enabled' : 'Disabled'}
                  </Badge>
                </div>
                <p className="text-xs text-muted-foreground">Expires: {formatDateTime(supportStatus?.expiresAt)}</p>
                <p className="text-xs text-muted-foreground">Enabled by: {supportStatus?.enabledBy || 'Not configured'}</p>
                <p className="text-xs text-muted-foreground">Reason: {supportStatus?.reason || 'Not configured'}</p>
                {supportStatus?.revokedAt && (
                  <p className="text-xs text-muted-foreground">Revoked: {formatDateTime(supportStatus?.revokedAt)}</p>
                )}
              </div>
            )}

            {!supportStatus?.enabled ? (
              <div className="rounded-xl border border-border p-4 space-y-3">
                <h3 className="text-sm font-semibold text-card-foreground">Enable support access</h3>
                <div className="grid gap-3 md:grid-cols-2">
                  <div>
                    <p className="text-xs uppercase tracking-wide text-muted-foreground mb-1">Duration</p>
                    <select
                      value={enableDurationMinutes}
                      onChange={(e) => setEnableDurationMinutes(e.target.value)}
                      className="h-10 w-full rounded-xl border border-input bg-background px-3 py-2 text-sm text-foreground"
                    >
                      <option value="15">15 minutes</option>
                      <option value="60">60 minutes</option>
                      <option value="240">240 minutes</option>
                    </select>
                  </div>
                  <div>
                    <p className="text-xs uppercase tracking-wide text-muted-foreground mb-1">Reason</p>
                    <Input
                      value={enableReason}
                      onChange={(e) => setEnableReason(e.target.value)}
                      placeholder="Reason for enabling support access"
                    />
                  </div>
                </div>
                <Button onClick={handleEnableSupportAccess} disabled={supportActionLoading === 'enable'}>
                  {supportActionLoading === 'enable' ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
                  Enable support access
                </Button>
              </div>
            ) : (
              <div className="rounded-xl border border-border p-4 space-y-3">
                <h3 className="text-sm font-semibold text-card-foreground">Revoke support access</h3>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted-foreground mb-1">Revoke reason</p>
                  <Input
                    value={revokeReason}
                    onChange={(e) => setRevokeReason(e.target.value)}
                    placeholder="Reason for revoking support access"
                  />
                </div>
                <Button variant="secondary" onClick={handleRevokeSupportAccess} disabled={supportActionLoading === 'revoke'}>
                  {supportActionLoading === 'revoke' ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
                  Revoke support access
                </Button>
              </div>
            )}

            {supportMessage && <p className="text-sm text-primary">{supportMessage}</p>}
            {supportError && <p className="text-sm text-destructive">{supportError}</p>}

            <div className="space-y-2">
              <h3 className="text-sm font-semibold text-card-foreground">Audit log</h3>
              {supportAudit.length === 0 ? (
                <p className="text-sm text-muted-foreground">No data yet</p>
              ) : (
                supportAudit.map((row) => (
                  <div key={row.id} className="rounded-xl border border-border p-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant="ghost">{row.action || 'UNKNOWN'}</Badge>
                      <span className="text-xs text-muted-foreground">{formatDateTime(row.created_at)}</span>
                    </div>
                    <p className="text-xs text-muted-foreground mt-1">Actor: {row.actor_email || row.actor_user_id || 'Unknown'}</p>
                    <p className="text-xs text-muted-foreground">Reason: {row.reason || 'Not configured'}</p>
                  </div>
                ))
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Shield className="h-4 w-4" /> Security</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <p className="text-sm text-card-foreground">SSO: Enterprise (planned)</p>
          </CardContent>
        </Card>
          </TabsContent>

          <TabsContent value="integrations" className="space-y-6">
            <div className="space-y-6 max-w-5xl">
              {[
                {
                  id: 'google-calendar',
                  name: 'Google Calendar',
                  description: 'Automatically add tender deadlines to your calendar',
                  icon: Calendar,
                  color: 'text-blue-200',
                  bgColor: 'bg-blue-500/15 border border-blue-400/40',
                  features: ['Add tender deadlines to calendar', 'Sync important dates automatically', 'Get calendar reminders'],
                  available: true
                },
                {
                  id: 'slack',
                  name: 'Slack',
                  description: 'Send alert notifications to Slack channels',
                  icon: MessageSquare,
                  color: 'text-violet-200',
                  bgColor: 'bg-violet-500/15 border border-violet-400/40',
                  features: ['Post tender alerts to channels', 'Get real-time notifications', 'Share tenders with team'],
                  available: true
                },
                {
                  id: 'google-drive',
                  name: 'Google Drive',
                  description: 'Export and save tender reports to your Drive',
                  icon: HardDrive,
                  color: 'text-emerald-200',
                  bgColor: 'bg-emerald-500/15 border border-emerald-400/40',
                  features: ['Export tender data to Drive', 'Save reports automatically', 'Organize tender documents'],
                  available: true
                }
              ].map((integration) => {
                const Icon = integration.icon;
                return (
                  <Card key={integration.id} className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                    <CardContent className="p-6">
                      <div className="flex items-start gap-4">
                        <div className={`p-4 rounded-xl ${integration.bgColor}`}>
                          <Icon className={`h-6 w-6 ${integration.color}`} />
                        </div>
                        <div className="flex-1">
                          <div className="flex items-center gap-3 mb-2">
                            <h3 className="text-lg font-semibold text-slate-100">{integration.name}</h3>
                            {integration.available ? (
                              <Badge className="bg-emerald-500/15 text-emerald-200 border border-emerald-400/40">Available</Badge>
                            ) : (
                              <Badge variant="ghost">Coming Soon</Badge>
                            )}
                          </div>
                          <p className="text-slate-300 mb-4">{integration.description}</p>
                          <ul className="space-y-2 mb-4">
                            {integration.features.map((feature) => (
                              <li key={feature} className="flex items-center gap-2 text-sm text-slate-300">
                                <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                                {feature}
                              </li>
                            ))}
                          </ul>
                          <p className="text-sm text-slate-200 bg-slate-900/70 border border-white/[0.06] rounded-lg p-3">
                            <strong className="text-primary">Admin Setup Required:</strong> Contact your administrator to authorize this integration for the organization.
                          </p>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}

              <Card className="border border-white/[0.06] bg-white/[0.02] shadow-none">
                <CardContent className="p-6">
                  <div className="flex gap-4">
                    <Settings className="h-5 w-5 text-primary mt-0.5" />
                    <div>
                      <h3 className="font-semibold text-slate-100 mb-2">How Integrations Work</h3>
                      <ul className="space-y-2 text-sm text-slate-300">
                        <li>Integrations are set up once by an admin for the entire organization</li>
                        <li>Once connected, integration features appear throughout the app</li>
                        <li>Look for "Add to Calendar", "Send to Slack", and "Export to Drive" buttons</li>
                        <li>All data is securely synced with your connected accounts</li>
                      </ul>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </PageBody>
    </Page>
  );
}
