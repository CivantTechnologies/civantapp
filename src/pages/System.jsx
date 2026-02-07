import React, { useEffect, useMemo, useState } from 'react';
import { civant } from '@/api/civantClient';
import { AlertCircle, Loader2, RefreshCw, Shield, Building2, Users, PlugZap } from 'lucide-react';
import { Page, PageHeader, PageTitle, PageDescription, PageBody, Card, CardHeader, CardTitle, CardContent, Badge, Button, Input } from '@/components/ui';
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

function StatusBadge({ status }) {
  const normalized = String(status || 'no_data').toLowerCase();
  if (normalized === 'success') return <Badge variant="primary">Healthy</Badge>;
  if (normalized === 'partial') return <Badge variant="secondary">Partial</Badge>;
  if (normalized === 'fail') return <Badge variant="destructive">Failed</Badge>;
  if (normalized === 'disabled') return <Badge variant="ghost">Disabled</Badge>;
  return <Badge variant="ghost">No data yet</Badge>;
}

export default function System() {
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [denied, setDenied] = useState(false);
  const [error, setError] = useState('');

  const [tenant, setTenant] = useState(null);
  const [tenantUsers, setTenantUsers] = useState([]);
  const [connectors, setConnectors] = useState([]);

  const loadSystemData = async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    setError('');

    try {
      const tenantPayload = unwrapResponse(await civant.system.getTenant());
      const resolvedTenantId = tenantPayload?.tenantId || 'civant_default';
      setTenant(tenantPayload || null);

      const [usersPayload, connectorsPayload] = await Promise.all([
        civant.system.listTenantUsers(resolvedTenantId),
        civant.system.getConnectorStatus(resolvedTenantId)
      ]);

      setTenantUsers(Array.isArray(unwrapResponse(usersPayload)) ? unwrapResponse(usersPayload) : []);
      setConnectors(Array.isArray(unwrapResponse(connectorsPayload)) ? unwrapResponse(connectorsPayload) : []);
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
    loadSystemData(false);
  }, []);

  const regionText = useMemo(() => {
    if (!tenant?.regions || !tenant.regions.length) return 'Not configured';
    return tenant.regions.join(', ');
  }, [tenant]);

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
            <p className="text-muted-foreground">You don’t have access to System settings.</p>
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
          <Button variant="secondary" onClick={() => loadSystemData(true)} disabled={refreshing}>
            {refreshing ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-2" />}
            Refresh
          </Button>
        </div>
      </PageHeader>

      <PageBody>
        {error && (
          <Card>
            <CardContent className="p-4">
              <p className="text-sm text-destructive">{error}</p>
            </CardContent>
          </Card>
        )}

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
              <Input readOnly value={tenant?.tenantId || 'civant_default'} />
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

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Users className="h-4 w-4" /> Users & Roles</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {tenantUsers.length === 0 ? (
              <p className="text-sm text-muted-foreground">No data yet</p>
            ) : (
              tenantUsers.map((row) => (
                <div key={row.userId} className="flex flex-col md:flex-row md:items-center md:justify-between gap-2 rounded-xl border border-border p-3">
                  <div>
                    <p className="text-sm font-medium text-card-foreground">{row.email || 'Not configured'}</p>
                    <p className="text-xs text-muted-foreground">{row.userId}</p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {(row.roles || []).length === 0 ? (
                      <Badge variant="ghost">No role</Badge>
                    ) : (
                      row.roles.map((role) => <Badge key={`${row.userId}-${role}`} variant={role === 'admin' ? 'primary' : 'secondary'}>{role}</Badge>)
                    )}
                  </div>
                </div>
              ))
            )}
          </CardContent>
        </Card>

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

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Shield className="h-4 w-4" /> Security</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <p className="text-sm text-card-foreground">Support access: audited & consent-based (planned)</p>
            <p className="text-sm text-card-foreground">SSO: Enterprise (planned)</p>
          </CardContent>
        </Card>
      </PageBody>
    </Page>
  );
}
