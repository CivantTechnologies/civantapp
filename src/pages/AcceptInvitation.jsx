import { useEffect, useState, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { civant } from '@/api/civantClient';
import { useAuth } from '@/lib/auth';
import { useTenant } from '@/lib/tenant';
import { Loader2, CheckCircle, AlertCircle, UserCheck } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, Button } from '@/components/ui';

const PENDING_TOKEN_KEY = 'pendingInviteToken';

function unwrapResponse(r) {
  return r?.data ?? r ?? null;
}

export default function AcceptInvitation() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { isAuthenticated, isLoadingAuth } = useAuth();
  const { refreshTenants, setActiveTenantId } = useTenant();

  const tokenFromUrl = searchParams.get('token') || '';

  const [status, setStatus] = useState('idle'); // idle | loading | success | error
  const [result, setResult] = useState(null);
  const [errorMessage, setErrorMessage] = useState('');

  // Store token in sessionStorage so it survives login redirect
  useEffect(() => {
    if (tokenFromUrl) {
      sessionStorage.setItem(PENDING_TOKEN_KEY, tokenFromUrl);
    }
  }, [tokenFromUrl]);

  const token = tokenFromUrl || sessionStorage.getItem(PENDING_TOKEN_KEY) || '';

  const accept = useCallback(async () => {
    if (!token) return;
    setStatus('loading');
    setErrorMessage('');
    try {
      const payload = unwrapResponse(await civant.system.acceptInvitation({ token }));
      sessionStorage.removeItem(PENDING_TOKEN_KEY);
      setResult(payload);
      setStatus('success');
      // Refresh tenant list so the new workspace appears in the switcher
      await refreshTenants();
      if (payload?.tenantId) {
        setActiveTenantId(payload.tenantId);
      }
    } catch (err) {
      setStatus('error');
      setErrorMessage(err?.message || 'Failed to accept the invitation.');
    }
  }, [token, refreshTenants, setActiveTenantId]);

  // Auto-accept when authenticated and token is present
  useEffect(() => {
    if (isLoadingAuth) return;
    if (!isAuthenticated) return;
    if (!token) return;
    if (status !== 'idle') return;
    accept();
  }, [isAuthenticated, isLoadingAuth, token, status, accept]);

  // ── No token ───────────────────────────────────────────────
  if (!token) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background p-4">
        <Card className="max-w-md w-full">
          <CardContent className="p-8 text-center space-y-4">
            <AlertCircle className="h-10 w-10 mx-auto text-destructive" />
            <h2 className="text-xl font-semibold text-card-foreground">Invalid invitation link</h2>
            <p className="text-sm text-muted-foreground">
              This invitation link is missing or malformed. Please ask the sender to share a new link.
            </p>
            <Button variant="secondary" onClick={() => navigate('/login')}>Go to login</Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  // ── Loading auth ───────────────────────────────────────────
  if (isLoadingAuth) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  // ── Not authenticated ──────────────────────────────────────
  if (!isAuthenticated) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background p-4">
        <Card className="max-w-md w-full">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <UserCheck className="h-5 w-5" /> You've been invited to Civant
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-muted-foreground">
              Sign in to your existing account or create a new one to accept this invitation.
              Your invite will complete automatically after you log in.
            </p>
            <div className="flex flex-col sm:flex-row gap-3">
              <Button onClick={() => navigate(`/login?returnTo=/invite`)} className="flex-1">
                Sign in
              </Button>
            </div>
            <p className="text-xs text-muted-foreground text-center">
              New to Civant? Contact your workspace admin to set up your account first.
            </p>
          </CardContent>
        </Card>
      </div>
    );
  }

  // ── Accepting (loading) ────────────────────────────────────
  if (status === 'loading' || status === 'idle') {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background p-4">
        <Card className="max-w-md w-full">
          <CardContent className="p-8 text-center space-y-4">
            <Loader2 className="h-10 w-10 mx-auto animate-spin text-primary" />
            <h2 className="text-xl font-semibold text-card-foreground">Accepting invitation…</h2>
            <p className="text-sm text-muted-foreground">Just a moment while we set up your access.</p>
          </CardContent>
        </Card>
      </div>
    );
  }

  // ── Success ────────────────────────────────────────────────
  if (status === 'success') {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background p-4">
        <Card className="max-w-md w-full">
          <CardContent className="p-8 text-center space-y-4">
            <CheckCircle className="h-10 w-10 mx-auto text-primary" />
            <h2 className="text-xl font-semibold text-card-foreground">
              Welcome to {result?.tenantName || 'your new workspace'}!
            </h2>
            <p className="text-sm text-muted-foreground">
              You've joined as <strong>{result?.role || 'member'}</strong>. You're all set.
            </p>
            <Button onClick={() => navigate('/')}>Go to dashboard</Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  // ── Error ──────────────────────────────────────────────────
  return (
    <div className="min-h-screen flex items-center justify-center bg-background p-4">
      <Card className="max-w-md w-full">
        <CardContent className="p-8 text-center space-y-4">
          <AlertCircle className="h-10 w-10 mx-auto text-destructive" />
          <h2 className="text-xl font-semibold text-card-foreground">Invitation error</h2>
          <p className="text-sm text-muted-foreground">{errorMessage}</p>
          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <Button variant="secondary" onClick={() => navigate('/')}>Go to dashboard</Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
