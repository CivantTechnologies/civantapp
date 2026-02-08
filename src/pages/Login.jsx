import React, { useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { useAuth } from '@/lib/auth';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';

export default function Login() {
  const navigate = useNavigate();
  const {
    isAuthenticated,
    isLoadingAuth,
    authError,
    loginWithPassword,
    requestPasswordReset
  } = useAuth();

  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [isResetting, setIsResetting] = useState(false);

  if (isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  const onSubmit = async (event) => {
    event.preventDefault();
    const normalizedEmail = email.trim().toLowerCase();

    if (!normalizedEmail || !password) {
      setError('Email and password are required.');
      return;
    }

    setError('');
    setNotice('');
    const result = await loginWithPassword({ email: normalizedEmail, password });
    if (result.ok) {
      navigate('/', { replace: true });
    } else {
      setError(result.error || 'Login failed');
    }
  };

  const onForgotPassword = async () => {
    const normalizedEmail = email.trim().toLowerCase();
    if (!normalizedEmail) {
      setError('Enter your email first, then click Forgot my password.');
      setNotice('');
      return;
    }

    setIsResetting(true);
    setError('');
    setNotice('');
    const result = await requestPasswordReset(normalizedEmail);
    if (result.ok) {
      setNotice('If this email exists, a password reset link has been sent.');
    } else {
      setError(result.error || 'Unable to send reset email.');
    }
    setIsResetting(false);
  };

  return (
    <div className="min-h-screen bg-background text-foreground flex items-center justify-center p-6">
      <Card className="w-full max-w-md">
        <CardHeader>
          <div className="mb-2 flex justify-center">
            <img src="/civant-logo.jpg" alt="Civant logo" className="h-auto w-52 rounded-md border border-border/60" />
          </div>
          <CardTitle>Log in</CardTitle>
        </CardHeader>
        <CardContent>
          <form className="space-y-4" onSubmit={onSubmit}>
            <div className="space-y-1">
              <label htmlFor="email" className="text-sm text-muted-foreground">Email</label>
              <Input
                id="email"
                type="email"
                autoComplete="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                placeholder="you@company.com"
                disabled={isLoadingAuth}
              />
            </div>

            <div className="space-y-1">
              <label htmlFor="password" className="text-sm text-muted-foreground">Password</label>
              <Input
                id="password"
                type="password"
                autoComplete="current-password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                placeholder="Your password"
                disabled={isLoadingAuth}
              />
            </div>

            {(error || authError) && <p className="text-sm text-destructive">{error || authError}</p>}
            {notice && <p className="text-sm text-emerald-400">{notice}</p>}

            <Button
              type="button"
              variant="ghost"
              className="w-full justify-center text-sm text-muted-foreground hover:text-card-foreground"
              onClick={onForgotPassword}
              disabled={isLoadingAuth || isResetting}
            >
              {isResetting ? 'Sending reset link...' : 'Forgot my password?'}
            </Button>

            <Button type="submit" className="w-full" disabled={isLoadingAuth}>
              {isLoadingAuth ? 'Signing in...' : 'Log in'}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
