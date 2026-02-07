import React, { useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { useAuth } from '@/lib/auth';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';

export default function Login() {
  const navigate = useNavigate();
  const { isAuthenticated, isLoadingAuth, authError, loginWithPassword } = useAuth();

  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

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
    const result = await loginWithPassword({ email: normalizedEmail, password });
    if (result.ok) {
      navigate('/', { replace: true });
    } else {
      setError(result.error || 'Login failed');
    }
  };

  return (
    <div className="min-h-screen bg-background text-foreground flex items-center justify-center p-6">
      <Card className="w-full max-w-md">
        <CardHeader>
          <CardTitle>Sign in to Civant</CardTitle>
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

            {(error || authError) && (
              <p className="text-sm text-destructive">{error || authError}</p>
            )}

            <Button type="submit" className="w-full" disabled={isLoadingAuth}>
              {isLoadingAuth ? 'Signing in...' : 'Log in'}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
