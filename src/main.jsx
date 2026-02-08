import React from 'react';
import ReactDOM from 'react-dom/client';
import App from '@/App.jsx';
import '@/index.css';
import { AppErrorBoundary } from '@/components/AppErrorBoundary';
import { logRuntimeConfigStatus, runtimeConfig } from '@/config';

function ConfigErrorScreen() {
  return (
    <div className="min-h-screen bg-background text-foreground flex items-center justify-center px-6">
      <div className="w-full max-w-2xl rounded-2xl border border-border bg-card p-8 space-y-4">
        <h1 className="text-2xl font-semibold text-card-foreground">Civant configuration is incomplete</h1>
        <p className="text-sm text-muted-foreground">
          Required runtime variables are missing. Update your deployment environment and redeploy.
        </p>
        <div className="rounded-xl border border-border bg-muted/40 p-4">
          <p className="text-xs uppercase tracking-wide text-muted-foreground mb-2">Missing variables</p>
          <ul className="list-disc pl-5 text-sm text-card-foreground space-y-1">
            {runtimeConfig.missingRequired.map((key) => (
              <li key={key}>{key}</li>
            ))}
          </ul>
        </div>
        <p className="text-xs text-muted-foreground">
          Required: <code>VITE_SUPABASE_URL</code>, <code>VITE_SUPABASE_ANON_KEY</code>, <code>VITE_CIVANT_APP_ID</code>.
        </p>
      </div>
    </div>
  );
}

logRuntimeConfigStatus();

ReactDOM.createRoot(document.getElementById('root')).render(
  <AppErrorBoundary>
    {runtimeConfig.isValid ? <App /> : <ConfigErrorScreen />}
  </AppErrorBoundary>
);
