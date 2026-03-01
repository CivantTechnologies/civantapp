import React from 'react';
import { sendClientTelemetry } from '@/lib/client-telemetry';

export class AppErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, errorMessage: '' };
  }

  static getDerivedStateFromError(error) {
    return {
      hasError: true,
      errorMessage: error?.message || 'Unexpected application error'
    };
  }

  componentDidCatch(error, info) {
    console.error('[Civant] Unhandled UI error:', error, info);
    sendClientTelemetry({
      event_type: 'ui_error',
      severity: 'error',
      path: typeof window !== 'undefined' ? window.location.pathname : '',
      message: error instanceof Error ? error.message : 'Unhandled UI error',
      stack: error instanceof Error ? error.stack || '' : '',
      context: {
        componentStack: info?.componentStack || ''
      }
    });
  }

  handleRetry = () => {
    this.setState({ hasError: false, errorMessage: '' });
  };

  handleResetWorkspace = () => {
    if (typeof window !== 'undefined' && window.localStorage) {
      window.localStorage.setItem('civant_active_tenant', 'civant_default');
    }
    window.location.href = '/';
  };

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-background text-foreground flex items-center justify-center px-6">
          <div className="max-w-xl rounded-2xl border border-border bg-card p-8 space-y-4">
            <h1 className="text-2xl font-semibold text-card-foreground">Something went wrong</h1>
            <p className="text-sm text-muted-foreground">
              The app hit an unexpected error. Try refreshing, or switch back to the default workspace.
            </p>
            <pre className="text-xs rounded-xl border border-border bg-muted/40 p-3 overflow-auto max-h-32">{this.state.errorMessage}</pre>
            <div className="flex gap-3 pt-2">
              <button
                onClick={this.handleRetry}
                className="px-4 py-2 text-sm font-medium rounded-lg bg-primary text-primary-foreground hover:opacity-90 transition"
              >
                Try Again
              </button>
              <button
                onClick={this.handleResetWorkspace}
                className="px-4 py-2 text-sm font-medium rounded-lg border border-border text-muted-foreground hover:bg-muted/40 transition"
              >
                Reset to Default Workspace
              </button>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
