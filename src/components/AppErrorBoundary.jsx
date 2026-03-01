import React from 'react';
import { sendClientTelemetry } from '@/lib/client-telemetry';

function ErrorScreen({ errorMessage, errorRef, onReset }) {
  const [showDetails, setShowDetails] = React.useState(false);

  return (
    <div className="min-h-screen bg-background text-foreground flex items-center justify-center px-6">
      <div className="w-full max-w-lg rounded-2xl border border-white/[0.06] bg-white/[0.015] p-8 space-y-5">

        {/* Icon + heading */}
        <div className="space-y-2">
          <div className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-red-500/10 border border-red-500/20">
            <svg className="w-5 h-5 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round"
                d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z" />
            </svg>
          </div>
          <h1 className="text-xl font-semibold text-card-foreground">Something went wrong</h1>
          <p className="text-sm text-muted-foreground leading-relaxed">
            The app hit an unexpected error. Your data is safe — nothing was lost. Try recovering below, or reload the page if the problem persists.
          </p>
        </div>

        {/* Recovery actions */}
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={onReset}
            className="inline-flex items-center gap-1.5 rounded-lg bg-civant-teal px-4 py-2 text-sm font-medium text-white hover:bg-civant-teal/90 transition-colors"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99" />
            </svg>
            Try again
          </button>
          <button
            type="button"
            onClick={() => window.location.reload()}
            className="inline-flex items-center gap-1.5 rounded-lg border border-white/[0.08] px-4 py-2 text-sm font-medium text-card-foreground hover:bg-white/[0.04] transition-colors"
          >
            Reload page
          </button>
          <a
            href="/"
            className="inline-flex items-center gap-1.5 rounded-lg border border-white/[0.08] px-4 py-2 text-sm font-medium text-muted-foreground hover:bg-white/[0.04] transition-colors"
          >
            Go home
          </a>
        </div>

        {/* Collapsible technical details */}
        <div className="border-t border-white/[0.04] pt-4 space-y-2">
          <button
            type="button"
            onClick={() => setShowDetails((v) => !v)}
            className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-card-foreground transition-colors"
          >
            <svg
              className={`w-3 h-3 transition-transform ${showDetails ? 'rotate-90' : ''}`}
              fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="m8.25 4.5 7.5 7.5-7.5 7.5" />
            </svg>
            Technical details
          </button>

          {showDetails && (
            <pre className="text-xs rounded-xl border border-white/[0.06] bg-black/20 p-3 overflow-auto max-h-40 text-red-300 whitespace-pre-wrap break-all">
              {errorMessage || 'No details available'}
            </pre>
          )}

          {errorRef && (
            <p className="text-[11px] text-muted-foreground">
              Reference: <span className="font-mono text-slate-400">{errorRef}</span>
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

export class AppErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, errorMessage: '', errorRef: null };
    this.handleReset = this.handleReset.bind(this);
  }

  static getDerivedStateFromError(error) {
    const ts = Date.now().toString(36).toUpperCase();
    return {
      hasError: true,
      errorMessage: error?.message || 'Unexpected application error',
      errorRef: `ERR-${ts}`
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
        componentStack: info?.componentStack || '',
        errorRef: this.state.errorRef
      }
    });
  }

  handleReset() {
    this.setState({ hasError: false, errorMessage: '', errorRef: null });
  }

  render() {
    if (this.state.hasError) {
      return (
        <ErrorScreen
          errorMessage={this.state.errorMessage}
          errorRef={this.state.errorRef}
          onReset={this.handleReset}
        />
      );
    }
    return this.props.children;
  }
}
