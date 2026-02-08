import React from 'react';

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
    // eslint-disable-next-line no-console
    console.error('[Civant] Unhandled UI error:', error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-background text-foreground flex items-center justify-center px-6">
          <div className="max-w-xl rounded-2xl border border-border bg-card p-8 space-y-3">
            <h1 className="text-2xl font-semibold text-card-foreground">Something went wrong</h1>
            <p className="text-sm text-muted-foreground">
              The app hit an unexpected error. Refresh the page, and if it continues, contact support with this message.
            </p>
            <pre className="text-xs rounded-xl border border-border bg-muted/40 p-3 overflow-auto">{this.state.errorMessage}</pre>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
