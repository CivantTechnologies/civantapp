import { MutationCache, QueryCache, QueryClient } from '@tanstack/react-query';
import { sendClientTelemetry } from '@/lib/client-telemetry';

function errorMessage(error) {
  if (error instanceof Error) return error.message;
  return String(error || 'Unknown query error');
}

export const queryClientInstance = new QueryClient({
	queryCache: new QueryCache({
		onError: (error, query) => {
			sendClientTelemetry({
				event_type: 'query_error',
				severity: 'error',
				path: typeof window !== 'undefined' ? window.location.pathname : '',
				message: `${String(query?.queryHash || 'unknown_query')} :: ${errorMessage(error)}`,
				stack: error instanceof Error ? error.stack || '' : ''
			});
		}
	}),
	mutationCache: new MutationCache({
		onError: (error, _variables, _context, mutation) => {
			sendClientTelemetry({
				event_type: 'mutation_error',
				severity: 'error',
				path: typeof window !== 'undefined' ? window.location.pathname : '',
				message: `${String(mutation?.options?.mutationKey || 'unknown_mutation')} :: ${errorMessage(error)}`,
				stack: error instanceof Error ? error.stack || '' : ''
			});
		}
	}),
	defaultOptions: {
		queries: {
			refetchOnWindowFocus: false,
			retry: 1,
		},
	},
});
