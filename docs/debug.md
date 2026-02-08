# Runtime Config Debug

## Required environment variables

- `VITE_SUPABASE_URL`
- `VITE_SUPABASE_ANON_KEY`
- `VITE_CIVANT_APP_ID`

Optional:

- `VITE_API_BASE_URL` (defaults to `/api`)
- `VITE_DEBUG=true` (prints runtime config presence checks in browser console)

## Local run

1. Create `.env.local` with the variables above.
2. Run `npm run dev`.

## Validation with `VITE_DEBUG`

Set `VITE_DEBUG=true` and refresh. In browser console you should see logs like:

- `SUPABASE_URL present: true`
- `SUPABASE_ANON_KEY present: true (len=...)`
- `CIVANT_APP_ID present: true`

If required vars are missing, the app renders an in-app configuration error screen instead of a blank page.
