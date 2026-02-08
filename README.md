**Welcome to your Civant project** 

**About**

View and Edit  your app on [Civant.com](http://Civant.com) 

This project contains everything you need to run your app locally.

**Edit the code in your local development environment**

Any change pushed to the repo will also be reflected in the Civant Builder.

**Prerequisites:** 

1. Clone the repository using the project's Git URL 
2. Navigate to the project directory
3. Install dependencies: `npm install`
4. Create an `.env.local` file and set the right environment variables

```
VITE_CIVANT_APP_ID=your_app_id
VITE_CIVANT_APP_BASE_URL=your_backend_url

e.g.
VITE_CIVANT_APP_ID=cbef744a8545c389ef439ea6
VITE_CIVANT_APP_BASE_URL=https://my-to-do-list-81bfaad7.civant.app
```

Run the app: `npm run dev`

**Publish your changes**

Open [Civant.com](http://Civant.com) and click on Publish.

**Docs & Support**

Documentation: [https://docs.civant.com/Integrations/Using-GitHub](https://docs.civant.com/Integrations/Using-GitHub)

Support: [https://app.civant.com/support](https://app.civant.com/support)

## Deployment Debug Checklist (Vercel)

Use this quick checklist before escalating production issues.

1. Hard refresh first (`Cmd+Shift+R`) after each new deployment.
2. Test in an Incognito window to rule out extension interference.
3. If console errors start with `chrome-extension://`, they are browser extension errors, not Civant app errors.
4. Confirm required frontend env vars are set in Vercel for the deployed environment:
   - `VITE_SUPABASE_URL`
   - `VITE_SUPABASE_ANON_KEY`
   - `VITE_CIVANT_APP_ID`
   - `VITE_API_BASE_URL` (usually `/api`)
5. Confirm production routes load:
   - `/` should return app HTML
   - `/login` should return app HTML (SPA fallback), not Vercel `NOT_FOUND`
6. In DevTools Network, filter for failed requests and inspect the exact URL:
   - Treat first-party failures (`https://civantapp.vercel.app/...`) as app issues
   - Ignore third-party extension URLs when validating app health
