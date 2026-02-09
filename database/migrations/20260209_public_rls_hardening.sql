-- Enable and standardize RLS for public tables exposed through PostgREST.
-- Idempotent: safe to re-run.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
      AND p.proname = 'current_tenant_id'
      AND pg_get_function_identity_arguments(p.oid) = ''
  ) THEN
    EXECUTE $fn$
      CREATE FUNCTION public.current_tenant_id()
      RETURNS text
      LANGUAGE plpgsql
      STABLE
      AS $body$
      DECLARE
        claims_json jsonb := '{}'::jsonb;
        headers_json jsonb := '{}'::jsonb;
        tenant text;
      BEGIN
        BEGIN
          claims_json := COALESCE(NULLIF(current_setting('request.jwt.claims', true), '')::jsonb, '{}'::jsonb);
        EXCEPTION WHEN OTHERS THEN
          claims_json := '{}'::jsonb;
        END;

        BEGIN
          headers_json := COALESCE(NULLIF(current_setting('request.headers', true), '')::jsonb, '{}'::jsonb);
        EXCEPTION WHEN OTHERS THEN
          headers_json := '{}'::jsonb;
        END;

        tenant := COALESCE(
          NULLIF(current_setting('request.jwt.claim.tenant_id', true), ''),
          NULLIF(claims_json ->> 'tenant_id', ''),
          NULLIF(claims_json -> 'app_metadata' ->> 'tenant_id', ''),
          NULLIF(headers_json ->> 'x-tenant-id', ''),
          NULLIF(headers_json ->> 'X-Tenant-Id', '')
        );

        IF tenant IS NULL OR tenant !~ '^[a-z0-9_]{3,40}$' THEN
          RETURN NULL;
        END IF;

        RETURN lower(tenant);
      END;
      $body$;
    $fn$;
  END IF;
END $$;

DO $$
DECLARE
  table_name text;
  policy_name text;
BEGIN
  FOR table_name, policy_name IN
    SELECT * FROM (VALUES
      ('reconciliation_queue', 'reconciliation_queue_tenant_select'),
      ('tender_features_weekly', 'tender_features_weekly_tenant_select'),
      ('users', 'users_tenant_select'),
      ('ingestion_runs', 'ingestion_runs_tenant_select'),
      ('Competitors', 'competitors_tenant_select'),
      ('Alerts', 'alerts_tenant_select'),
      ('AlertEvents', 'alertevents_tenant_select'),
      ('user_profiles', 'user_profiles_tenant_select')
    ) AS t(table_name, policy_name)
  LOOP
    IF to_regclass(format('public.%I', table_name)) IS NOT NULL THEN
      EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', table_name);
      EXECUTE format('ALTER TABLE public.%I FORCE ROW LEVEL SECURITY', table_name);

      IF NOT EXISTS (
        SELECT 1
        FROM pg_policies
        WHERE schemaname = 'public'
          AND tablename = table_name
          AND policyname = policy_name
      ) THEN
        EXECUTE format(
          'CREATE POLICY %I ON public.%I FOR SELECT TO authenticated USING (tenant_id = current_tenant_id())',
          policy_name,
          table_name
        );
      END IF;
    END IF;
  END LOOP;
END $$;

DO $$
BEGIN
  IF to_regclass('public.tenants') IS NOT NULL THEN
    ALTER TABLE public.tenants ENABLE ROW LEVEL SECURITY;
    ALTER TABLE public.tenants FORCE ROW LEVEL SECURITY;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_policies
      WHERE schemaname = 'public'
        AND tablename = 'tenants'
        AND policyname = 'tenants_tenant_select'
    ) THEN
      CREATE POLICY tenants_tenant_select
      ON public.tenants
      FOR SELECT
      TO authenticated
      USING (id = current_tenant_id());
    END IF;
  END IF;
END $$;

DO $$
BEGIN
  IF to_regclass('public.user_roles') IS NOT NULL THEN
    ALTER TABLE public.user_roles ENABLE ROW LEVEL SECURITY;
    ALTER TABLE public.user_roles FORCE ROW LEVEL SECURITY;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_policies
      WHERE schemaname = 'public'
        AND tablename = 'user_roles'
        AND policyname = 'user_roles_tenant_select'
    ) THEN
      CREATE POLICY user_roles_tenant_select
      ON public.user_roles
      FOR SELECT
      TO authenticated
      USING (
        EXISTS (
          SELECT 1
          FROM public.users u
          WHERE u.id = user_roles.user_id
            AND u.tenant_id = current_tenant_id()
        )
      );
    END IF;
  END IF;
END $$;
