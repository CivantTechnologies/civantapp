-- User profile information for account personalization and tender preferences

create table if not exists user_profiles (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null default 'civant_default',
  user_id text not null,
  email text not null,
  first_name text,
  last_name text,
  birth_date date,
  phone_number text,
  country text,
  company text,
  industry text,
  job_title text,
  role_focus text,
  tender_interest_types text[] not null default '{}',
  procurement_regions text[] not null default '{}',
  cpv_interest_codes text[] not null default '{}',
  preferred_notice_types text[] not null default '{}',
  preferred_contract_types text[] not null default '{}',
  budget_range text,
  notification_frequency text,
  language text,
  timezone text,
  avatar_url text,
  linkedin_url text,
  website_url text,
  bio text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, user_id)
);

create index if not exists idx_user_profiles_tenant_id on user_profiles(tenant_id);
create index if not exists idx_user_profiles_user_id on user_profiles(user_id);
create index if not exists idx_user_profiles_email on user_profiles(email);
