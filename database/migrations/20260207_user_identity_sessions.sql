-- User identity and session foundations

alter table if exists users
  add column if not exists role text not null default 'user';

create table if not exists sessions (
  id text primary key,
  user_id text not null references users(id) on delete cascade,
  token text not null unique,
  expires_at timestamptz not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_sessions_user_id on sessions(user_id);
create index if not exists idx_sessions_expires_at on sessions(expires_at);
