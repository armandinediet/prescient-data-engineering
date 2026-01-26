create schema if not exists ops;

create table if not exists ops.scheduler_runs (
  job_key        text not null,
  scheduled_for  timestamptz not null,
  started_at     timestamptz not null default now(),
  finished_at    timestamptz,
  status         text not null default 'running', -- running|success|failed|skipped
  exit_code      int,
  command        text,
  error          text,
  primary key (job_key, scheduled_for)
);
