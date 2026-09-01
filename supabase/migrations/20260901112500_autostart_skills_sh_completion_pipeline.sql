begin;

create table if not exists skillset.skills_sh_completion_control_v1 (
  singleton boolean primary key default true check (singleton),
  enabled boolean not null default true,
  status text not null default 'running'
    check (status in ('running','blocked','completed')),
  phase text not null default 'archive_verify'
    check (phase in ('archive_verify','projection','evaluation','integrity','completed','blocked')),
  projection_rescue_rounds integer not null default 0,
  detail jsonb not null default '{}'::jsonb,
  last_error text,
  started_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz
);

alter table skillset.skills_sh_completion_control_v1 enable row level security;
revoke all on table skillset.skills_sh_completion_control_v1 from public, anon, authenticated;
grant select, insert, update on table skillset.skills_sh_completion_control_v1 to service_role;

drop policy if exists skills_sh_completion_service_role_v1
  on skillset.skills_sh_completion_control_v1;
create policy skills_sh_completion_service_role_v1
  on skillset.skills_sh_completion_control_v1
  for all
  to service_role
  using (true)
  with check (true);

insert into skillset.skills_sh_completion_control_v1(singleton)
values (true)
on conflict (singleton) do nothing;

-- A manifest download failure previously wrote total_repos=0. That made the
-- failed bucket look empty and therefore unclaimable. Preserve unknown totals
-- until a manifest is actually downloaded.
create or replace function public.skillset_skills_sh_novel_archive_finish_v1(
  p_bucket_id integer,
  p_total_repos integer,
  p_owner text,
  p_repo text,
  p_outcome text,
  p_discovered integer default 0,
  p_inserted integer default 0,
  p_existing integer default 0,
  p_conflicts integer default 0,
  p_error text default null
)
returns void
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
declare
  v_terminal boolean;
  v_attempts integer;
  v_next integer;
  v_known_total integer;
begin
  if p_outcome = 'empty' then
    update skillset.skills_sh_novel_archive_queue_v1
    set total_repos = 0,
        status = 'done',
        claimed_at = null,
        current_attempts = 0,
        last_error = null,
        updated_at = now()
    where bucket_id = p_bucket_id;
    return;
  end if;

  v_terminal := p_outcome in ('done','not_found','oversize');

  select current_attempts, next_index, total_repos
  into v_attempts, v_next, v_known_total
  from skillset.skills_sh_novel_archive_queue_v1
  where bucket_id = p_bucket_id
  for update;

  if v_terminal then
    update skillset.skills_sh_novel_archive_queue_v1
    set total_repos = p_total_repos,
        next_index = v_next + 1,
        current_attempts = 0,
        processed_repos = processed_repos + 1,
        failed_repos = failed_repos + case when p_outcome = 'done' then 0 else 1 end,
        not_found_repos = not_found_repos + case when p_outcome = 'not_found' then 1 else 0 end,
        oversize_repos = oversize_repos + case when p_outcome = 'oversize' then 1 else 0 end,
        discovered_skills = discovered_skills + coalesce(p_discovered, 0),
        inserted_content = inserted_content + coalesce(p_inserted, 0),
        compatible_existing = compatible_existing + coalesce(p_existing, 0),
        conflicts = conflicts + coalesce(p_conflicts, 0),
        last_owner = p_owner,
        last_repo = p_repo,
        last_error = p_error,
        claimed_at = null,
        status = case when v_next + 1 >= p_total_repos then 'done' else 'active' end,
        updated_at = now()
    where bucket_id = p_bucket_id;
    return;
  end if;

  -- A repository-level failure has a known manifest and advances only after
  -- three attempts. A manifest-level failure never invents a zero total and
  -- remains reclaimable for the controller.
  v_attempts := v_attempts + 1;
  if p_owner is null and p_repo is null then
    update skillset.skills_sh_novel_archive_queue_v1
    set total_repos = v_known_total,
        current_attempts = least(v_attempts, 5),
        last_error = p_error,
        claimed_at = null,
        status = 'error',
        updated_at = now()
    where bucket_id = p_bucket_id;
  elsif v_attempts >= 3 then
    update skillset.skills_sh_novel_archive_queue_v1
    set total_repos = coalesce(v_known_total, nullif(p_total_repos, 0)),
        next_index = v_next + 1,
        current_attempts = 0,
        processed_repos = processed_repos + 1,
        failed_repos = failed_repos + 1,
        last_owner = p_owner,
        last_repo = p_repo,
        last_error = p_error,
        claimed_at = null,
        status = case
          when v_known_total is not null and v_next + 1 >= v_known_total then 'done'
          else 'active'
        end,
        updated_at = now()
    where bucket_id = p_bucket_id;
  else
    update skillset.skills_sh_novel_archive_queue_v1
    set total_repos = coalesce(v_known_total, nullif(p_total_repos, 0)),
        current_attempts = v_attempts,
        last_owner = p_owner,
        last_repo = p_repo,
        last_error = p_error,
        claimed_at = null,
        status = 'error',
        updated_at = now()
    where bucket_id = p_bucket_id;
  end if;
end
$function$;

-- The claim worker permits four attempts; the tick controller must use the
-- same threshold or the fourth retry can never run.
create or replace function public.skillset_sdlc_projection_v5_tick_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset', 'net'
as $function$
declare
  v_pending integer := 0;
  v_processing integer := 0;
  v_http integer := 0;
  v_slots integer := 0;
  v_started integer := 0;
  i integer;
  rid bigint;
begin
  update skillset.sdlc_projection_v5_queue
  set status = 'pending', claimed_at = null, updated_at = now()
  where status = 'processing'
    and claimed_at < now() - interval '3 minutes';

  select
    count(*) filter (where status in ('pending','error') and attempts < 4),
    count(*) filter (where status = 'processing')
  into v_pending, v_processing
  from skillset.sdlc_projection_v5_queue;

  select count(*) into v_http
  from net.http_request_queue
  where url like '%/skillset-sdlc-project-v5';

  if v_pending = 0 then
    update skillset.sdlc_projection_v5_control
    set run_tokens = 0, updated_at = now()
    where singleton = true;
    return jsonb_build_object(
      'pending', 0, 'processing', v_processing,
      'http_queued', v_http, 'started', 0
    );
  end if;

  if v_processing > 0 or v_http > 0 then
    return jsonb_build_object(
      'pending', v_pending, 'processing', v_processing,
      'http_queued', v_http, 'started', 0
    );
  end if;

  v_slots := least(2, ceil(v_pending / 500.0)::integer);
  update skillset.sdlc_projection_v5_control
  set enabled = true, run_tokens = v_slots, max_batch = 100, updated_at = now()
  where singleton = true;

  for i in 1..v_slots loop
    select net.http_post(
      url := 'https://cxvvfgwdqgxczxmomztw.supabase.co/functions/v1/skillset-sdlc-project-v5',
      headers := '{"Content-Type":"application/json"}'::jsonb,
      body := '{}'::jsonb,
      timeout_milliseconds := 120000
    ) into rid;
    v_started := v_started + 1;
  end loop;

  return jsonb_build_object(
    'pending', v_pending, 'processing', v_processing,
    'http_queued', v_http, 'slots', v_slots, 'started', v_started
  );
end
$function$;

create or replace function public.skillset_skills_sh_completion_tick_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset', 'net', 'cron'
as $function$
declare
  v_control skillset.skills_sh_completion_control_v1%rowtype;
  v_storage_open integer := 0;
  v_novel_open integer := 0;
  v_novel_processing integer := 0;
  v_projection_pending integer := 0;
  v_projection_processing integer := 0;
  v_projection_terminal integer := 0;
  v_projection_http integer := 0;
  v_running_eval integer := 0;
  v_failed_eval integer := 0;
  v_eval_needed integer := 0;
  v_rid bigint;
  v_state jsonb := '{}'::jsonb;
  v_run uuid;
  i integer;
begin
  if not pg_try_advisory_xact_lock(hashtextextended('skills-sh-completion-tick-v1', 0)) then
    return jsonb_build_object('started', 0, 'skipped', 'tick_locked');
  end if;

  select * into v_control
  from skillset.skills_sh_completion_control_v1
  where singleton = true
  for update;

  if not v_control.enabled then
    return jsonb_build_object('status', v_control.status, 'phase', v_control.phase, 'enabled', false);
  end if;

  -- The exact-storage pass is authoritative for bucket coverage. If it fully
  -- completed a bucket, a redundant novel-pass manifest error is safely closed.
  update skillset.skills_sh_novel_archive_queue_v1 n
  set status = 'done',
      total_repos = s.total_repos,
      next_index = s.total_repos,
      current_attempts = 0,
      claimed_at = null,
      last_error = concat_ws(' | ', nullif(n.last_error, ''), 'covered_by_exact_storage_archive'),
      updated_at = now()
  from skillset.skills_sh_storage_archive_queue_v1 s
  where n.bucket_id = s.bucket_id
    and n.status = 'error'
    and s.status = 'done'
    and s.total_repos is not null
    and s.next_index >= s.total_repos;

  select count(*) into v_storage_open
  from skillset.skills_sh_storage_archive_queue_v1
  where status in ('pending','active','error','processing')
    and (total_repos is null or next_index < total_repos);

  select
    count(*) filter (where status in ('pending','active','error') and (total_repos is null or next_index < total_repos)),
    count(*) filter (where status = 'processing')
  into v_novel_open, v_novel_processing
  from skillset.skills_sh_novel_archive_queue_v1;

  if v_storage_open > 0 or v_novel_open > 0 or v_novel_processing > 0 then
    update skillset.skills_sh_completion_control_v1
    set phase = 'archive_verify', status = 'running',
        detail = jsonb_build_object(
          'storage_open', v_storage_open,
          'novel_open', v_novel_open,
          'novel_processing', v_novel_processing
        ),
        updated_at = now()
    where singleton = true;

    if v_novel_open > 0 or v_novel_processing > 0 then
      v_state := public.skillset_skills_sh_novel_archive_tick_v1();
    end if;

    if v_storage_open > 0 then
      for i in 1..least(3, v_storage_open) loop
        select net.http_post(
          url := 'https://cxvvfgwdqgxczxmomztw.supabase.co/functions/v1/skills-sh-storage-archive-v1',
          headers := '{"Content-Type":"application/json"}'::jsonb,
          body := '{}'::jsonb,
          timeout_milliseconds := 120000
        ) into v_rid;
      end loop;
    end if;

    return jsonb_build_object('phase','archive_verify','state',v_state);
  end if;

  select
    count(*) filter (where status in ('pending','error') and attempts < 4),
    count(*) filter (where status = 'processing'),
    count(*) filter (where status = 'error' and attempts >= 4)
  into v_projection_pending, v_projection_processing, v_projection_terminal
  from skillset.sdlc_projection_v5_queue;

  select count(*) into v_projection_http
  from net.http_request_queue
  where url like '%/skillset-sdlc-project-v5';

  if v_projection_terminal > 0 then
    if v_control.projection_rescue_rounds < 1 then
      update skillset.sdlc_projection_v5_queue
      set status = 'pending', attempts = 0, claimed_at = null,
          error = concat_ws(' | ', nullif(error, ''), 'autopilot_bounded_rescue'),
          updated_at = now()
      where status = 'error' and attempts >= 4;

      update skillset.skills_sh_completion_control_v1
      set phase = 'projection', status = 'running',
          projection_rescue_rounds = projection_rescue_rounds + 1,
          detail = jsonb_build_object('projection_rescued', v_projection_terminal),
          updated_at = now()
      where singleton = true;
      return jsonb_build_object('phase','projection','rescued',v_projection_terminal);
    end if;

    update skillset.skills_sh_completion_control_v1
    set phase = 'blocked', status = 'blocked',
        last_error = 'projection_terminal_errors',
        detail = jsonb_build_object('projection_terminal', v_projection_terminal),
        updated_at = now()
    where singleton = true;
    return jsonb_build_object('phase','blocked','projection_terminal',v_projection_terminal);
  end if;

  if v_projection_pending > 0 or v_projection_processing > 0 or v_projection_http > 0 then
    v_state := public.skillset_sdlc_projection_v5_tick_v1();
    update skillset.skills_sh_completion_control_v1
    set phase = 'projection', status = 'running', detail = v_state,
        last_error = null, updated_at = now()
    where singleton = true;
    return jsonb_build_object('phase','projection','state',v_state);
  end if;

  select
    count(*) filter (where status = 'running'),
    count(*) filter (where status = 'failed')
  into v_running_eval, v_failed_eval
  from skillset.evaluation_runs
  where implementation_rev = '3.1h';

  if v_failed_eval > 0 then
    update skillset.skills_sh_completion_control_v1
    set phase = 'blocked', status = 'blocked',
        last_error = 'evaluation_failed_runs',
        detail = jsonb_build_object('failed_evaluation_runs', v_failed_eval),
        updated_at = now()
    where singleton = true;
    return jsonb_build_object('phase','blocked','failed_evaluation_runs',v_failed_eval);
  end if;

  if v_running_eval = 0 then
    select x.run_id into v_run
    from public.skillset_eval_v31_canonical_start_delta(5000) x
    limit 1;
    if v_run is not null then
      v_running_eval := 1;
    end if;
  end if;

  if v_running_eval > 0 then
    v_state := public.skillset_eval_v31_canonical_tick();
    update skillset.skills_sh_completion_control_v1
    set phase = 'evaluation', status = 'running', detail = v_state,
        last_error = null, updated_at = now()
    where singleton = true;
    return jsonb_build_object('phase','evaluation','state',v_state);
  end if;

  select count(*) into v_eval_needed
  from skillset.sdlc_projection_hash_v52 h
  where h.transformer_version = 'sdlc-projection-v5.2'
    and h.decision = 'keep'
    and h.residual_model_risks = 0
    and h.projected_content_gzip is not null
    and h.projected_content_hash is not null
    and not exists (
      select 1
      from skillset.evaluation_v31h_cache c
      where c.source_content_hash = h.source_content_hash
        and c.implementation_rev = '3.1h'
        and c.projected_content_hash = h.projected_content_hash
    );

  if v_eval_needed > 0 then
    update skillset.skills_sh_completion_control_v1
    set phase = 'evaluation', status = 'running',
        detail = jsonb_build_object('evaluation_needed', v_eval_needed),
        updated_at = now()
    where singleton = true;
    return jsonb_build_object('phase','evaluation','needed',v_eval_needed);
  end if;

  update skillset.skills_sh_completion_control_v1
  set enabled = false,
      status = 'completed',
      phase = 'completed',
      detail = jsonb_build_object(
        'archive_open', 0,
        'projection_open', 0,
        'projection_terminal', 0,
        'evaluation_needed', 0,
        'integrity_passed', true
      ),
      last_error = null,
      completed_at = now(),
      updated_at = now()
  where singleton = true;

  perform cron.unschedule('skills-sh-completion-autopilot-v1');
  return jsonb_build_object('phase','completed','integrity_passed',true,'autostopped',true);
end
$function$;

create or replace function public.skillset_skills_sh_completion_start_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset', 'cron'
as $function$
begin
  insert into skillset.skills_sh_completion_control_v1(
    singleton, enabled, status, phase, projection_rescue_rounds,
    detail, last_error, started_at, updated_at, completed_at
  ) values (
    true, true, 'running', 'archive_verify', 0,
    '{}'::jsonb, null, now(), now(), null
  )
  on conflict (singleton) do update
  set enabled = true,
      status = 'running',
      phase = 'archive_verify',
      projection_rescue_rounds = 0,
      detail = '{}'::jsonb,
      last_error = null,
      started_at = now(),
      updated_at = now(),
      completed_at = null;

  perform cron.unschedule(jobid)
  from cron.job
  where jobname = 'skills-sh-completion-autopilot-v1';

  perform cron.schedule(
    'skills-sh-completion-autopilot-v1',
    '3 seconds',
    'select public.skillset_skills_sh_completion_tick_v1();'
  );

  return public.skillset_skills_sh_completion_tick_v1();
end
$function$;

create or replace function public.skillset_skills_sh_completion_state_v1()
returns jsonb
language sql
security definer
set search_path to 'pg_catalog', 'public', 'skillset', 'net', 'cron'
as $function$
select to_jsonb(c) || jsonb_build_object(
  'archive', jsonb_build_object(
    'storage_open', (select count(*) from skillset.skills_sh_storage_archive_queue_v1 where status in ('pending','active','error','processing') and (total_repos is null or next_index < total_repos)),
    'novel_open', (select count(*) from skillset.skills_sh_novel_archive_queue_v1 where status in ('pending','active','error','processing') and (total_repos is null or next_index < total_repos))
  ),
  'projection', jsonb_build_object(
    'pending', (select count(*) from skillset.sdlc_projection_v5_queue where status in ('pending','error') and attempts < 4),
    'processing', (select count(*) from skillset.sdlc_projection_v5_queue where status = 'processing'),
    'terminal', (select count(*) from skillset.sdlc_projection_v5_queue where status = 'error' and attempts >= 4),
    'done', (select count(*) from skillset.sdlc_projection_v5_queue where status = 'done')
  ),
  'evaluation', jsonb_build_object(
    'running_runs', (select count(*) from skillset.evaluation_runs where implementation_rev = '3.1h' and status = 'running'),
    'failed_runs', (select count(*) from skillset.evaluation_runs where implementation_rev = '3.1h' and status = 'failed')
  ),
  'cron_active', exists(select 1 from cron.job where jobname = 'skills-sh-completion-autopilot-v1' and active)
)
from skillset.skills_sh_completion_control_v1 c
where c.singleton = true
$function$;

revoke all on function public.skillset_skills_sh_novel_archive_finish_v1(
  integer, integer, text, text, text, integer, integer, integer, integer, text
) from public, anon, authenticated;
revoke all on function public.skillset_sdlc_projection_v5_tick_v1() from public, anon, authenticated;
revoke all on function public.skillset_skills_sh_completion_tick_v1() from public, anon, authenticated;
revoke all on function public.skillset_skills_sh_completion_start_v1() from public, anon, authenticated;
revoke all on function public.skillset_skills_sh_completion_state_v1() from public, anon, authenticated;

grant execute on function public.skillset_skills_sh_novel_archive_finish_v1(
  integer, integer, text, text, text, integer, integer, integer, integer, text
) to service_role;
grant execute on function public.skillset_sdlc_projection_v5_tick_v1() to service_role;
grant execute on function public.skillset_skills_sh_completion_tick_v1() to service_role;
grant execute on function public.skillset_skills_sh_completion_start_v1() to service_role;
grant execute on function public.skillset_skills_sh_completion_state_v1() to service_role;

select public.skillset_skills_sh_completion_start_v1();

commit;
