begin;

create or replace function public.skillset_skills_sh_novel_archive_claim_v1()
returns table(bucket_id integer, next_index integer)
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
begin
  if (select count(*) from skillset.skills_sh_repo_reduce_queue_v1 where status='done') < 32 then
    return;
  end if;

  update skillset.skills_sh_novel_archive_queue_v1 as q
  set next_index = case
        when q.current_attempts + 1 >= 2 then least(q.next_index + 1, coalesce(q.total_repos, q.next_index + 1))
        else q.next_index
      end,
      current_attempts = case when q.current_attempts + 1 >= 2 then 0 else q.current_attempts + 1 end,
      processed_repos = q.processed_repos + case when q.current_attempts + 1 >= 2 then 1 else 0 end,
      failed_repos = q.failed_repos + case when q.current_attempts + 1 >= 2 then 1 else 0 end,
      status = case
        when q.current_attempts + 1 >= 2 then
          case when q.total_repos is not null and q.next_index + 1 >= q.total_repos then 'done' else 'active' end
        else 'error'
      end,
      claimed_at = null,
      last_error = concat_ws(' | ', nullif(q.last_error, ''), 'stale_processing'),
      updated_at = now()
  where q.status = 'processing'
    and q.claimed_at < now() - interval '1 minute';

  return query
  with p as(
    select q.bucket_id
    from skillset.skills_sh_novel_archive_queue_v1 q
    where q.status in ('pending','active','error')
      and (q.total_repos is null or q.next_index < q.total_repos)
    order by q.bucket_id
    for update skip locked
    limit 1
  ),
  u as(
    update skillset.skills_sh_novel_archive_queue_v1 q
    set status='processing',
        claimed_at=now(),
        updated_at=now()
    from p
    where q.bucket_id=p.bucket_id
    returning q.bucket_id,q.next_index
  )
  select * from u;
end
$function$;

create or replace function public.skillset_skills_sh_novel_archive_tick_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset', 'net'
as $function$
declare
  v_ready integer;
  v_processing integer;
  v_reclaimed integer := 0;
  v_slots integer;
  v_started integer := 0;
  i integer;
  v_rid bigint;
begin
  if not pg_try_advisory_xact_lock(hashtextextended('skills-sh-novel-archive-tick-v1', 0)) then
    return jsonb_build_object('ready', true, 'started', 0, 'skipped', 'tick_locked');
  end if;

  select count(*) into v_ready
  from skillset.skills_sh_repo_reduce_queue_v1
  where status = 'done';

  if v_ready < 32 then
    return jsonb_build_object('ready', false, 'repo_buckets_done', v_ready);
  end if;

  update skillset.skills_sh_novel_archive_queue_v1
  set next_index = case
        when current_attempts + 1 >= 2 then least(next_index + 1, coalesce(total_repos, next_index + 1))
        else next_index
      end,
      current_attempts = case when current_attempts + 1 >= 2 then 0 else current_attempts + 1 end,
      processed_repos = processed_repos + case when current_attempts + 1 >= 2 then 1 else 0 end,
      failed_repos = failed_repos + case when current_attempts + 1 >= 2 then 1 else 0 end,
      status = case
        when current_attempts + 1 >= 2 then
          case when total_repos is not null and next_index + 1 >= total_repos then 'done' else 'active' end
        else 'error'
      end,
      claimed_at = null,
      last_error = concat_ws(' | ', nullif(last_error, ''), 'stale_processing'),
      updated_at = now()
  where status = 'processing'
    and claimed_at < now() - interval '1 minute';
  get diagnostics v_reclaimed = row_count;

  select count(*) into v_processing
  from skillset.skills_sh_novel_archive_queue_v1
  where status = 'processing';

  v_slots := greatest(0, 2 - v_processing);

  for i in 1..v_slots loop
    exit when not exists(
      select 1
      from skillset.skills_sh_novel_archive_queue_v1
      where status in ('pending', 'active', 'error')
        and (total_repos is null or next_index < total_repos)
    );

    select net.http_post(
      url := 'https://cxvvfgwdqgxczxmomztw.supabase.co/functions/v1/skills-sh-novel-archive-v1',
      headers := '{"Content-Type":"application/json"}'::jsonb,
      body := '{}'::jsonb,
      timeout_milliseconds := 120000
    ) into v_rid;

    v_started := v_started + 1;
  end loop;

  return jsonb_build_object(
    'ready', true,
    'reclaimed', v_reclaimed,
    'started', v_started,
    'processing', v_processing,
    'done_buckets', (
      select count(*)
      from skillset.skills_sh_novel_archive_queue_v1
      where status = 'done'
    ),
    'processed_repos', (
      select coalesce(sum(processed_repos), 0)
      from skillset.skills_sh_novel_archive_queue_v1
    ),
    'failed_repos', (
      select coalesce(sum(failed_repos), 0)
      from skillset.skills_sh_novel_archive_queue_v1
    )
  );
end
$function$;

revoke all on function public.skillset_skills_sh_novel_archive_claim_v1()
  from public, anon, authenticated;
grant execute on function public.skillset_skills_sh_novel_archive_claim_v1()
  to service_role;

revoke all on function public.skillset_skills_sh_novel_archive_tick_v1()
  from public, anon, authenticated;
grant execute on function public.skillset_skills_sh_novel_archive_tick_v1()
  to service_role;

commit;
