begin;

create or replace function public.skillset_skills_sh_novel_archive_autopilot_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset', 'cron'
as $function$
declare
  v_state jsonb;
  v_open integer;
  v_processing integer;
  v_done integer;
begin
  v_state := public.skillset_skills_sh_novel_archive_tick_v1();

  select
    count(*) filter (
      where status in ('pending','active','error')
        and (total_repos is null or next_index < total_repos)
    ),
    count(*) filter (where status = 'processing'),
    count(*) filter (where status = 'done')
  into v_open, v_processing, v_done
  from skillset.skills_sh_novel_archive_queue_v1;

  if v_open = 0 and v_processing = 0 then
    perform cron.unschedule('skills-sh-novel-archive-autopilot-v1');
  end if;

  return coalesce(v_state, '{}'::jsonb) || jsonb_build_object(
    'open_buckets', v_open,
    'processing_buckets', v_processing,
    'done_buckets', v_done,
    'autostopped', v_open = 0 and v_processing = 0
  );
end
$function$;

revoke all on function public.skillset_skills_sh_novel_archive_autopilot_v1()
  from public, anon, authenticated;
grant execute on function public.skillset_skills_sh_novel_archive_autopilot_v1()
  to service_role;

select cron.unschedule(jobid)
from cron.job
where jobname = 'skills-sh-novel-archive-autopilot-v1';

select cron.schedule(
  'skills-sh-novel-archive-autopilot-v1',
  '10 seconds',
  'select public.skillset_skills_sh_novel_archive_autopilot_v1();'
);

commit;
