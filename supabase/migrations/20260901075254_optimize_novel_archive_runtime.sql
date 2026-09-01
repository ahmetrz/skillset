begin;

do $do$
declare
  v_definition text;
begin
  select pg_get_functiondef(
    'public.skillset_skills_sh_novel_archive_tick_v1()'::regprocedure
  )
  into v_definition;

  if position('greatest(0, 2 - v_processing)' in v_definition) > 0 then
    execute replace(
      v_definition,
      'greatest(0, 2 - v_processing)',
      'greatest(0, 3 - v_processing)'
    );
  elsif position('greatest(0, 3 - v_processing)' in v_definition) = 0 then
    raise exception 'unexpected novel archive slot expression';
  end if;
end
$do$;

comment on function public.skillset_skills_sh_novel_archive_tick_v1() is
  'Runs the skills.sh novel archive queue at the measured safe optimum: three concurrent workers with stale-claim recovery.';

revoke all on function public.skillset_skills_sh_novel_archive_finish_v1(
  integer, integer, text, text, text, integer, integer, integer, integer, text
) from public, anon, authenticated;

grant execute on function public.skillset_skills_sh_novel_archive_finish_v1(
  integer, integer, text, text, text, integer, integer, integer, integer, text
) to service_role;

select cron.unschedule(jobid)
from cron.job
where jobname = 'skills-sh-novel-archive-autopilot-v1';

select cron.schedule(
  'skills-sh-novel-archive-autopilot-v1',
  '3 seconds',
  'select public.skillset_skills_sh_novel_archive_autopilot_v1();'
);

commit;
