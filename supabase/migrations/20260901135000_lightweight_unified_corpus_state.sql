begin;

create or replace function public.skillset_unified_corpus_state_v1()
returns jsonb
language sql
security definer
set search_path to 'pg_catalog','public','skillset','cron'
as $function$
select to_jsonb(c)||jsonb_build_object(
  'canonical_rows',coalesce((c.detail->>'canonical')::bigint,(c.detail->>'canonical_rows')::bigint),
  'source_rows',(c.detail->>'source_rows')::bigint,
  'exports',jsonb_build_object(
    'done',(select count(*) from skillset.unified_canonical_export_manifest_v1 where status='done'),
    'open',(select count(*) from skillset.unified_canonical_export_manifest_v1 where status<>'done')
  ),
  'synthesis_candidates',coalesce((c.detail->>'synthesis_candidates')::bigint,
    (select count(*) from skillset.unified_synthesis_candidates_v1)),
  'database_size',pg_size_pretty(pg_database_size(current_database())),
  'cron_active',exists(select 1 from cron.job where jobname='unified-corpus-autopilot-v1' and active)
)
from skillset.unified_corpus_control_v1 c where singleton=true
$function$;

revoke all on function public.skillset_unified_corpus_state_v1() from public,anon,authenticated;
grant execute on function public.skillset_unified_corpus_state_v1() to service_role;

commit;
