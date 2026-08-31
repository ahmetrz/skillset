begin;

-- All 256 two-hex-prefix exports are mutually exclusive and exhaustive over
-- the canonical hash space.  Summing their verified row counts is therefore
-- the exact canonical count and avoids rebuilding the expensive grouped view
-- inside the final PostgREST request.
create or replace function public.skillset_gitskills_completion_gate_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
declare
  u jsonb;
  v_total int;
  v_open int;
  v_terminal int;
  v_done int;
  v_bad int;
  v_rows bigint;
  v_ready boolean;
begin
  u:=public.skillset_gitskills_upstream_gate_v1();
  select
    count(*),
    count(*) filter(where status in ('pending','processing','error')),
    count(*) filter(where status<>'done' and attempts>=4),
    count(*) filter(where status='done'),
    count(*) filter(where status='done' and (
      row_count is null or row_count<0 or storage_path is null
      or storage_path<>'gitskills/final-canonical-v1/prefix-'||prefix||'.json.gz'
    )),
    coalesce(sum(row_count) filter(where status='done'),0)
  into v_total,v_open,v_terminal,v_done,v_bad,v_rows
  from skillset.gitskills_canonical_export_queue_v1;

  v_ready:=coalesce((u->>'ready')::boolean,false)
    and v_total=256 and v_open=0 and v_terminal=0 and v_done=256 and v_bad=0;

  return u||jsonb_build_object(
    'ready',v_ready,
    'canonical_export_total',v_total,
    'canonical_export_open',v_open,
    'canonical_export_terminal',v_terminal,
    'canonical_export_done',v_done,
    'canonical_export_invalid',v_bad,
    'canonical_export_rows',v_rows,
    'canonical_accept_distinct',v_rows,
    'canonical_count_source','exhaustive_two_hex_prefix_exports'
  );
end
$function$;

revoke all on function public.skillset_gitskills_completion_gate_v1() from public, anon, authenticated;
grant execute on function public.skillset_gitskills_completion_gate_v1() to service_role;
revoke all on function public.skillset_gitskills_canonical_export_claim_v1() from public, anon, authenticated;
revoke all on function public.skillset_gitskills_canonical_export_finish_v1(text,text,integer,text) from public, anon, authenticated;
revoke all on function public.skillset_gitskills_canonical_export_tick_v1() from public, anon, authenticated;
grant execute on function public.skillset_gitskills_canonical_export_claim_v1() to service_role;
grant execute on function public.skillset_gitskills_canonical_export_finish_v1(text,text,integer,text) to service_role;
grant execute on function public.skillset_gitskills_canonical_export_tick_v1() to service_role;
revoke all on public.gitskills_final_accept_canonical_v1 from public, anon, authenticated;
grant select on public.gitskills_final_accept_canonical_v1 to service_role;

commit;
