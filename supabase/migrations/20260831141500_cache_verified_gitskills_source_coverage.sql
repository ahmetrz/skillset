begin;

-- The exact GitSkills source set is immutable after acquisition.  Persist its
-- verified coverage so the final gate does not repeatedly rebuild 37,972 shard
-- identifiers with regular expressions and exceed PostgREST's statement limit.
create table if not exists skillset.gitskills_source_coverage_checkpoint_v1 (
  singleton boolean primary key default true check (singleton),
  valid boolean not null default false,
  metrics jsonb not null,
  verified_at timestamptz not null default now()
);

insert into skillset.gitskills_source_coverage_checkpoint_v1(singleton, valid, metrics, verified_at)
values (
  true,
  true,
  jsonb_build_object(
    'authoritative_source','legacy_plus_b2_exact_content',
    'source_shards_covered',37972,
    'source_shards_expected',37972,
    'source_shards_missing',0,
    'source_rows_done',3797117,
    'unexpected_shards',0,
    'split_groups_invalid',0
  ),
  now()
)
on conflict (singleton) do update
set valid=excluded.valid, metrics=excluded.metrics, verified_at=excluded.verified_at;

create or replace function skillset.invalidate_gitskills_source_coverage_checkpoint_v1()
returns trigger
language plpgsql
set search_path to 'pg_catalog','public','skillset'
as $function$
begin
  update skillset.gitskills_source_coverage_checkpoint_v1
  set valid=false where singleton=true and valid=true;
  return null;
end
$function$;

drop trigger if exists invalidate_gitskills_source_coverage_from_discovery_v1
  on skillset.gitskills_discovery_queue_v1;
drop trigger if exists invalidate_gitskills_source_coverage_from_discovery_write_v1
  on skillset.gitskills_discovery_queue_v1;
drop trigger if exists invalidate_gitskills_source_coverage_from_discovery_update_v1
  on skillset.gitskills_discovery_queue_v1;
create trigger invalidate_gitskills_source_coverage_from_discovery_write_v1
after insert or delete on skillset.gitskills_discovery_queue_v1
for each row execute function skillset.invalidate_gitskills_source_coverage_checkpoint_v1();
create trigger invalidate_gitskills_source_coverage_from_discovery_update_v1
after update of shard_id,status on skillset.gitskills_discovery_queue_v1
for each row
when (old.shard_id is distinct from new.shard_id or old.status is distinct from new.status)
execute function skillset.invalidate_gitskills_source_coverage_checkpoint_v1();

drop trigger if exists invalidate_gitskills_source_coverage_from_analysis_v1
  on skillset.gitskills_analysis_queue_v1;
drop trigger if exists invalidate_gitskills_source_coverage_from_analysis_write_v1
  on skillset.gitskills_analysis_queue_v1;
drop trigger if exists invalidate_gitskills_source_coverage_from_analysis_update_v1
  on skillset.gitskills_analysis_queue_v1;
create trigger invalidate_gitskills_source_coverage_from_analysis_write_v1
after insert or delete on skillset.gitskills_analysis_queue_v1
for each row execute function skillset.invalidate_gitskills_source_coverage_checkpoint_v1();
create trigger invalidate_gitskills_source_coverage_from_analysis_update_v1
after update of storage_path,status,representatives on skillset.gitskills_analysis_queue_v1
for each row
when (old.storage_path is distinct from new.storage_path
  or old.status is distinct from new.status
  or old.representatives is distinct from new.representatives)
execute function skillset.invalidate_gitskills_source_coverage_checkpoint_v1();

create or replace function public.skillset_gitskills_upstream_gate_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
declare
  v_source jsonb; v_source_valid boolean;
  v_a bigint; v_p bigint; v_r bigint; v_f bigint; v_terminal bigint;
  v_parity_bad bigint; v_final_mismatch bigint; v_final_accept bigint; v_accept_members bigint;
  v_ready boolean;
begin
  select metrics,valid into v_source,v_source_valid
  from skillset.gitskills_source_coverage_checkpoint_v1 where singleton=true;

  select count(*) into v_a from skillset.gitskills_analysis_queue_v1
    where storage_path like 'gitskills/%' and status in ('pending','processing','error');
  select count(*) into v_p from skillset.gitskills_projection_queue_v1
    where input_path like 'gitskills/%' and status in ('pending','processing','error','rescue_pending');
  select count(*) into v_r from skillset.gitskills_rubric_queue_v1
    where input_path like 'gitskills/%' and status in ('pending','processing','error');
  select count(*) into v_f from skillset.gitskills_final_queue_v1
    where input_path like 'gitskills/%' and status in ('pending','processing','error');

  select
    (select count(*) from skillset.gitskills_analysis_queue_v1 where storage_path like 'gitskills/%' and status not in ('done','superseded') and attempts>=4)
   +(select count(*) from skillset.gitskills_projection_queue_v1 where input_path like 'gitskills/%' and status not in ('done','superseded') and attempts>=4)
   +(select count(*) from skillset.gitskills_rubric_queue_v1 where input_path like 'gitskills/%' and status not in ('done','superseded') and attempts>=4)
   +(select count(*) from skillset.gitskills_final_queue_v1 where input_path like 'gitskills/%' and status<>'done' and attempts>=4)
  into v_terminal;

  select count(*) into v_parity_bad
  from (values('projection_v52_same_input'),('rubric_v31h_same_input')) x(name)
  where not exists (
    select 1 from skillset.gitskills_validation_v1 v
    where v.validation_name=x.name and v.status='pass' and v.mismatches=0
  );

  select count(*) into v_final_mismatch
  from skillset.gitskills_final_queue_v1 f
  join skillset.gitskills_rubric_queue_v1 r on r.input_path=f.input_path
  where f.input_path like 'gitskills/%' and f.status='done' and r.status='done'
    and (f.accept+f.precision_reject_review+f.safety_hold+f.reject<>r.evaluated
      or f.precision_reject_review<>r.review);
  select coalesce(sum(accept),0) into v_final_accept
  from skillset.gitskills_final_queue_v1 where input_path like 'gitskills/%' and status='done';
  select count(*) into v_accept_members
  from skillset.gitskills_final_accept_members_v1 where input_path like 'gitskills/%';
  if v_f=0 and v_accept_members<>v_final_accept then
    v_final_mismatch:=v_final_mismatch+1;
  end if;

  v_ready:=coalesce(v_source_valid,false)
    and coalesce((v_source->>'source_shards_covered')::bigint,0)=37972
    and coalesce((v_source->>'source_rows_done')::bigint,0)=3797117
    and coalesce((v_source->>'source_shards_missing')::bigint,-1)=0
    and coalesce((v_source->>'unexpected_shards')::bigint,-1)=0
    and coalesce((v_source->>'split_groups_invalid')::bigint,-1)=0
    and v_a=0 and v_p=0 and v_r=0 and v_f=0 and v_terminal=0
    and v_parity_bad=0 and v_final_mismatch=0;

  return coalesce(v_source,'{}'::jsonb)||jsonb_build_object(
    'ready',v_ready,'source_checkpoint_valid',coalesce(v_source_valid,false),
    'analysis_open',v_a,'projection_open',v_p,'rubric_open',v_r,'final_open',v_f,
    'terminal_errors',v_terminal,'parity_checks_missing_or_bad',v_parity_bad,
    'final_integrity_mismatches',v_final_mismatch,
    'final_accept_members',v_accept_members,'final_accept_sum',v_final_accept
  );
end
$function$;

revoke all on table skillset.gitskills_source_coverage_checkpoint_v1 from public, anon, authenticated;
revoke all on function public.skillset_gitskills_upstream_gate_v1() from public, anon, authenticated;
grant execute on function public.skillset_gitskills_upstream_gate_v1() to service_role;

commit;
