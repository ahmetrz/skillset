begin;

-- GitSkills acquisition is intentionally hybrid: the first wave was stored in
-- the legacy discovery queue and the remaining exact packs were stored in B2.
-- The parked Parquet fallback is not an authoritative completion source.
create or replace function public.skillset_gitskills_upstream_gate_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
declare
  v_covered bigint; v_source_rows bigint; v_missing bigint; v_unexpected bigint;
  v_min_shard integer; v_max_shard integer; v_split_bad bigint;
  v_a bigint; v_p bigint; v_r bigint; v_f bigint; v_terminal bigint;
  v_parity_bad bigint; v_final_mismatch bigint; v_final_accept bigint; v_accept_members bigint;
  v_ready boolean;
begin
  with expected_split(start_shard, expected_parts, expected_reps) as (
    values
      (135012,8,1674),(135108,7,1494),(135236,8,1634),(135268,7,1441),
      (135300,7,1561),(135332,7,1568),(135364,7,1567),(135396,7,1609),
      (135428,7,1521),(135460,6,1275),(135556,9,1726),(135588,8,1799),
      (135652,7,1568),(135684,8,1756),(135716,8,1717),(135748,8,1818),
      (135780,9,1764),(135812,6,1339),(135844,6,1405),(135876,7,1540),
      (135908,8,1730),(135940,7,1519),(135972,8,1651),(136004,7,1547),
      (136036,7,1531),(136068,8,1704),(136100,7,1631),(136132,8,1620),
      (136164,7,1587)
  ), split_observed as (
    select
      (regexp_match(q.storage_path, 'pack-([0-9]+)-part-'))[1]::integer as start_shard,
      count(distinct (regexp_match(q.storage_path, 'part-([0-9]+)[.]json[.]gz$'))[1]::integer) as parts,
      min((regexp_match(q.storage_path, 'part-([0-9]+)[.]json[.]gz$'))[1]::integer) as min_part,
      max((regexp_match(q.storage_path, 'part-([0-9]+)[.]json[.]gz$'))[1]::integer) as max_part,
      coalesce(sum(q.representatives),0) as reps,
      count(*) filter (where q.status not in ('done','superseded')) as open
    from skillset.gitskills_analysis_queue_v1 q
    where q.storage_path ~ '^gitskills/discovery-b2-split-v1/pack-[0-9]+-part-[0-9]+[.]json[.]gz$'
    group by 1
  ), split_ok as (
    select e.start_shard
    from expected_split e
    join split_observed o using (start_shard)
    where o.parts=e.expected_parts and o.min_part=1 and o.max_part=e.expected_parts
      and o.reps=e.expected_reps and o.open=0
  ), legacy_ids as (
    select q.shard_id
    from skillset.gitskills_discovery_queue_v1 q
    where q.status='done'
  ), regular_b2_ids as (
    select token::integer as shard_id
    from skillset.gitskills_analysis_queue_v1 q
    cross join lateral regexp_split_to_table(
      regexp_replace(regexp_replace(q.storage_path, '^.*/pack-', ''), '[.]json[.]gz$', ''), '-'
    ) token
    where q.storage_path ~ '^gitskills/discovery-b2-v[12]/pack-[0-9]+(-[0-9]+)*[.]json[.]gz$'
      and q.status in ('done','superseded')
  ), split_ids as (
    select generate_series(s.start_shard, s.start_shard+31) as shard_id
    from split_ok s
  ), covered as (
    select shard_id from legacy_ids
    union
    select shard_id from regular_b2_ids
    union
    select shard_id from split_ids
  ), stats as (
    select
      count(*) filter(where shard_id between 100000 and 137971) as covered,
      coalesce(sum(case when shard_id=137971 then 17 else 100 end)
        filter(where shard_id between 100000 and 137971),0) as source_rows,
      count(*) filter(where shard_id not between 100000 and 137971) as unexpected,
      min(shard_id) filter(where shard_id between 100000 and 137971) as min_shard,
      max(shard_id) filter(where shard_id between 100000 and 137971) as max_shard
    from covered
  )
  select s.covered,s.source_rows,37972-s.covered,s.unexpected,s.min_shard,s.max_shard,
         (select count(*) from expected_split e left join split_ok o using(start_shard)
          where o.start_shard is null)
  into v_covered,v_source_rows,v_missing,v_unexpected,v_min_shard,v_max_shard,v_split_bad
  from stats s;

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

  v_ready:=v_covered=37972 and v_source_rows=3797117 and v_missing=0
    and v_unexpected=0 and v_min_shard=100000 and v_max_shard=137971 and v_split_bad=0
    and v_a=0 and v_p=0 and v_r=0 and v_f=0 and v_terminal=0
    and v_parity_bad=0 and v_final_mismatch=0;
  return jsonb_build_object(
    'ready',v_ready,'authoritative_source','legacy_plus_b2_exact_content',
    'source_shards_covered',v_covered,'source_shards_expected',37972,
    'source_shards_missing',v_missing,'source_rows_done',v_source_rows,
    'unexpected_shards',v_unexpected,'split_groups_invalid',v_split_bad,
    'analysis_open',v_a,'projection_open',v_p,'rubric_open',v_r,'final_open',v_f,
    'terminal_errors',v_terminal,'parity_checks_missing_or_bad',v_parity_bad,
    'final_integrity_mismatches',v_final_mismatch,
    'final_accept_members',v_accept_members,'final_accept_sum',v_final_accept
  );
end
$function$;

revoke all on function public.skillset_gitskills_upstream_gate_v1() from public, anon, authenticated;
grant execute on function public.skillset_gitskills_upstream_gate_v1() to service_role;
revoke all on function public.skillset_gitskills_completion_gate_v1() from public, anon, authenticated;
grant execute on function public.skillset_gitskills_completion_gate_v1() to service_role;

commit;
