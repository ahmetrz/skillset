begin;

-- Oversized 32-shard packs exceed the hosted Edge memory/CPU envelope.  Park
-- only unfinished large packs while a GitHub runner losslessly splits their
-- already-acquired input and prefilter artifacts into bounded children.
update skillset.gitskills_projection_queue_v1 q
set status = 'rescue_pending',
    attempts = case when q.status = 'processing' then greatest(q.attempts - 1, 0) else q.attempts end,
    claimed_at = null,
    error = null,
    updated_at = now()
from storage.objects o
where o.bucket_id = 'skill-discovery-v1'
  and o.name = q.input_path
  and q.status in ('pending', 'processing', 'error')
  and q.input_path ~ '^gitskills/discovery-b2-v2/pack-[0-9-]+[.]json[.]gz$'
  and coalesce((o.metadata ->> 'size')::bigint, 0) > 2000000;

-- A late response from a timed-out worker must never resurrect a parent that
-- has been superseded by verified children.
create or replace function public.skillset_gitskills_projection_finish_v1(
  p_input_path text,
  p_output_path text,
  p_retained integer,
  p_keep integer,
  p_drop integer,
  p_error text
)
returns void
language sql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
  update skillset.gitskills_projection_queue_v1
  set status = case when p_error is null then 'done' else 'error' end,
      output_path = p_output_path,
      retained = p_retained,
      projected_keep = p_keep,
      projected_drop = p_drop,
      error = p_error,
      claimed_at = null,
      finished_at = case when p_error is null then now() else finished_at end,
      updated_at = now()
  where input_path = p_input_path
    and status = 'processing';
$function$;

create or replace function public.skillset_gitskills_projection_rescue_manifest_v1(
  p_worker integer,
  p_workers integer,
  p_limit integer default 100
)
returns table(input_path text, prefilter_path text)
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
begin
  if p_workers < 1 or p_workers > 32 or p_worker < 1 or p_worker > p_workers then
    raise exception 'invalid_worker_partition';
  end if;
  return query
  select q.input_path, q.prefilter_path
  from skillset.gitskills_projection_queue_v1 q
  where q.status = 'rescue_pending'
    and mod((hashtextextended(q.input_path, 0) & 9223372036854775807), p_workers) = p_worker - 1
  order by q.created_at
  limit least(greatest(p_limit, 1), 200);
end
$function$;

create or replace function public.skillset_gitskills_projection_rescue_finish_v1(
  p_parent_input text,
  p_parent_prefilter text,
  p_children jsonb
)
returns integer
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
declare
  child jsonb;
  child_count integer;
  parent_status text;
begin
  if jsonb_typeof(p_children) <> 'array' then
    raise exception 'children_must_be_array';
  end if;
  child_count := jsonb_array_length(p_children);
  if child_count < 1 or child_count > 128 then
    raise exception 'invalid_child_count:%', child_count;
  end if;

  select q.status into parent_status
  from skillset.gitskills_projection_queue_v1 q
  where q.input_path = p_parent_input and q.prefilter_path = p_parent_prefilter
  for update;
  if parent_status is null then raise exception 'parent_not_found'; end if;
  if parent_status not in ('rescue_pending', 'superseded') then
    raise exception 'parent_not_rescuable:%', parent_status;
  end if;

  for child in select value from jsonb_array_elements(p_children)
  loop
    if coalesce(child ->> 'input_path', '') !~ '^gitskills/discovery-projection-split-v1/[a-f0-9]{16}-part-[0-9]{3}[.]json[.]gz$'
       or coalesce(child ->> 'prefilter_path', '') !~ '^gitskills/prefilter-projection-split-v1/[a-f0-9]{16}-part-[0-9]{3}[.]json[.]gz$' then
      raise exception 'invalid_child_path';
    end if;
    if not exists (
      select 1 from storage.objects o
      where o.bucket_id = 'skill-discovery-v1' and o.name = child ->> 'input_path'
    ) or not exists (
      select 1 from storage.objects o
      where o.bucket_id = 'skill-discovery-v1' and o.name = child ->> 'prefilter_path'
    ) then
      raise exception 'child_object_missing';
    end if;

    insert into skillset.gitskills_projection_queue_v1(input_path, prefilter_path, status, attempts)
    values (child ->> 'input_path', child ->> 'prefilter_path', 'pending', 0)
    on conflict (input_path) do nothing;
  end loop;

  update skillset.gitskills_projection_queue_v1
  set status = 'superseded', claimed_at = null, error = null, updated_at = now()
  where input_path = p_parent_input and status = 'rescue_pending';
  return child_count;
end
$function$;

revoke all on function public.skillset_gitskills_projection_rescue_manifest_v1(integer, integer, integer) from public, anon, authenticated;
revoke all on function public.skillset_gitskills_projection_rescue_finish_v1(text, text, jsonb) from public, anon, authenticated;
grant execute on function public.skillset_gitskills_projection_rescue_manifest_v1(integer, integer, integer) to service_role;
grant execute on function public.skillset_gitskills_projection_rescue_finish_v1(text, text, jsonb) to service_role;

-- Rescue rows remain open for completion/integrity gates, but normal workers
-- cannot claim them until verified child objects have replaced them.
create or replace function public.skillset_gitskills_pipeline_status_fast_v1()
returns jsonb
language sql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
with s as (
  select 'analysis' stage,status,count(*) n,
         count(*) filter(where status='error' and attempts>=4) terminal
  from skillset.gitskills_analysis_queue_v1 group by status
  union all
  select 'projection',status,count(*),
         count(*) filter(where status='error' and attempts>=4)
  from skillset.gitskills_projection_queue_v1 group by status
  union all
  select 'rubric',status,count(*),
         count(*) filter(where status='error' and attempts>=4)
  from skillset.gitskills_rubric_queue_v1 group by status
  union all
  select 'final',status,count(*),
         count(*) filter(where status='error' and attempts>=4)
  from skillset.gitskills_final_queue_v1 group by status
  union all
  select 'export',status,count(*),count(*) filter(where status='error')
  from skillset.gitskills_canonical_export_queue_v1 group by status
), agg as (
  select stage,
         jsonb_object_agg(status,n) counts,
         sum(case when status in ('pending','processing','rescue_pending') then n
                  when status='error' and terminal=0 then n else 0 end) open,
         sum(terminal) terminal
  from s group by stage
)
select jsonb_object_agg(stage, counts || jsonb_build_object('open',open,'terminal',terminal))
from agg
$function$;

commit;
