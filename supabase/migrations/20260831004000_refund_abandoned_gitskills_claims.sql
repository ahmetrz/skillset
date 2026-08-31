-- A cancelled worker never completed its claim, so its attempt must not count
-- as a processing failure. Real finish errors still consume the four-attempt
-- budget and remain fail-closed.

create or replace function public.skillset_gitskills_analysis_claim_v1()
returns table(storage_path text, attempts integer)
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
begin
  perform pg_advisory_xact_lock(hashtextextended('gitskills_analysis_claim_capacity_v1', 0));
  update skillset.gitskills_analysis_queue_v1 q
  set status = 'pending',
      attempts = greatest(q.attempts - 1, 0),
      claimed_at = null,
      error = 'stale_processing',
      updated_at = now()
  where status = 'processing'
    and claimed_at < now() - interval '4 minutes';
  if (select count(*) from skillset.gitskills_analysis_queue_v1 where status = 'processing') >= 16 then
    raise exception 'capacity_limited:analysis';
  end if;
  return query
  with p as (
    select q.storage_path
    from skillset.gitskills_analysis_queue_v1 q
    where q.status in ('pending', 'error') and q.attempts < 4
    order by q.created_at
    for update skip locked
    limit 1
  ), u as (
    update skillset.gitskills_analysis_queue_v1 q
    set status = 'processing', attempts = q.attempts + 1,
        claimed_at = now(), error = null, updated_at = now()
    from p
    where q.storage_path = p.storage_path
    returning q.storage_path, q.attempts
  )
  select * from u;
end
$function$;

create or replace function public.skillset_gitskills_projection_claim_v1()
returns table(input_path text, prefilter_path text, attempts integer)
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
begin
  perform pg_advisory_xact_lock(hashtextextended('gitskills_projection_claim_capacity_v1', 0));
  update skillset.gitskills_projection_queue_v1 q
  set status = 'pending',
      attempts = greatest(q.attempts - 1, 0),
      claimed_at = null,
      error = 'stale_processing',
      updated_at = now()
  where status = 'processing'
    and claimed_at < now() - interval '4 minutes';
  if (select count(*) from skillset.gitskills_projection_queue_v1 where status = 'processing') >= 24 then
    raise exception 'capacity_limited:projection';
  end if;
  return query
  with p as (
    select q.input_path
    from skillset.gitskills_projection_queue_v1 q
    where q.status in ('pending', 'error') and q.attempts < 4
    order by q.created_at
    for update skip locked
    limit 1
  ), u as (
    update skillset.gitskills_projection_queue_v1 q
    set status = 'processing', attempts = q.attempts + 1,
        claimed_at = now(), error = null, updated_at = now()
    from p
    where q.input_path = p.input_path
    returning q.input_path, q.prefilter_path, q.attempts
  )
  select * from u;
end
$function$;

create or replace function public.skillset_gitskills_rubric_claim_v1()
returns table(input_path text, projection_path text, attempts integer)
language plpgsql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
begin
  perform pg_advisory_xact_lock(hashtextextended('gitskills_rubric_claim_capacity_v1', 0));
  update skillset.gitskills_rubric_queue_v1 q
  set status = 'pending',
      attempts = greatest(q.attempts - 1, 0),
      claimed_at = null,
      error = 'stale_processing',
      updated_at = now()
  where status = 'processing'
    and claimed_at < now() - interval '4 minutes';
  if (select count(*) from skillset.gitskills_rubric_queue_v1 where status = 'processing') >= 24 then
    raise exception 'capacity_limited:rubric';
  end if;
  return query
  with p as (
    select q.input_path
    from skillset.gitskills_rubric_queue_v1 q
    where q.status in ('pending', 'error') and q.attempts < 4
    order by q.created_at
    for update skip locked
    limit 1
  ), u as (
    update skillset.gitskills_rubric_queue_v1 q
    set status = 'processing', attempts = q.attempts + 1,
        claimed_at = now(), error = null, updated_at = now()
    from p
    where q.input_path = p.input_path
    returning q.input_path, q.projection_path, q.attempts
  )
  select * from u;
end
$function$;

update skillset.gitskills_projection_queue_v1
set attempts = greatest(attempts - 1, 0),
    updated_at = now()
where status = 'pending'
  and attempts >= 4
  and error = 'stale_processing';
