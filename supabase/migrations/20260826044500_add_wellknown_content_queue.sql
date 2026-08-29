create or replace function public.skillset_claim_pending(p_limit integer default 50)
returns table(id text, source text, skill_name text, repo text)
language plpgsql
security definer
set search_path to 'skillset','public'
as $function$
begin
  return query
  with picked as (
    select s.id
    from skillset.skills s
    where s.source like '%/%'
      and (
        s.retrieval_status='pending'
        or (s.retrieval_status='retry' and coalesce(s.next_retry_at,now())<=now())
        or (s.retrieval_status='processing' and s.updated_at < now()-interval '15 minutes')
      )
    order by s.first_seen_at,s.id
    for update skip locked
    limit greatest(1,least(p_limit,100))
  ), claimed as (
    update skillset.skills s
       set retrieval_status='processing',retrieval_attempts=s.retrieval_attempts+1,next_retry_at=null,updated_at=now()
      from picked p where s.id=p.id
    returning s.id,s.source,s.skill_name,s.repo
  )
  select * from claimed;
end;
$function$;

create or replace function public.skillset_claim_wellknown(p_limit integer default 10)
returns table(id text, source text, skill_name text)
language plpgsql
security definer
set search_path to 'skillset','public'
as $function$
begin
  return query
  with picked as (
    select s.id
    from skillset.skills s
    where s.source not like '%/%'
      and (
        s.retrieval_status='pending'
        or (s.retrieval_status='retry' and coalesce(s.next_retry_at,now())<=now())
        or (s.retrieval_status='processing' and s.updated_at < now()-interval '15 minutes')
      )
    order by s.first_seen_at,s.id
    for update skip locked
    limit greatest(1,least(p_limit,25))
  ), claimed as (
    update skillset.skills s
      set retrieval_status='processing',retrieval_attempts=s.retrieval_attempts+1,next_retry_at=null,updated_at=now()
    from picked p where s.id=p.id
    returning s.id,s.source,s.skill_name
  )
  select * from claimed;
end;
$function$;

update skillset.skills
set retrieval_status='pending',retrieval_error=null,retrieval_attempts=0,next_retry_at=null,updated_at=now()
where source not like '%/%' and retrieval_status<>'ok';