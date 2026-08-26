alter table skillset.skills add column if not exists retrieval_attempts integer not null default 0;
alter table skillset.skills add column if not exists next_retry_at timestamptz;

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
    where s.retrieval_status='pending'
       or (s.retrieval_status='retry' and coalesce(s.next_retry_at,now())<=now())
       or (s.retrieval_status='processing' and s.updated_at < now()-interval '15 minutes')
    order by s.first_seen_at,s.id
    for update skip locked
    limit greatest(1,least(p_limit,100))
  ), claimed as (
    update skillset.skills s
       set retrieval_status='processing',
           retrieval_attempts=s.retrieval_attempts+1,
           next_retry_at=null,
           updated_at=now()
      from picked p
     where s.id=p.id
    returning s.id,s.source,s.skill_name,s.repo
  )
  select * from claimed;
end;
$function$;

create or replace function public.skillset_mark_error(p_id text,p_error text)
returns void
language plpgsql
security definer
set search_path to 'skillset','public'
as $function$
declare
  v_attempts integer;
  v_error text:=left(coalesce(p_error,'unknown_error'),2000);
begin
  select retrieval_attempts into v_attempts from skillset.skills where id=p_id for update;
  if not found then return; end if;

  update skillset.skills
  set retrieval_status = case
        when v_error like 'download_http_404%' then 'failed'
        when v_error like 'skill_md_not_found_in_snapshot%' and v_attempts>=2 then 'failed'
        when v_attempts>=3 and v_error not like 'download_http_429%' then 'failed'
        when v_attempts>=6 then 'failed'
        else 'retry'
      end,
      next_retry_at = case
        when v_error like 'download_http_429%' and v_attempts<6 then now()+interval '20 minutes'
        when v_error like 'download_http_5%' and v_attempts<3 then now()+interval '5 minutes'
        when v_error like '%timed out%' and v_attempts<3 then now()+interval '5 minutes'
        when v_error like 'skill_md_not_found_in_snapshot%' and v_attempts<2 then now()+interval '10 minutes'
        else null
      end,
      retrieval_error=v_error,
      updated_at=now()
  where id=p_id;
end;
$function$;

update skillset.skills
set retrieval_attempts=case when retrieval_status='failed' then greatest(retrieval_attempts,3) when retrieval_status='retry' then greatest(retrieval_attempts,1) else retrieval_attempts end,
    next_retry_at=case when retrieval_status='retry' and retrieval_error like 'download_http_429%' then now()+interval '20 minutes' else next_retry_at end
where retrieval_status in ('retry','failed');