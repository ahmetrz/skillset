create or replace function public.skillset_seed_leaderboard_page(p_rows jsonb,p_page integer,p_total integer,p_has_more boolean)
returns integer
language plpgsql
security definer
set search_path to 'public','skillset'
as $function$
declare v_count integer;begin
  with normalized as (
    select concat_ws('/',r->>'source',r->>'skillId') as id,
           r->>'source' as source,
           r->>'skillId' as skill_id,
           coalesce(r->>'name',r->>'skillId') as skill_name,
           coalesce((r->>'installs')::bigint,0) as installs
    from jsonb_array_elements(p_rows) r
    where coalesce(r->>'source','')<>'' and coalesce(r->>'skillId','')<>''
  ), deduped as (select distinct on(id)* from normalized order by id,installs desc)
  insert into skillset.skills(id,source,skill_name,owner,repo,skill_url,source_url,installs,retrieval_status,first_seen_at,last_seen_at,updated_at)
  select id,source,skill_name,
         case when source like '%/%' then split_part(source,'/',1) end,
         case when source like '%/%' then split_part(source,'/',2) end,
         'https://skills.sh/'||id,
         case when source like '%/%' then 'https://github.com/'||source else null end,
         installs,'pending',now(),now(),now()
  from deduped
  on conflict(id) do update set installs=excluded.installs,skill_name=coalesce(excluded.skill_name,skillset.skills.skill_name),owner=coalesce(skillset.skills.owner,excluded.owner),repo=coalesce(skillset.skills.repo,excluded.repo),skill_url=coalesce(skillset.skills.skill_url,excluded.skill_url),source_url=coalesce(skillset.skills.source_url,excluded.source_url),last_seen_at=now(),updated_at=now();
  get diagnostics v_count=row_count;

  insert into skillset.repositories(source,owner,repo)
  select distinct r->>'source',split_part(r->>'source','/',1),split_part(r->>'source','/',2)
  from jsonb_array_elements(p_rows) r where coalesce(r->>'source','') like '%/%'
  on conflict(source) do nothing;

  insert into skillset.owner_search(owner)
  select distinct split_part(r->>'source','/',1)
  from jsonb_array_elements(p_rows) r where coalesce(r->>'source','') like '%/%'
  on conflict(owner) do nothing;

  insert into skillset.crawl_state(key,value,updated_at)
  values('leaderboard:all-time',jsonb_build_object('page',p_page,'nextPage',p_page+1,'total',p_total,'hasMore',p_has_more,'status',case when p_has_more then 'running' else 'complete' end,'inProgress',false),now())
  on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at;
  return v_count;
end;$function$;

create or replace function public.skillset_trigger_wellknown_content()
returns bigint
language plpgsql
security definer
set search_path to 'public','skillset','extensions'
as $function$
declare v_token uuid:=gen_random_uuid();v_request_id bigint;begin
 if not public.skillset_storage_has_capacity() then return 0; end if;
 insert into skillset.job_tokens(token,purpose,expires_at) values(v_token,'wellknown-content',now()+interval '10 minutes');
 select net.http_post(url:='https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-wellknown-content',headers:=jsonb_build_object('Content-Type','application/json'),body:=jsonb_build_object('token',v_token::text,'limit',25)) into v_request_id;
 return v_request_id;
end;$function$;

do $$ declare v_jobid bigint; begin
 select jobid into v_jobid from cron.job where jobname='skillset-wellknown-content-every-2-minutes'; if v_jobid is not null then perform cron.unschedule(v_jobid); end if;
 select jobid into v_jobid from cron.job where jobname='skillset-wellknown-content-every-minute'; if v_jobid is not null then perform cron.unschedule(v_jobid); end if;
end $$;
select cron.schedule('skillset-wellknown-content-every-minute','* * * * *','select public.skillset_trigger_wellknown_content();');

select public.skillset_sync_owner_queue();