alter table skillset.skills add column if not exists source_type text;
alter table skillset.skills add column if not exists install_url text;

create or replace function public.skillset_seed_v1_page(p_rows jsonb,p_page integer,p_per_page integer,p_total integer,p_has_more boolean)
returns integer language plpgsql security definer set search_path='skillset','public' as $$
declare v_count integer;begin
  with n as (
    select r->>'id' id,r->>'source' source,coalesce(r->>'name',r->>'slug') skill_name,
      coalesce((r->>'installs')::bigint,0) installs,r->>'sourceType' source_type,
      r->>'installUrl' install_url,r->>'url' skill_url
    from jsonb_array_elements(p_rows) r where coalesce(r->>'id','')<>'' and coalesce(r->>'source','')<>''
  ),d as (select distinct on(id)* from n order by id,installs desc)
  insert into skillset.skills(id,source,skill_name,owner,repo,skill_url,source_url,installs,source_type,install_url,retrieval_status,first_seen_at,last_seen_at,updated_at)
  select id,source,skill_name,
    case when source_type='github' and source like '%/%' then split_part(source,'/',1) end,
    case when source_type='github' and source like '%/%' then split_part(source,'/',2) end,
    skill_url,case when source_type='github' then install_url else null end,installs,source_type,install_url,'pending',now(),now(),now()
  from d on conflict(id) do update set installs=greatest(skillset.skills.installs,excluded.installs),
    skill_name=coalesce(excluded.skill_name,skillset.skills.skill_name),skill_url=coalesce(excluded.skill_url,skillset.skills.skill_url),
    source_type=coalesce(excluded.source_type,skillset.skills.source_type),install_url=coalesce(excluded.install_url,skillset.skills.install_url),
    owner=coalesce(skillset.skills.owner,excluded.owner),repo=coalesce(skillset.skills.repo,excluded.repo),last_seen_at=now(),updated_at=now();
  get diagnostics v_count=row_count;
  insert into skillset.repositories(source,owner,repo)
    select distinct r->>'source',split_part(r->>'source','/',1),split_part(r->>'source','/',2) from jsonb_array_elements(p_rows) r
    where r->>'sourceType'='github' and coalesce(r->>'source','') like '%/%' on conflict(source) do nothing;
  insert into skillset.owner_search(owner)
    select distinct split_part(r->>'source','/',1) from jsonb_array_elements(p_rows) r
    where r->>'sourceType'='github' and coalesce(r->>'source','') like '%/%' on conflict(owner) do nothing;
  insert into skillset.crawl_state(key,value,updated_at)
    values('v1:list',jsonb_build_object('page',p_page,'perPage',p_per_page,'total',p_total,'hasMore',p_has_more,'updatedAt',now()),now())
    on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at;
  return v_count;
end $$;