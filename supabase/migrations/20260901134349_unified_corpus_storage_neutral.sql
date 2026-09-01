begin;

do $$
declare v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname='unified-corpus-autopilot-v1';
  if v_jobid is not null then perform cron.unschedule(v_jobid); end if;
end $$;

drop function if exists public.skillset_unified_canonical_search_v1(text,text[],integer,text,integer);
drop function if exists public.skillset_unified_corpus_tick_v1();
drop function if exists public.skillset_unified_corpus_start_v1();
drop function if exists public.skillset_unified_corpus_state_v1();
drop table if exists skillset.unified_canonical_sources_v1;
drop table if exists skillset.unified_synthesis_candidates_v1;
drop table if exists skillset.unified_canonical_v1;

create view skillset.unified_canonical_v1
with (security_invoker=true)
as
with g as (
  select * from public.gitskills_final_accept_canonical_v1
), e as (
  select * from skillset.evaluation_v31h_cache
  where implementation_rev='3.1h' and decision='accept'
), hashes as (
  select source_content_hash from g
  union
  select source_content_hash from e
)
select
  h.source_content_hash canonical_hash,
  coalesce(e.projected_content_hash,g.projected_content_hash) projected_content_hash,
  'accept'::text decision,
  greatest(coalesce(g.score,0),coalesce(e.score,0)) score,
  greatest(coalesce(g.raw_structural_score,0),coalesce(e.raw_structural_score,0)) raw_structural_score,
  greatest(coalesce(g.projection_score,0),coalesce(e.projection_score,0)) projection_score,
  case when greatest(coalesce(g.score,0),coalesce(e.score,0))>=85 then 'exceptional'
       when greatest(coalesce(g.score,0),coalesce(e.score,0))>=75 then 'strong'
       else 'accepted' end quality_tier,
  coalesce(g.occurrence_count,0)+(case when e.source_content_hash is null then 0 else 1 end)::bigint occurrence_count,
  g.source_content_hash is not null in_gitskills,
  e.source_content_hash is not null in_skills_sh,
  array_remove(array[
    case when g.source_content_hash is not null then 'gitskills' end,
    case when e.source_content_hash is not null then 'skills.sh' end
  ],null)::text[] source_systems,
  coalesce(g.representative_repo,s.repo) representative_repo,
  coalesce(g.representative_path,s.source_url,s.skill_url) representative_path,
  coalesce(e.representative_skill_id,s.id) representative_skill_id,
  coalesce(p.sdlc_categories,'{}'::text[]) sdlc_categories,
  jsonb_build_object('gitskills_score',g.score,'skills_sh_score',e.score) metadata
from hashes h
left join g using(source_content_hash)
left join e using(source_content_hash)
left join skillset.skills s on s.id=e.representative_skill_id
left join skillset.sdlc_projection_hash_v52 p
  on p.source_content_hash=h.source_content_hash
 and p.transformer_version='sdlc-projection-v5.2';

create view skillset.unified_canonical_sources_v1
with (security_invoker=true)
as
select
  m.source_content_hash canonical_hash,
  'gitskills'::text source_kind,
  'gitskills:'||md5(concat_ws('|',m.input_path,m.repo_full_name,m.path)) source_key,
  m.repo_full_name repository,m.path,null::text skill_id,
  'https://github.com/'||m.repo_full_name||'/blob/HEAD/'||m.path source_url,
  jsonb_build_object('input_path',m.input_path,'score',m.score) metadata
from skillset.gitskills_final_accept_members_v1 m
union all
select
  s.content_hash,'skills.sh','skills.sh:'||s.id,s.repo,null::text,s.id,
  coalesce(s.source_url,s.skill_url),
  jsonb_build_object('skill_name',s.skill_name,'source',s.source,'installs',s.installs)
from skillset.skills s
join skillset.evaluation_v31h_cache e
  on e.source_content_hash=s.content_hash
 and e.implementation_rev='3.1h' and e.decision='accept';

revoke all on skillset.unified_canonical_v1 from public,anon,authenticated;
revoke all on skillset.unified_canonical_sources_v1 from public,anon,authenticated;
grant select on skillset.unified_canonical_v1 to service_role;
grant select on skillset.unified_canonical_sources_v1 to service_role;

create table skillset.unified_synthesis_candidates_v1 (
  target_area text not null check (target_area in ('sdlc','social_media','global')),
  category text not null,
  rank integer not null,
  canonical_hash text not null,
  score integer not null,
  occurrence_count bigint not null,
  selection_reason jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  primary key(target_area,category,rank),
  unique(target_area,category,canonical_hash)
);
create index unified_synthesis_candidates_v1_canonical_hash_idx
  on skillset.unified_synthesis_candidates_v1(canonical_hash);
alter table skillset.unified_synthesis_candidates_v1 enable row level security;
revoke all on skillset.unified_synthesis_candidates_v1 from public,anon,authenticated;
grant select,insert,update,delete on skillset.unified_synthesis_candidates_v1 to service_role;
create policy unified_synthesis_service_role_v1 on skillset.unified_synthesis_candidates_v1
  for all to service_role using(true) with check(true);

create or replace function public.skillset_unified_canonical_search_v1(
  p_query text default null,p_categories text[] default null,p_min_score integer default 70,
  p_after_hash text default null,p_limit integer default 100
)
returns table(canonical_hash text,score integer,quality_tier text,occurrence_count bigint,
  source_systems text[],representative_repo text,representative_path text,sdlc_categories text[])
language sql security definer set search_path to 'pg_catalog','public','skillset'
as $function$
  select c.canonical_hash,c.score,c.quality_tier,c.occurrence_count,c.source_systems,
         c.representative_repo,c.representative_path,c.sdlc_categories
  from skillset.unified_canonical_v1 c
  where c.score>=greatest(0,coalesce(p_min_score,70))
    and (p_after_hash is null or c.canonical_hash>p_after_hash)
    and (p_categories is null or c.sdlc_categories&&p_categories)
    and (p_query is null or btrim(p_query)='' or
      concat_ws(' ',c.representative_repo,c.representative_path,c.representative_skill_id)
      ilike '%'||replace(replace(p_query,'%','\%'),'_','\_')||'%' escape '\')
  order by c.canonical_hash limit greatest(1,least(coalesce(p_limit,100),1000))
$function$;

create or replace function public.skillset_unified_corpus_tick_v1()
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','skillset','cron'
as $function$
declare
  v_control skillset.unified_corpus_control_v1%rowtype;
  v_expected bigint; v_actual bigint; v_sources bigint; v_missing bigint;
  v_export_rows bigint; v_export_bad bigint; v_candidates bigint;
begin
  if not pg_try_advisory_xact_lock(hashtextextended('unified-corpus-tick-v2',0)) then
    return jsonb_build_object('started',0,'skipped','tick_locked');
  end if;
  select * into v_control from skillset.unified_corpus_control_v1 where singleton=true for update;
  if not v_control.enabled then return jsonb_build_object('status',v_control.status,'enabled',false); end if;

  if v_control.phase='canonical' then
    select count(*) into v_actual from skillset.unified_canonical_v1;
    update skillset.unified_corpus_control_v1 set phase='sources',consecutive_failures=0,last_error=null,
      detail=jsonb_build_object('canonical_rows',v_actual,'storage_mode','view'),updated_at=now()
    where singleton=true;
    return jsonb_build_object('phase','sources','canonical_rows',v_actual,'transitioned',true);
  end if;

  if v_control.phase='sources' then
    select count(*) into v_sources from skillset.unified_canonical_sources_v1;
    select count(*) into v_missing from skillset.unified_canonical_v1 c
    where not exists(select 1 from skillset.unified_canonical_sources_v1 s where s.canonical_hash=c.canonical_hash);
    if v_missing<>0 then raise exception 'canonical_without_provenance:%',v_missing; end if;
    update skillset.unified_canonical_export_manifest_v1 set status='pending',attempts=0,row_count=null,
      fingerprint=null,last_error=null,claimed_at=null,finished_at=null,updated_at=now();
    update skillset.unified_corpus_control_v1 set phase='exports',consecutive_failures=0,last_error=null,
      detail=jsonb_build_object('source_rows',v_sources,'missing_sources',v_missing,'storage_mode','view'),updated_at=now()
    where singleton=true;
    return jsonb_build_object('phase','exports','source_rows',v_sources,'transitioned',true);
  end if;

  if v_control.phase='exports' then
    with x as (
      select left(canonical_hash,2) prefix,count(*)::bigint n,
        md5(string_agg(canonical_hash||':'||projected_content_hash,',' order by canonical_hash)) fp
      from skillset.unified_canonical_v1 group by left(canonical_hash,2)
    )
    update skillset.unified_canonical_export_manifest_v1 m set status='done',attempts=attempts+1,
      row_count=coalesce(x.n,0),fingerprint=coalesce(x.fp,md5('')),last_error=null,
      claimed_at=null,finished_at=now(),updated_at=now()
    from x where m.prefix=x.prefix;
    update skillset.unified_canonical_export_manifest_v1 set status='done',attempts=attempts+1,
      row_count=0,fingerprint=md5(''),finished_at=now(),updated_at=now() where status<>'done';
    update skillset.unified_corpus_control_v1 set phase='synthesis',consecutive_failures=0,last_error=null,
      detail=jsonb_build_object('export_prefixes',256),updated_at=now() where singleton=true;
    return jsonb_build_object('phase','synthesis','export_prefixes',256,'transitioned',true);
  end if;

  if v_control.phase='synthesis' then
    truncate skillset.unified_synthesis_candidates_v1;
    insert into skillset.unified_synthesis_candidates_v1(target_area,category,rank,canonical_hash,score,occurrence_count,selection_reason)
    select 'sdlc',category,rn,canonical_hash,score,occurrence_count,
      jsonb_build_object('basis','sdlc_category_pareto','quality_tier',quality_tier)
    from (select c.canonical_hash,c.score,c.occurrence_count,c.quality_tier,category,
      row_number() over(partition by category order by c.score desc,c.occurrence_count desc,c.canonical_hash) rn
      from skillset.unified_canonical_v1 c cross join lateral unnest(c.sdlc_categories) category) x where rn<=100;
    insert into skillset.unified_synthesis_candidates_v1(target_area,category,rank,canonical_hash,score,occurrence_count,selection_reason)
    select 'global','top_quality',rn,canonical_hash,score,occurrence_count,
      jsonb_build_object('basis','global_quality_occurrence_pareto')
    from (select c.*,row_number() over(order by score desc,occurrence_count desc,canonical_hash) rn
      from skillset.unified_canonical_v1 c) x where rn<=500;
    with pool as (
      select distinct c.canonical_hash,c.score,c.occurrence_count,k.category
      from skillset.unified_canonical_v1 c join skillset.unified_canonical_sources_v1 s using(canonical_hash)
      cross join lateral (values
        ('content_creation','(content|creator|social|campaign|post|publishing)'),
        ('video','(video|youtube|tiktok|reels|shorts)'),('image','(image|visual|design|instagram|thumbnail)'),
        ('copywriting','(copy|writing|caption|headline|storytelling)'),
        ('analytics','(analytics|metric|engagement|seo|growth)'),
        ('automation','(automation|workflow|schedule|orchestrat)')) k(category,pattern)
      where lower(concat_ws(' ',s.repository,s.path,s.skill_id,s.metadata::text))~k.pattern
    )
    insert into skillset.unified_synthesis_candidates_v1(target_area,category,rank,canonical_hash,score,occurrence_count,selection_reason)
    select 'social_media',category,rn,canonical_hash,score,occurrence_count,
      jsonb_build_object('basis','metadata_keyword_bridge','requires_domain_review',true)
    from (select p.*,row_number() over(partition by category order by score desc,occurrence_count desc,canonical_hash) rn from pool p) x
    where rn<=100;
    update skillset.unified_corpus_control_v1 set phase='integrity',consecutive_failures=0,last_error=null,
      detail=jsonb_build_object('synthesis_candidates',(select count(*) from skillset.unified_synthesis_candidates_v1)),updated_at=now()
    where singleton=true;
    return jsonb_build_object('phase','integrity','transitioned',true);
  end if;

  if v_control.phase='integrity' then
    select count(*) into v_expected from (
      select source_content_hash from public.gitskills_final_accept_canonical_v1 union
      select source_content_hash from skillset.evaluation_v31h_cache where implementation_rev='3.1h' and decision='accept') u;
    select count(*) into v_actual from skillset.unified_canonical_v1;
    select count(*) into v_missing from skillset.unified_canonical_v1 c
      where not exists(select 1 from skillset.unified_canonical_sources_v1 s where s.canonical_hash=c.canonical_hash);
    select coalesce(sum(row_count),0),count(*) filter(where status<>'done' or fingerprint is null)
      into v_export_rows,v_export_bad from skillset.unified_canonical_export_manifest_v1;
    select count(*) into v_candidates from skillset.unified_synthesis_candidates_v1;
    if v_expected<>v_actual or v_missing<>0 or v_export_rows<>v_actual or v_export_bad<>0 or v_candidates=0 then
      update skillset.unified_corpus_control_v1 set status='blocked',phase='blocked',last_error='integrity_gate_failed',
        detail=jsonb_build_object('expected',v_expected,'actual',v_actual,'missing_sources',v_missing,
          'export_rows',v_export_rows,'export_bad',v_export_bad,'synthesis_candidates',v_candidates),updated_at=now()
      where singleton=true;
      perform cron.unschedule('unified-corpus-autopilot-v1');
      return jsonb_build_object('phase','blocked','reason','integrity_gate_failed');
    end if;
    select count(*) into v_sources from skillset.unified_canonical_sources_v1;
    update skillset.unified_corpus_control_v1 set enabled=false,status='completed',phase='completed',last_error=null,
      detail=jsonb_build_object('canonical',v_actual,'source_rows',v_sources,'export_prefixes',256,
        'export_rows',v_export_rows,'synthesis_candidates',v_candidates,'integrity_passed',true,'storage_mode','view'),
      completed_at=now(),updated_at=now() where singleton=true;
    perform cron.unschedule('unified-corpus-autopilot-v1');
    return jsonb_build_object('phase','completed','integrity_passed',true,'autostopped',true);
  end if;
  return jsonb_build_object('phase',v_control.phase,'status',v_control.status);
exception when others then
  update skillset.unified_corpus_control_v1 set consecutive_failures=consecutive_failures+1,last_error=sqlerrm,
    status=case when consecutive_failures+1>=5 then 'blocked' else status end,
    phase=case when consecutive_failures+1>=5 then 'blocked' else phase end,
    detail=detail||jsonb_build_object('last_failure_at',now(),'sqlstate',sqlstate),updated_at=now() where singleton=true;
  if (select consecutive_failures>=5 from skillset.unified_corpus_control_v1 where singleton=true) then
    perform cron.unschedule('unified-corpus-autopilot-v1');
  end if;
  return jsonb_build_object('ok',false,'error',sqlerrm,'sqlstate',sqlstate);
end
$function$;

create or replace function public.skillset_unified_corpus_start_v1()
returns jsonb language plpgsql security definer set search_path to 'pg_catalog','public','skillset','cron'
as $function$
begin
  update skillset.unified_corpus_control_v1 set enabled=true,status='running',phase='canonical',
    canonical_cursor=null,source_cursor=null,consecutive_failures=0,detail='{}'::jsonb,last_error=null,
    started_at=now(),updated_at=now(),completed_at=null where singleton=true;
  perform cron.unschedule(jobid) from cron.job where jobname='unified-corpus-autopilot-v1';
  perform cron.schedule('unified-corpus-autopilot-v1','10 seconds','select public.skillset_unified_corpus_tick_v1();');
  return public.skillset_unified_corpus_tick_v1();
end
$function$;

create or replace function public.skillset_unified_corpus_state_v1()
returns jsonb language sql security definer set search_path to 'pg_catalog','public','skillset','cron'
as $function$
select to_jsonb(c)||jsonb_build_object(
  'canonical_rows',case when c.phase in ('canonical','blocked') then null else (select count(*) from skillset.unified_canonical_v1) end,
  'source_rows',case when c.phase in ('canonical','sources','blocked') then null else (select count(*) from skillset.unified_canonical_sources_v1) end,
  'exports',jsonb_build_object('done',(select count(*) from skillset.unified_canonical_export_manifest_v1 where status='done'),
    'open',(select count(*) from skillset.unified_canonical_export_manifest_v1 where status<>'done')),
  'synthesis_candidates',(select count(*) from skillset.unified_synthesis_candidates_v1),
  'database_size',pg_size_pretty(pg_database_size(current_database())),
  'cron_active',exists(select 1 from cron.job where jobname='unified-corpus-autopilot-v1' and active))
from skillset.unified_corpus_control_v1 c where singleton=true
$function$;

revoke all on function public.skillset_unified_canonical_search_v1(text,text[],integer,text,integer) from public,anon,authenticated;
revoke all on function public.skillset_unified_corpus_tick_v1() from public,anon,authenticated;
revoke all on function public.skillset_unified_corpus_start_v1() from public,anon,authenticated;
revoke all on function public.skillset_unified_corpus_state_v1() from public,anon,authenticated;
grant execute on function public.skillset_unified_canonical_search_v1(text,text[],integer,text,integer) to service_role;
grant execute on function public.skillset_unified_corpus_tick_v1() to service_role;
grant execute on function public.skillset_unified_corpus_start_v1() to service_role;
grant execute on function public.skillset_unified_corpus_state_v1() to service_role;

select public.skillset_unified_corpus_start_v1();

commit;
