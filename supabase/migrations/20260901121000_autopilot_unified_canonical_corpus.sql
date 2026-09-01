begin;

create table if not exists skillset.unified_canonical_v1 (
  canonical_hash text primary key,
  projected_content_hash text not null,
  decision text not null default 'accept' check (decision = 'accept'),
  score integer not null,
  raw_structural_score integer not null,
  projection_score integer not null,
  quality_tier text not null,
  occurrence_count bigint not null default 0,
  in_gitskills boolean not null default false,
  in_skills_sh boolean not null default false,
  source_systems text[] not null default '{}'::text[],
  representative_repo text,
  representative_path text,
  representative_skill_id text,
  sdlc_categories text[] not null default '{}'::text[],
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists unified_canonical_v1_score_idx
  on skillset.unified_canonical_v1(score desc, canonical_hash);
create index if not exists unified_canonical_v1_categories_idx
  on skillset.unified_canonical_v1 using gin(sdlc_categories);

create table if not exists skillset.unified_canonical_sources_v1 (
  canonical_hash text not null references skillset.unified_canonical_v1(canonical_hash) on delete cascade,
  source_kind text not null check (source_kind in ('gitskills','skills.sh')),
  source_key text not null,
  repository text,
  path text,
  skill_id text,
  source_url text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key(canonical_hash, source_kind, source_key)
);

create index if not exists unified_canonical_sources_v1_key_idx
  on skillset.unified_canonical_sources_v1(source_kind, source_key);

create table if not exists skillset.unified_canonical_export_manifest_v1 (
  prefix text primary key check (prefix ~ '^[0-9a-f]{2}$'),
  status text not null default 'pending' check (status in ('pending','processing','done','error')),
  attempts integer not null default 0,
  row_count bigint,
  fingerprint text,
  last_error text,
  claimed_at timestamptz,
  finished_at timestamptz,
  updated_at timestamptz not null default now()
);

insert into skillset.unified_canonical_export_manifest_v1(prefix)
select lower(lpad(to_hex(i),2,'0')) from generate_series(0,255) i
on conflict(prefix) do nothing;

create table if not exists skillset.unified_synthesis_candidates_v1 (
  target_area text not null check (target_area in ('sdlc','social_media','global')),
  category text not null,
  rank integer not null,
  canonical_hash text not null references skillset.unified_canonical_v1(canonical_hash) on delete cascade,
  score integer not null,
  occurrence_count bigint not null,
  selection_reason jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  primary key(target_area, category, rank),
  unique(target_area, category, canonical_hash)
);

create table if not exists skillset.unified_corpus_control_v1 (
  singleton boolean primary key default true check(singleton),
  enabled boolean not null default true,
  status text not null default 'running' check(status in ('running','blocked','completed')),
  phase text not null default 'canonical'
    check(phase in ('canonical','sources','exports','synthesis','integrity','blocked','completed')),
  canonical_cursor text,
  source_cursor text,
  consecutive_failures integer not null default 0,
  detail jsonb not null default '{}'::jsonb,
  last_error text,
  started_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz
);

alter table skillset.unified_canonical_v1 enable row level security;
alter table skillset.unified_canonical_sources_v1 enable row level security;
alter table skillset.unified_canonical_export_manifest_v1 enable row level security;
alter table skillset.unified_synthesis_candidates_v1 enable row level security;
alter table skillset.unified_corpus_control_v1 enable row level security;

revoke all on table skillset.unified_canonical_v1 from public, anon, authenticated;
revoke all on table skillset.unified_canonical_sources_v1 from public, anon, authenticated;
revoke all on table skillset.unified_canonical_export_manifest_v1 from public, anon, authenticated;
revoke all on table skillset.unified_synthesis_candidates_v1 from public, anon, authenticated;
revoke all on table skillset.unified_corpus_control_v1 from public, anon, authenticated;

grant select, insert, update, delete on table skillset.unified_canonical_v1 to service_role;
grant select, insert, update, delete on table skillset.unified_canonical_sources_v1 to service_role;
grant select, insert, update, delete on table skillset.unified_canonical_export_manifest_v1 to service_role;
grant select, insert, update, delete on table skillset.unified_synthesis_candidates_v1 to service_role;
grant select, insert, update, delete on table skillset.unified_corpus_control_v1 to service_role;

drop policy if exists unified_canonical_service_role_v1 on skillset.unified_canonical_v1;
create policy unified_canonical_service_role_v1 on skillset.unified_canonical_v1
  for all to service_role using(true) with check(true);
drop policy if exists unified_canonical_sources_service_role_v1 on skillset.unified_canonical_sources_v1;
create policy unified_canonical_sources_service_role_v1 on skillset.unified_canonical_sources_v1
  for all to service_role using(true) with check(true);
drop policy if exists unified_export_service_role_v1 on skillset.unified_canonical_export_manifest_v1;
create policy unified_export_service_role_v1 on skillset.unified_canonical_export_manifest_v1
  for all to service_role using(true) with check(true);
drop policy if exists unified_synthesis_service_role_v1 on skillset.unified_synthesis_candidates_v1;
create policy unified_synthesis_service_role_v1 on skillset.unified_synthesis_candidates_v1
  for all to service_role using(true) with check(true);
drop policy if exists unified_control_service_role_v1 on skillset.unified_corpus_control_v1;
create policy unified_control_service_role_v1 on skillset.unified_corpus_control_v1
  for all to service_role using(true) with check(true);

insert into skillset.unified_corpus_control_v1(singleton)
values(true) on conflict(singleton) do nothing;

create or replace function public.skillset_unified_canonical_search_v1(
  p_query text default null,
  p_categories text[] default null,
  p_min_score integer default 70,
  p_after_hash text default null,
  p_limit integer default 100
)
returns table(
  canonical_hash text,
  score integer,
  quality_tier text,
  occurrence_count bigint,
  source_systems text[],
  representative_repo text,
  representative_path text,
  sdlc_categories text[]
)
language sql
security definer
set search_path to 'pg_catalog', 'public', 'skillset'
as $function$
  select c.canonical_hash,c.score,c.quality_tier,c.occurrence_count,
         c.source_systems,c.representative_repo,c.representative_path,c.sdlc_categories
  from skillset.unified_canonical_v1 c
  where c.score >= greatest(0,coalesce(p_min_score,70))
    and (p_after_hash is null or c.canonical_hash > p_after_hash)
    and (p_categories is null or c.sdlc_categories && p_categories)
    and (
      p_query is null or btrim(p_query)='' or
      concat_ws(' ',c.representative_repo,c.representative_path,c.representative_skill_id)
        ilike '%'||replace(replace(p_query,'%','\%'),'_','\_')||'%' escape '\'
    )
  order by c.canonical_hash
  limit greatest(1,least(coalesce(p_limit,100),1000))
$function$;

create or replace function public.skillset_unified_corpus_tick_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog','public','skillset','cron'
as $function$
declare
  v_control skillset.unified_corpus_control_v1%rowtype;
  v_hashes text[];
  v_last text;
  v_rows integer := 0;
  v_prefixes text[];
  v_prefix text;
  v_expected bigint := 0;
  v_actual bigint := 0;
  v_missing_sources bigint := 0;
  v_export_rows bigint := 0;
  v_export_bad bigint := 0;
  v_candidates bigint := 0;
begin
  if not pg_try_advisory_xact_lock(hashtextextended('unified-corpus-tick-v1',0)) then
    return jsonb_build_object('started',0,'skipped','tick_locked');
  end if;

  select * into v_control
  from skillset.unified_corpus_control_v1
  where singleton=true
  for update;

  if not v_control.enabled then
    return jsonb_build_object('status',v_control.status,'phase',v_control.phase,'enabled',false);
  end if;

  if v_control.phase='canonical' then
    select array_agg(source_content_hash order by source_content_hash)
    into v_hashes
    from (
      select source_content_hash
      from (
        select source_content_hash from public.gitskills_final_accept_canonical_v1
        union
        select source_content_hash
        from skillset.evaluation_v31h_cache
        where implementation_rev='3.1h' and decision='accept'
      ) u
      where v_control.canonical_cursor is null or source_content_hash>v_control.canonical_cursor
      order by source_content_hash
      limit 5000
    ) b;

    if coalesce(array_length(v_hashes,1),0)=0 then
      update skillset.unified_corpus_control_v1
      set phase='sources',source_cursor=null,consecutive_failures=0,
          detail=jsonb_build_object('canonical_rows',(select count(*) from skillset.unified_canonical_v1)),
          updated_at=now()
      where singleton=true;
      return jsonb_build_object('phase','sources','transitioned',true);
    end if;

    with g as (
      select * from public.gitskills_final_accept_canonical_v1
      where source_content_hash=any(v_hashes)
    ), e as (
      select * from skillset.evaluation_v31h_cache
      where implementation_rev='3.1h' and decision='accept'
        and source_content_hash=any(v_hashes)
    ), sc as (
      select content_hash,count(*)::bigint n,min(id) representative_skill_id
      from skillset.skills
      where content_hash=any(v_hashes)
      group by content_hash
    ), rep as (
      select s.content_hash,s.id,s.repo,s.source_url
      from skillset.skills s
      join sc on sc.representative_skill_id=s.id
    )
    insert into skillset.unified_canonical_v1(
      canonical_hash,projected_content_hash,score,raw_structural_score,projection_score,
      quality_tier,occurrence_count,in_gitskills,in_skills_sh,source_systems,
      representative_repo,representative_path,representative_skill_id,sdlc_categories,
      metadata,updated_at
    )
    select h,
      coalesce(e.projected_content_hash,g.projected_content_hash),
      greatest(coalesce(g.score,0),coalesce(e.score,0)),
      greatest(coalesce(g.raw_structural_score,0),coalesce(e.raw_structural_score,0)),
      greatest(coalesce(g.projection_score,0),coalesce(e.projection_score,0)),
      case when greatest(coalesce(g.score,0),coalesce(e.score,0))>=85 then 'exceptional'
           when greatest(coalesce(g.score,0),coalesce(e.score,0))>=75 then 'strong'
           else 'accepted' end,
      coalesce(g.occurrence_count,0)+coalesce(sc.n,0),
      g.source_content_hash is not null,e.source_content_hash is not null,
      array_remove(array[
        case when g.source_content_hash is not null then 'gitskills' end,
        case when e.source_content_hash is not null then 'skills.sh' end
      ],null),
      coalesce(g.representative_repo,rep.repo),
      coalesce(g.representative_path,rep.source_url),
      coalesce(e.representative_skill_id,rep.id),
      coalesce(p.sdlc_categories,'{}'::text[]),
      jsonb_build_object(
        'gitskills_score',g.score,'skills_sh_score',e.score,
        'gitskills_occurrences',g.occurrence_count,'skills_sh_occurrences',sc.n
      ),
      now()
    from unnest(v_hashes) h
    left join g on g.source_content_hash=h
    left join e on e.source_content_hash=h
    left join sc on sc.content_hash=h
    left join rep on rep.content_hash=h
    left join skillset.sdlc_projection_hash_v52 p
      on p.source_content_hash=h and p.transformer_version='sdlc-projection-v5.2'
    on conflict(canonical_hash) do update set
      projected_content_hash=excluded.projected_content_hash,
      score=excluded.score,raw_structural_score=excluded.raw_structural_score,
      projection_score=excluded.projection_score,quality_tier=excluded.quality_tier,
      occurrence_count=excluded.occurrence_count,in_gitskills=excluded.in_gitskills,
      in_skills_sh=excluded.in_skills_sh,source_systems=excluded.source_systems,
      representative_repo=excluded.representative_repo,
      representative_path=excluded.representative_path,
      representative_skill_id=excluded.representative_skill_id,
      sdlc_categories=excluded.sdlc_categories,metadata=excluded.metadata,updated_at=now();
    get diagnostics v_rows=row_count;
    v_last:=v_hashes[array_length(v_hashes,1)];

    update skillset.unified_corpus_control_v1
    set canonical_cursor=v_last,consecutive_failures=0,last_error=null,
        detail=jsonb_build_object('batch_rows',v_rows,'cursor',v_last,
          'canonical_rows',(select count(*) from skillset.unified_canonical_v1)),
        updated_at=now()
    where singleton=true;
    return jsonb_build_object('phase','canonical','batch_rows',v_rows,'cursor',v_last);
  end if;

  if v_control.phase='sources' then
    select array_agg(canonical_hash order by canonical_hash)
    into v_hashes
    from (
      select canonical_hash from skillset.unified_canonical_v1
      where v_control.source_cursor is null or canonical_hash>v_control.source_cursor
      order by canonical_hash limit 2000
    ) b;

    if coalesce(array_length(v_hashes,1),0)=0 then
      update skillset.unified_canonical_export_manifest_v1
      set status='pending',attempts=0,row_count=null,fingerprint=null,
          last_error=null,claimed_at=null,finished_at=null,updated_at=now();
      update skillset.unified_corpus_control_v1
      set phase='exports',consecutive_failures=0,
          detail=jsonb_build_object('source_rows',(select count(*) from skillset.unified_canonical_sources_v1)),
          updated_at=now()
      where singleton=true;
      return jsonb_build_object('phase','exports','transitioned',true);
    end if;

    insert into skillset.unified_canonical_sources_v1(
      canonical_hash,source_kind,source_key,repository,path,source_url,metadata,updated_at
    )
    select m.source_content_hash,'gitskills',
      'gitskills:'||md5(concat_ws('|',m.input_path,m.repo_full_name,m.path)),
      m.repo_full_name,m.path,
      'https://github.com/'||m.repo_full_name||'/blob/HEAD/'||m.path,
      jsonb_build_object('input_path',m.input_path,'score',m.score),now()
    from skillset.gitskills_final_accept_members_v1 m
    where m.source_content_hash=any(v_hashes)
    on conflict(canonical_hash,source_kind,source_key) do update
    set repository=excluded.repository,path=excluded.path,source_url=excluded.source_url,
        metadata=excluded.metadata,updated_at=now();

    insert into skillset.unified_canonical_sources_v1(
      canonical_hash,source_kind,source_key,repository,path,skill_id,source_url,metadata,updated_at
    )
    select s.content_hash,'skills.sh','skills.sh:'||s.id,s.repo,null,s.id,
      coalesce(s.source_url,s.skill_url),
      jsonb_build_object('skill_name',s.skill_name,'source',s.source,'installs',s.installs),now()
    from skillset.skills s
    where s.content_hash=any(v_hashes)
    on conflict(canonical_hash,source_kind,source_key) do update
    set repository=excluded.repository,skill_id=excluded.skill_id,
        source_url=excluded.source_url,metadata=excluded.metadata,updated_at=now();

    v_last:=v_hashes[array_length(v_hashes,1)];
    update skillset.unified_corpus_control_v1
    set source_cursor=v_last,consecutive_failures=0,last_error=null,
        detail=jsonb_build_object('cursor',v_last,
          'source_rows',(select count(*) from skillset.unified_canonical_sources_v1)),
        updated_at=now()
    where singleton=true;
    return jsonb_build_object('phase','sources','hashes',array_length(v_hashes,1),'cursor',v_last);
  end if;

  if v_control.phase='exports' then
    select array_agg(prefix order by prefix) into v_prefixes
    from (
      select prefix from skillset.unified_canonical_export_manifest_v1
      where status in ('pending','error') and attempts<4
      order by prefix for update skip locked limit 8
    ) p;

    if coalesce(array_length(v_prefixes,1),0)=0 then
      if exists(select 1 from skillset.unified_canonical_export_manifest_v1 where status<>'done') then
        raise exception 'export_terminal_or_processing_rows';
      end if;
      update skillset.unified_corpus_control_v1
      set phase='synthesis',consecutive_failures=0,
          detail=jsonb_build_object('export_prefixes',256),updated_at=now()
      where singleton=true;
      return jsonb_build_object('phase','synthesis','transitioned',true);
    end if;

    foreach v_prefix in array v_prefixes loop
      update skillset.unified_canonical_export_manifest_v1
      set status='processing',attempts=attempts+1,claimed_at=now(),updated_at=now()
      where prefix=v_prefix;
      update skillset.unified_canonical_export_manifest_v1 m
      set status='done',row_count=x.n,fingerprint=x.fp,last_error=null,
          claimed_at=null,finished_at=now(),updated_at=now()
      from (
        select count(*)::bigint n,
          md5(coalesce(string_agg(canonical_hash||':'||projected_content_hash,
            ',' order by canonical_hash),'')) fp
        from skillset.unified_canonical_v1 where left(canonical_hash,2)=v_prefix
      ) x
      where m.prefix=v_prefix;
    end loop;
    return jsonb_build_object('phase','exports','prefixes_done',array_length(v_prefixes,1));
  end if;

  if v_control.phase='synthesis' then
    truncate table skillset.unified_synthesis_candidates_v1;

    insert into skillset.unified_synthesis_candidates_v1(
      target_area,category,rank,canonical_hash,score,occurrence_count,selection_reason
    )
    select 'sdlc',category,rn,canonical_hash,score,occurrence_count,
      jsonb_build_object('basis','sdlc_category_pareto','quality_tier',quality_tier)
    from (
      select c.canonical_hash,c.score,c.occurrence_count,c.quality_tier,category,
        row_number() over(partition by category order by c.score desc,c.occurrence_count desc,c.canonical_hash) rn
      from skillset.unified_canonical_v1 c
      cross join lateral unnest(c.sdlc_categories) category
    ) x where rn<=100;

    insert into skillset.unified_synthesis_candidates_v1(
      target_area,category,rank,canonical_hash,score,occurrence_count,selection_reason
    )
    select 'global','top_quality',rn,canonical_hash,score,occurrence_count,
      jsonb_build_object('basis','global_quality_occurrence_pareto')
    from (
      select c.*,row_number() over(order by score desc,occurrence_count desc,canonical_hash) rn
      from skillset.unified_canonical_v1 c
    ) x where rn<=500;

    with social_pool as (
      select distinct c.canonical_hash,c.score,c.occurrence_count,k.category
      from skillset.unified_canonical_v1 c
      join skillset.unified_canonical_sources_v1 s using(canonical_hash)
      cross join lateral (
        values
          ('content_creation','(content|creator|social|campaign|post|publishing)'),
          ('video','(video|youtube|tiktok|reels|shorts)'),
          ('image','(image|visual|design|instagram|thumbnail)'),
          ('copywriting','(copy|writing|caption|headline|storytelling)'),
          ('analytics','(analytics|metric|engagement|seo|growth)'),
          ('automation','(automation|workflow|schedule|orchestrat)')
      ) k(category,pattern)
      where lower(concat_ws(' ',s.repository,s.path,s.skill_id,s.metadata::text)) ~ k.pattern
    )
    insert into skillset.unified_synthesis_candidates_v1(
      target_area,category,rank,canonical_hash,score,occurrence_count,selection_reason
    )
    select 'social_media',category,rn,canonical_hash,score,occurrence_count,
      jsonb_build_object('basis','metadata_keyword_bridge','requires_domain_review',true)
    from (
      select p.*,row_number() over(partition by category order by score desc,occurrence_count desc,canonical_hash) rn
      from social_pool p
    ) x where rn<=100;

    update skillset.unified_corpus_control_v1
    set phase='integrity',consecutive_failures=0,last_error=null,
        detail=jsonb_build_object('synthesis_candidates',
          (select count(*) from skillset.unified_synthesis_candidates_v1)),
        updated_at=now()
    where singleton=true;
    return jsonb_build_object('phase','integrity','transitioned',true);
  end if;

  if v_control.phase='integrity' then
    select count(*) into v_expected from (
      select source_content_hash from public.gitskills_final_accept_canonical_v1
      union
      select source_content_hash from skillset.evaluation_v31h_cache
      where implementation_rev='3.1h' and decision='accept'
    ) u;
    select count(*) into v_actual from skillset.unified_canonical_v1;
    select count(*) into v_missing_sources
    from skillset.unified_canonical_v1 c
    where not exists(select 1 from skillset.unified_canonical_sources_v1 s where s.canonical_hash=c.canonical_hash);
    select coalesce(sum(row_count),0),count(*) filter(where status<>'done' or fingerprint is null)
    into v_export_rows,v_export_bad
    from skillset.unified_canonical_export_manifest_v1;
    select count(*) into v_candidates from skillset.unified_synthesis_candidates_v1;

    if v_expected<>v_actual or v_missing_sources<>0 or v_export_rows<>v_actual
       or v_export_bad<>0 or v_candidates=0 then
      update skillset.unified_corpus_control_v1
      set status='blocked',phase='blocked',last_error='integrity_gate_failed',
          detail=jsonb_build_object('expected',v_expected,'actual',v_actual,
            'missing_sources',v_missing_sources,'export_rows',v_export_rows,
            'export_bad',v_export_bad,'synthesis_candidates',v_candidates),
          updated_at=now()
      where singleton=true;
      perform cron.unschedule('unified-corpus-autopilot-v1');
      return jsonb_build_object('phase','blocked','reason','integrity_gate_failed');
    end if;

    update skillset.unified_corpus_control_v1
    set enabled=false,status='completed',phase='completed',last_error=null,
        detail=jsonb_build_object('expected',v_expected,'canonical',v_actual,
          'source_rows',(select count(*) from skillset.unified_canonical_sources_v1),
          'export_prefixes',256,'export_rows',v_export_rows,
          'synthesis_candidates',v_candidates,'integrity_passed',true),
        completed_at=now(),updated_at=now()
    where singleton=true;
    perform cron.unschedule('unified-corpus-autopilot-v1');
    return jsonb_build_object('phase','completed','integrity_passed',true,'autostopped',true);
  end if;

  return jsonb_build_object('phase',v_control.phase,'status',v_control.status);
exception when others then
  update skillset.unified_corpus_control_v1
  set consecutive_failures=consecutive_failures+1,last_error=sqlerrm,
      status=case when consecutive_failures+1>=5 then 'blocked' else status end,
      phase=case when consecutive_failures+1>=5 then 'blocked' else phase end,
      detail=detail||jsonb_build_object('last_failure_at',now(),'sqlstate',sqlstate),
      updated_at=now()
  where singleton=true;
  if (select consecutive_failures>=5 from skillset.unified_corpus_control_v1 where singleton=true) then
    perform cron.unschedule('unified-corpus-autopilot-v1');
  end if;
  return jsonb_build_object('ok',false,'error',sqlerrm,'sqlstate',sqlstate);
end
$function$;

create or replace function public.skillset_unified_corpus_start_v1()
returns jsonb
language plpgsql
security definer
set search_path to 'pg_catalog','public','skillset','cron'
as $function$
begin
  insert into skillset.unified_corpus_control_v1(singleton)
  values(true) on conflict(singleton) do nothing;
  update skillset.unified_corpus_control_v1
  set enabled=true,status='running',phase='canonical',canonical_cursor=null,source_cursor=null,
      consecutive_failures=0,detail='{}'::jsonb,last_error=null,
      started_at=now(),updated_at=now(),completed_at=null
  where singleton=true;
  perform cron.unschedule(jobid) from cron.job where jobname='unified-corpus-autopilot-v1';
  perform cron.schedule('unified-corpus-autopilot-v1','3 seconds',
    'select public.skillset_unified_corpus_tick_v1();');
  return public.skillset_unified_corpus_tick_v1();
end
$function$;

create or replace function public.skillset_unified_corpus_state_v1()
returns jsonb
language sql
security definer
set search_path to 'pg_catalog','public','skillset','cron'
as $function$
select to_jsonb(c)||jsonb_build_object(
  'canonical_rows',(select count(*) from skillset.unified_canonical_v1),
  'source_rows',(select count(*) from skillset.unified_canonical_sources_v1),
  'exports',jsonb_build_object(
    'done',(select count(*) from skillset.unified_canonical_export_manifest_v1 where status='done'),
    'open',(select count(*) from skillset.unified_canonical_export_manifest_v1 where status<>'done')
  ),
  'synthesis_candidates',(select count(*) from skillset.unified_synthesis_candidates_v1),
  'cron_active',exists(select 1 from cron.job where jobname='unified-corpus-autopilot-v1' and active)
)
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
