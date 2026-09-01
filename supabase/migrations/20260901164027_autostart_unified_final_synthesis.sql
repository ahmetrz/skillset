begin;

create table if not exists skillset.unified_synthesis_source_queue_v1 (
  canonical_hash text primary key,
  source_kind text not null check(source_kind in ('projected_db','gitskills_pack','unsupported')),
  input_path text,
  expected_materialized_hash text,
  status text not null default 'pending' check(status in ('pending','processing','ready','terminal')),
  attempts integer not null default 0,
  claimed_at timestamptz,
  output_path text,
  materialized_hash text,
  content_bytes integer,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists unified_synthesis_source_queue_v1_work_idx
  on skillset.unified_synthesis_source_queue_v1(status,source_kind,input_path,attempts);

create table if not exists skillset.unified_synthesis_bundle_queue_v1 (
  target_area text not null,
  category text not null,
  expected_components integer not null,
  status text not null default 'pending' check(status in ('pending','processing','done','terminal')),
  attempts integer not null default 0,
  claimed_at timestamptz,
  output_path text,
  component_count integer,
  artifact_sha256 text,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key(target_area,category)
);

create table if not exists skillset.unified_final_synthesis_control_v1 (
  singleton boolean primary key default true check(singleton),
  enabled boolean not null default true,
  status text not null default 'running' check(status in ('running','blocked','completed')),
  phase text not null default 'materialize' check(phase in ('materialize','compose','integrity','blocked','completed')),
  consecutive_failures integer not null default 0,
  detail jsonb not null default '{}'::jsonb,
  last_error text,
  started_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz
);

alter table skillset.unified_synthesis_source_queue_v1 enable row level security;
alter table skillset.unified_synthesis_bundle_queue_v1 enable row level security;
alter table skillset.unified_final_synthesis_control_v1 enable row level security;
revoke all on skillset.unified_synthesis_source_queue_v1 from public,anon,authenticated;
revoke all on skillset.unified_synthesis_bundle_queue_v1 from public,anon,authenticated;
revoke all on skillset.unified_final_synthesis_control_v1 from public,anon,authenticated;
grant select,insert,update,delete on skillset.unified_synthesis_source_queue_v1 to service_role;
grant select,insert,update,delete on skillset.unified_synthesis_bundle_queue_v1 to service_role;
grant select,insert,update,delete on skillset.unified_final_synthesis_control_v1 to service_role;
drop policy if exists unified_synthesis_source_service_role_v1 on skillset.unified_synthesis_source_queue_v1;
create policy unified_synthesis_source_service_role_v1 on skillset.unified_synthesis_source_queue_v1
  for all to service_role using(true) with check(true);
drop policy if exists unified_synthesis_bundle_service_role_v1 on skillset.unified_synthesis_bundle_queue_v1;
create policy unified_synthesis_bundle_service_role_v1 on skillset.unified_synthesis_bundle_queue_v1
  for all to service_role using(true) with check(true);
drop policy if exists unified_final_control_service_role_v1 on skillset.unified_final_synthesis_control_v1;
create policy unified_final_control_service_role_v1 on skillset.unified_final_synthesis_control_v1
  for all to service_role using(true) with check(true);

insert into skillset.unified_final_synthesis_control_v1(singleton)
values(true) on conflict(singleton) do nothing;

insert into skillset.unified_synthesis_source_queue_v1(
  canonical_hash,source_kind,input_path,expected_materialized_hash,status,last_error
)
select c.canonical_hash,
  case when p.projected_content_gzip is not null then 'projected_db'
       when m.input_path is not null then 'gitskills_pack' else 'unsupported' end,
  case when p.projected_content_gzip is null then m.input_path end,
  coalesce(p.projected_content_hash,c.canonical_hash),
  case when p.projected_content_gzip is not null or m.input_path is not null then 'pending' else 'terminal' end,
  case when p.projected_content_gzip is null and m.input_path is null then 'source_not_found' end
from (select distinct canonical_hash from skillset.unified_synthesis_candidates_v1) c
left join skillset.sdlc_projection_hash_v52 p
  on p.source_content_hash=c.canonical_hash and p.transformer_version='sdlc-projection-v5.2'
left join lateral (
  select x.input_path from skillset.gitskills_final_accept_members_v1 x
  where x.source_content_hash=c.canonical_hash order by x.score desc,x.input_path limit 1
) m on true
on conflict(canonical_hash) do update set
  source_kind=excluded.source_kind,input_path=excluded.input_path,
  expected_materialized_hash=excluded.expected_materialized_hash,
  status=case when skillset.unified_synthesis_source_queue_v1.status='ready' then 'ready' else excluded.status end,
  last_error=case when skillset.unified_synthesis_source_queue_v1.status='ready' then null else excluded.last_error end,
  updated_at=now();

insert into skillset.unified_synthesis_bundle_queue_v1(target_area,category,expected_components)
select target_area,category,count(*) from skillset.unified_synthesis_candidates_v1
group by target_area,category
on conflict(target_area,category) do update set expected_components=excluded.expected_components,
  status=case when skillset.unified_synthesis_bundle_queue_v1.status='done' then 'done' else 'pending' end,
  updated_at=now();

create or replace function public.skillset_unified_synthesis_claim_v1()
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','skillset'
as $function$
declare
  v_control skillset.unified_final_synthesis_control_v1%rowtype;
  v_path text; v_hashes text[]; v_rows jsonb; v_area text; v_category text; v_candidates jsonb;
begin
  if not pg_try_advisory_xact_lock(hashtextextended('unified-synthesis-claim-v1',0)) then
    return jsonb_build_object('kind','none','reason','claim_locked');
  end if;
  select * into v_control from skillset.unified_final_synthesis_control_v1 where singleton=true for update;
  if not v_control.enabled or v_control.status<>'running' then
    return jsonb_build_object('kind','none','status',v_control.status);
  end if;

  update skillset.unified_synthesis_source_queue_v1 set status=case when attempts>=4 then 'terminal' else 'pending' end,
    claimed_at=null,last_error=coalesce(last_error,'stale_claim'),updated_at=now()
  where status='processing' and claimed_at<now()-interval '5 minutes';
  update skillset.unified_synthesis_bundle_queue_v1 set status=case when attempts>=4 then 'terminal' else 'pending' end,
    claimed_at=null,last_error=coalesce(last_error,'stale_claim'),updated_at=now()
  where status='processing' and claimed_at<now()-interval '5 minutes';

  if v_control.phase='materialize' then
    select input_path into v_path from skillset.unified_synthesis_source_queue_v1
    where status='pending' and source_kind='gitskills_pack' and attempts<4
    group by input_path order by count(*) desc,input_path limit 1;
    if v_path is not null then
      select array_agg(canonical_hash order by canonical_hash) into v_hashes
      from skillset.unified_synthesis_source_queue_v1
      where status='pending' and source_kind='gitskills_pack' and input_path=v_path and attempts<4;
      update skillset.unified_synthesis_source_queue_v1 set status='processing',attempts=attempts+1,
        claimed_at=now(),last_error=null,updated_at=now() where canonical_hash=any(v_hashes);
      return jsonb_build_object('kind','source_gitskills','input_path',v_path,'hashes',v_hashes);
    end if;

    select array_agg(canonical_hash order by canonical_hash) into v_hashes from (
      select canonical_hash from skillset.unified_synthesis_source_queue_v1
      where status='pending' and source_kind='projected_db' and attempts<4
      order by canonical_hash for update skip locked limit 40
    ) q;
    if coalesce(array_length(v_hashes,1),0)>0 then
      update skillset.unified_synthesis_source_queue_v1 set status='processing',attempts=attempts+1,
        claimed_at=now(),last_error=null,updated_at=now() where canonical_hash=any(v_hashes);
      select jsonb_agg(jsonb_build_object('canonical_hash',q.canonical_hash,
        'projected_content_hash',p.projected_content_hash,
        'content_gzip_base64',encode(p.projected_content_gzip,'base64')) order by q.canonical_hash)
      into v_rows
      from skillset.unified_synthesis_source_queue_v1 q
      join skillset.sdlc_projection_hash_v52 p on p.source_content_hash=q.canonical_hash
       and p.transformer_version='sdlc-projection-v5.2'
      where q.canonical_hash=any(v_hashes);
      return jsonb_build_object('kind','source_projected','hashes',v_hashes,'rows',v_rows);
    end if;
    return jsonb_build_object('kind','none','reason','materialize_drained');
  end if;

  if v_control.phase='compose' then
    select target_area,category into v_area,v_category
    from skillset.unified_synthesis_bundle_queue_v1
    where status='pending' and attempts<4 order by target_area,category for update skip locked limit 1;
    if v_area is null then return jsonb_build_object('kind','none','reason','compose_drained'); end if;
    update skillset.unified_synthesis_bundle_queue_v1 set status='processing',attempts=attempts+1,
      claimed_at=now(),last_error=null,updated_at=now()
    where target_area=v_area and category=v_category;
    select jsonb_agg(jsonb_build_object('rank',c.rank,'canonical_hash',c.canonical_hash,
      'score',c.score,'occurrence_count',c.occurrence_count,'output_path',q.output_path) order by c.rank)
    into v_candidates from skillset.unified_synthesis_candidates_v1 c
    join skillset.unified_synthesis_source_queue_v1 q using(canonical_hash)
    where c.target_area=v_area and c.category=v_category and q.status='ready';
    return jsonb_build_object('kind','compose','target_area',v_area,'category',v_category,'candidates',v_candidates);
  end if;
  return jsonb_build_object('kind','none','phase',v_control.phase);
end
$function$;

create or replace function public.skillset_unified_synthesis_source_finish_v1(
  p_claimed_hashes text[],p_rows jsonb,p_output_path text,p_error text default null
) returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','skillset'
as $function$
declare v_ready integer; v_retry integer; v_terminal integer;
begin
  with r as (select * from jsonb_to_recordset(coalesce(p_rows,'[]'::jsonb))
    as x(canonical_hash text,materialized_hash text,content_bytes integer))
  update skillset.unified_synthesis_source_queue_v1 q set status='ready',claimed_at=null,
    output_path=p_output_path,materialized_hash=r.materialized_hash,content_bytes=r.content_bytes,
    last_error=null,updated_at=now() from r
  where q.canonical_hash=r.canonical_hash and q.canonical_hash=any(p_claimed_hashes)
    and r.materialized_hash=q.expected_materialized_hash;
  get diagnostics v_ready=row_count;
  update skillset.unified_synthesis_source_queue_v1 q set status=case when attempts>=4 then 'terminal' else 'pending' end,
    claimed_at=null,last_error=coalesce(p_error,'claimed_content_not_materialized'),updated_at=now()
  where q.canonical_hash=any(p_claimed_hashes) and q.status='processing';
  get diagnostics v_retry=row_count;
  select count(*) into v_terminal from skillset.unified_synthesis_source_queue_v1
    where canonical_hash=any(p_claimed_hashes) and status='terminal';
  return jsonb_build_object('ready',v_ready,'retry_or_terminal',v_retry,'terminal',v_terminal);
end
$function$;

create or replace function public.skillset_unified_synthesis_bundle_finish_v1(
  p_target_area text,p_category text,p_output_path text,p_component_count integer,
  p_artifact_sha256 text,p_error text default null
) returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','skillset'
as $function$
declare v_expected integer;
begin
  select expected_components into v_expected from skillset.unified_synthesis_bundle_queue_v1
  where target_area=p_target_area and category=p_category for update;
  update skillset.unified_synthesis_bundle_queue_v1 set
    status=case when p_error is null and p_output_path is not null and p_component_count=v_expected
                then 'done' when attempts>=4 then 'terminal' else 'pending' end,
    claimed_at=null,output_path=case when p_error is null then p_output_path else output_path end,
    component_count=case when p_error is null then p_component_count else component_count end,
    artifact_sha256=case when p_error is null then p_artifact_sha256 else artifact_sha256 end,
    last_error=case when p_error is null and p_component_count=v_expected then null
      else coalesce(p_error,format('component_count_mismatch:%s/%s',p_component_count,v_expected)) end,
    updated_at=now() where target_area=p_target_area and category=p_category;
  return jsonb_build_object('target_area',p_target_area,'category',p_category,'expected',v_expected,
    'received',p_component_count);
end
$function$;

create or replace function public.skillset_unified_final_synthesis_tick_v1()
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','skillset','net','cron','vault'
as $function$
declare
  v_control skillset.unified_final_synthesis_control_v1%rowtype;
  v_pending integer; v_processing integer; v_ready integer; v_terminal integer;
  v_bundle_pending integer; v_bundle_processing integer; v_bundle_done integer; v_bundle_terminal integer;
  v_http integer; v_slots integer; v_started integer:=0; v_rid bigint; v_jwt text; i integer;
begin
  if not pg_try_advisory_xact_lock(hashtextextended('unified-final-synthesis-tick-v1',0)) then
    return jsonb_build_object('started',0,'skipped','tick_locked');
  end if;
  select * into v_control from skillset.unified_final_synthesis_control_v1 where singleton=true for update;
  if not v_control.enabled then return jsonb_build_object('status',v_control.status,'enabled',false); end if;
  select decrypted_secret into v_jwt from vault.decrypted_secrets where name='unified_synthesis_anon_key_v1' limit 1;
  if v_jwt is null then raise exception 'synthesis_worker_jwt_missing'; end if;

  update skillset.unified_synthesis_source_queue_v1 set status=case when attempts>=4 then 'terminal' else 'pending' end,
    claimed_at=null,last_error=coalesce(last_error,'stale_claim'),updated_at=now()
  where status='processing' and claimed_at<now()-interval '5 minutes';
  update skillset.unified_synthesis_bundle_queue_v1 set status=case when attempts>=4 then 'terminal' else 'pending' end,
    claimed_at=null,last_error=coalesce(last_error,'stale_claim'),updated_at=now()
  where status='processing' and claimed_at<now()-interval '5 minutes';
  select count(*) filter(where status='pending'),count(*) filter(where status='processing'),
    count(*) filter(where status='ready'),count(*) filter(where status='terminal')
    into v_pending,v_processing,v_ready,v_terminal from skillset.unified_synthesis_source_queue_v1;
  select count(*) into v_http from net.http_request_queue
    where url like '%/functions/v1/skills-sh-search-behavior-probe%';

  if v_control.phase='materialize' then
    if v_terminal>0 then
      update skillset.unified_final_synthesis_control_v1 set enabled=false,status='blocked',phase='blocked',
        last_error='terminal_source_rows',detail=jsonb_build_object('ready',v_ready,'terminal',v_terminal),updated_at=now()
      where singleton=true;
      perform cron.unschedule('unified-final-synthesis-autopilot-v1');
      return jsonb_build_object('phase','blocked','terminal_sources',v_terminal);
    end if;
    if v_pending=0 and v_processing=0 and v_http=0 then
      update skillset.unified_final_synthesis_control_v1 set phase='compose',consecutive_failures=0,last_error=null,
        detail=jsonb_build_object('materialized',v_ready),updated_at=now() where singleton=true;
      return jsonb_build_object('phase','compose','transitioned',true,'materialized',v_ready);
    end if;
    v_slots:=greatest(0,4-v_processing-v_http);
  elsif v_control.phase='compose' then
    select count(*) filter(where status='pending'),count(*) filter(where status='processing'),
      count(*) filter(where status='done'),count(*) filter(where status='terminal')
      into v_bundle_pending,v_bundle_processing,v_bundle_done,v_bundle_terminal
      from skillset.unified_synthesis_bundle_queue_v1;
    if v_bundle_terminal>0 then
      update skillset.unified_final_synthesis_control_v1 set enabled=false,status='blocked',phase='blocked',
        last_error='terminal_bundle_rows',detail=jsonb_build_object('done',v_bundle_done,'terminal',v_bundle_terminal),updated_at=now()
      where singleton=true;
      perform cron.unschedule('unified-final-synthesis-autopilot-v1');
      return jsonb_build_object('phase','blocked','terminal_bundles',v_bundle_terminal);
    end if;
    if v_bundle_pending=0 and v_bundle_processing=0 and v_http=0 then
      update skillset.unified_final_synthesis_control_v1 set phase='integrity',consecutive_failures=0,last_error=null,
        detail=detail||jsonb_build_object('bundles_done',v_bundle_done),updated_at=now() where singleton=true;
      return jsonb_build_object('phase','integrity','transitioned',true,'bundles_done',v_bundle_done);
    end if;
    v_slots:=greatest(0,2-v_bundle_processing-v_http);
  elsif v_control.phase='integrity' then
    select count(*) filter(where status='ready'),count(*) filter(where status='terminal')
      into v_ready,v_terminal from skillset.unified_synthesis_source_queue_v1;
    select count(*) filter(where status='done'),count(*) filter(where status='terminal')
      into v_bundle_done,v_bundle_terminal from skillset.unified_synthesis_bundle_queue_v1;
    if v_ready<>(select count(distinct canonical_hash) from skillset.unified_synthesis_candidates_v1)
       or v_terminal<>0 or v_bundle_done<>(select count(*) from skillset.unified_synthesis_bundle_queue_v1)
       or v_bundle_terminal<>0 or exists(
         select 1 from skillset.unified_synthesis_bundle_queue_v1
         where component_count<>expected_components or output_path is null or artifact_sha256 is null) then
      update skillset.unified_final_synthesis_control_v1 set enabled=false,status='blocked',phase='blocked',
        last_error='final_synthesis_integrity_failed',updated_at=now() where singleton=true;
      perform cron.unschedule('unified-final-synthesis-autopilot-v1');
      return jsonb_build_object('phase','blocked','reason','integrity_failed');
    end if;
    update skillset.unified_final_synthesis_control_v1 set enabled=false,status='completed',phase='completed',last_error=null,
      detail=detail||jsonb_build_object('materialized',v_ready,'bundles_done',v_bundle_done,'integrity_passed',true),
      completed_at=now(),updated_at=now() where singleton=true;
    perform cron.unschedule('unified-final-synthesis-autopilot-v1');
    return jsonb_build_object('phase','completed','integrity_passed',true,'autostopped',true);
  else return jsonb_build_object('phase',v_control.phase,'status',v_control.status); end if;

  for i in 1..v_slots loop
    select net.http_post(
      url:='https://cxvvfgwdqgxczxmomztw.supabase.co/functions/v1/skills-sh-search-behavior-probe',
      headers:=jsonb_build_object('Content-Type','application/json','Authorization','Bearer '||v_jwt),
      body:='{}'::jsonb,timeout_milliseconds:=120000) into v_rid;
    v_started:=v_started+1;
  end loop;
  update skillset.unified_final_synthesis_control_v1 set consecutive_failures=0,last_error=null,
    detail=jsonb_build_object('phase',v_control.phase,'pending',v_pending,'processing',v_processing,
      'ready',v_ready,'terminal',v_terminal,'http_queued',v_http,'started',v_started),updated_at=now()
  where singleton=true;
  return jsonb_build_object('phase',v_control.phase,'pending',v_pending,'processing',v_processing,
    'ready',v_ready,'terminal',v_terminal,'http_queued',v_http,'started',v_started);
exception when others then
  update skillset.unified_final_synthesis_control_v1 set consecutive_failures=consecutive_failures+1,
    last_error=sqlerrm,updated_at=now() where singleton=true;
  return jsonb_build_object('ok',false,'error',sqlerrm,'sqlstate',sqlstate);
end
$function$;

create or replace function public.skillset_unified_final_synthesis_start_v1()
returns jsonb language plpgsql security definer set search_path to 'pg_catalog','public','skillset','cron'
as $function$
begin
  update skillset.unified_final_synthesis_control_v1 set enabled=true,status='running',phase='materialize',
    consecutive_failures=0,detail='{}'::jsonb,last_error=null,started_at=now(),updated_at=now(),completed_at=null
  where singleton=true;
  perform cron.unschedule(jobid) from cron.job where jobname='unified-final-synthesis-autopilot-v1';
  perform cron.schedule('unified-final-synthesis-autopilot-v1','5 seconds',
    'select public.skillset_unified_final_synthesis_tick_v1();');
  return public.skillset_unified_final_synthesis_tick_v1();
end
$function$;

create or replace function public.skillset_unified_final_synthesis_state_v1()
returns jsonb language sql security definer set search_path to 'pg_catalog','public','skillset','cron'
as $function$
select to_jsonb(c)||jsonb_build_object(
  'sources',jsonb_build_object(
    'total',(select count(*) from skillset.unified_synthesis_source_queue_v1),
    'pending',(select count(*) from skillset.unified_synthesis_source_queue_v1 where status='pending'),
    'processing',(select count(*) from skillset.unified_synthesis_source_queue_v1 where status='processing'),
    'ready',(select count(*) from skillset.unified_synthesis_source_queue_v1 where status='ready'),
    'terminal',(select count(*) from skillset.unified_synthesis_source_queue_v1 where status='terminal')),
  'bundles',jsonb_build_object(
    'total',(select count(*) from skillset.unified_synthesis_bundle_queue_v1),
    'pending',(select count(*) from skillset.unified_synthesis_bundle_queue_v1 where status='pending'),
    'processing',(select count(*) from skillset.unified_synthesis_bundle_queue_v1 where status='processing'),
    'done',(select count(*) from skillset.unified_synthesis_bundle_queue_v1 where status='done'),
    'terminal',(select count(*) from skillset.unified_synthesis_bundle_queue_v1 where status='terminal')),
  'cron_active',exists(select 1 from cron.job where jobname='unified-final-synthesis-autopilot-v1' and active))
from skillset.unified_final_synthesis_control_v1 c where singleton=true
$function$;

revoke all on function public.skillset_unified_synthesis_claim_v1() from public,anon,authenticated;
revoke all on function public.skillset_unified_synthesis_source_finish_v1(text[],jsonb,text,text) from public,anon,authenticated;
revoke all on function public.skillset_unified_synthesis_bundle_finish_v1(text,text,text,integer,text,text) from public,anon,authenticated;
revoke all on function public.skillset_unified_final_synthesis_tick_v1() from public,anon,authenticated;
revoke all on function public.skillset_unified_final_synthesis_start_v1() from public,anon,authenticated;
revoke all on function public.skillset_unified_final_synthesis_state_v1() from public,anon,authenticated;
grant execute on function public.skillset_unified_synthesis_claim_v1() to service_role;
grant execute on function public.skillset_unified_synthesis_source_finish_v1(text[],jsonb,text,text) to service_role;
grant execute on function public.skillset_unified_synthesis_bundle_finish_v1(text,text,text,integer,text,text) to service_role;
grant execute on function public.skillset_unified_final_synthesis_tick_v1() to service_role;
grant execute on function public.skillset_unified_final_synthesis_start_v1() to service_role;
grant execute on function public.skillset_unified_final_synthesis_state_v1() to service_role;

select public.skillset_unified_final_synthesis_start_v1();

commit;
