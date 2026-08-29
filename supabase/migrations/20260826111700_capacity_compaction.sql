-- Capacity-aware semantic compaction for the SKILL.md corpus.
-- Safe order: profile -> candidate -> full-content validation -> apply.
-- Below 650 MiB this pipeline is dormant. At/above 650 MiB it profiles and
-- analyzes coverage; at/above 780 MiB batch sizes increase. Target is <550 MiB.

alter table skillset.skill_files
  add column if not exists active boolean not null default true,
  add column if not exists compaction_status text not null default 'none',
  add column if not exists compacted_into_file_id text,
  add column if not exists compaction_manifest jsonb,
  add column if not exists physical_deleted_at timestamptz,
  add column if not exists content_fidelity text not null default 'exact'
    check (content_fidelity in ('exact','derived'));

alter table skillset.skills
  add column if not exists content_fidelity text not null default 'exact'
    check (content_fidelity in ('exact','reconstructed','derived')),
  add column if not exists compaction_status text not null default 'none',
  add column if not exists compacted_into_file_id text;

create table if not exists skillset.compaction_state (
  singleton boolean primary key default true check (singleton),
  enabled boolean not null default true,
  start_at_bytes bigint not null default 681574400,
  aggressive_at_bytes bigint not null default 817889280,
  target_bytes bigint not null default 576716800,
  profile_version integer not null default 1,
  mode text not null default 'idle',
  last_run_at timestamptz,
  last_error text,
  updated_at timestamptz not null default now()
);
insert into skillset.compaction_state(singleton) values(true) on conflict(singleton) do nothing;

create table if not exists skillset.compaction_profiles (
  file_id text primary key references skillset.skill_files(file_id) on delete cascade,
  content_sha256 text,
  unit_hashes text[] not null,
  unit_count integer not null,
  normalized_bytes integer not null,
  profile_version integer not null default 1,
  profiled_at timestamptz not null default now(),
  candidate_scanned_at timestamptz
);
create index if not exists idx_compaction_profiles_units on skillset.compaction_profiles using gin(unit_hashes);
create index if not exists idx_compaction_profiles_count on skillset.compaction_profiles(unit_count);

create table if not exists skillset.compaction_candidates (
  candidate_id bigserial primary key,
  victim_file_id text not null references skillset.skill_files(file_id),
  survivor_file_ids text[] not null,
  action text not null check (action in ('delete_covered','merge_then_delete')),
  coverage_ratio numeric(6,5) not null,
  victim_unit_count integer not null,
  covered_unit_count integer not null,
  unique_unit_hashes text[] not null default '{}',
  estimated_saved_bytes bigint not null default 0,
  status text not null default 'proposed',
  validation jsonb,
  created_at timestamptz not null default now(),
  validating_at timestamptz,
  validated_at timestamptz,
  applied_at timestamptz
);
create index if not exists idx_compaction_candidates_status on skillset.compaction_candidates(status,estimated_saved_bytes desc);

create table if not exists skillset.derived_skill_files (
  derived_file_id text primary key,
  base_file_id text not null references skillset.skill_files(file_id),
  source_file_ids text[] not null,
  content_sha256 text not null,
  content_object_key text not null,
  content_bytes integer not null,
  compressed_bytes integer not null,
  merge_manifest jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists skillset.compaction_provenance (
  provenance_id bigserial primary key,
  removed_file_id text not null,
  survivor_file_ids text[] not null,
  derived_file_id text,
  action text not null,
  manifest jsonb not null,
  removed_object_key text,
  removed_content_sha256 text,
  removed_at timestamptz not null default now()
);

create or replace function public.skillset_compaction_needed()
returns boolean language sql stable security definer set search_path='skillset','public' as $$
  select coalesce((select sum(compressed_bytes)::bigint from skillset.skill_files where active and content_object_key is not null),0)
       >= (select start_at_bytes from skillset.compaction_state where singleton=true);
$$;

create or replace function public.skillset_compaction_profile_version()
returns integer language sql stable security definer set search_path='skillset' as $$
 select profile_version from skillset.compaction_state where singleton=true;
$$;

create or replace function public.skillset_claim_compaction_profiles(p_limit integer default 25, p_force boolean default false)
returns table(file_id text, content_object_key text, content_sha256 text)
language plpgsql security definer set search_path='skillset','public' as $$
begin
  if not p_force and not public.skillset_compaction_needed() then return; end if;
  return query
  select f.file_id,f.content_object_key,f.content_sha256
  from skillset.skill_files f
  left join skillset.compaction_profiles p on p.file_id=f.file_id and p.profile_version=(select profile_version from skillset.compaction_state where singleton=true)
  where f.active and f.content_object_key is not null and p.file_id is null
  order by coalesce(f.compressed_bytes,0) desc,f.file_id
  limit greatest(1,least(p_limit,100));
end $$;

create or replace function public.skillset_store_compaction_profile(
 p_file_id text,p_sha text,p_unit_hashes text[],p_unit_count integer,p_normalized_bytes integer,p_profile_version integer default 1)
returns void language plpgsql security definer set search_path='skillset','public' as $$
begin
 insert into skillset.compaction_profiles(file_id,content_sha256,unit_hashes,unit_count,normalized_bytes,profile_version,profiled_at)
 values(p_file_id,p_sha,p_unit_hashes,p_unit_count,p_normalized_bytes,p_profile_version,now())
 on conflict(file_id) do update set content_sha256=excluded.content_sha256,unit_hashes=excluded.unit_hashes,
 unit_count=excluded.unit_count,normalized_bytes=excluded.normalized_bytes,profile_version=excluded.profile_version,profiled_at=now();
end $$;

create or replace function public.skillset_claim_compaction_victims(p_limit integer default 10, p_force boolean default false)
returns table(file_id text, unit_hashes text[], unit_count integer, compressed_bytes integer)
language plpgsql security definer set search_path='skillset','public' as $$
begin
 if not p_force and not public.skillset_compaction_needed() then return; end if;
 return query
 with picked as (
   select p.file_id from skillset.compaction_profiles p join skillset.skill_files f on f.file_id=p.file_id
   where f.active and (p.candidate_scanned_at is null or p.candidate_scanned_at<now()-interval '7 days')
   order by coalesce(f.compressed_bytes,0),p.unit_count,p.file_id
   for update of p skip locked limit greatest(1,least(p_limit,50))
 ), upd as (
   update skillset.compaction_profiles p set candidate_scanned_at=now() from picked x where p.file_id=x.file_id
   returning p.file_id,p.unit_hashes,p.unit_count
 )
 select u.file_id,u.unit_hashes,u.unit_count,coalesce(f.compressed_bytes,0)
 from upd u join skillset.skill_files f on f.file_id=u.file_id;
end $$;

create or replace function public.skillset_compaction_survivors(p_victim_file_id text,p_limit integer default 12)
returns table(file_id text,unit_hashes text[],unit_count integer,compressed_bytes integer,overlap_count integer)
language sql stable security definer set search_path='skillset','public' as $$
 with v as (select unit_hashes,unit_count from skillset.compaction_profiles where file_id=p_victim_file_id),
 c as (
   select p.file_id,p.unit_hashes,p.unit_count,coalesce(f.compressed_bytes,0) compressed_bytes,
          (select count(*)::int from unnest((select unit_hashes from v)) x where x=any(p.unit_hashes)) overlap_count
   from skillset.compaction_profiles p join skillset.skill_files f on f.file_id=p.file_id,v
   where p.file_id<>p_victim_file_id and f.active
     and p.unit_count>=greatest(1,ceil(v.unit_count*0.40)::int)
     and p.unit_hashes && v.unit_hashes
 )
 select * from c where overlap_count>0 order by overlap_count desc,unit_count desc,compressed_bytes desc,file_id
 limit greatest(1,least(p_limit,30));
$$;

create or replace function public.skillset_record_compaction_candidate(
 p_victim_file_id text,p_survivor_file_ids text[],p_action text,p_coverage numeric,p_victim_units integer,p_covered_units integer,p_unique_hashes text[],p_estimated_saved bigint,p_validation jsonb)
returns bigint language plpgsql security definer set search_path='skillset','public' as $$
declare v_id bigint;begin
 if exists(select 1 from skillset.compaction_candidates where victim_file_id=p_victim_file_id and status in ('proposed','validated')) then
   select candidate_id into v_id from skillset.compaction_candidates where victim_file_id=p_victim_file_id and status in ('proposed','validated') order by candidate_id desc limit 1;
   return v_id;
 end if;
 insert into skillset.compaction_candidates(victim_file_id,survivor_file_ids,action,coverage_ratio,victim_unit_count,covered_unit_count,unique_unit_hashes,estimated_saved_bytes,status,validation)
 values(p_victim_file_id,p_survivor_file_ids,p_action,p_coverage,p_victim_units,p_covered_units,coalesce(p_unique_hashes,'{}'),greatest(0,p_estimated_saved),'proposed',p_validation)
 returning candidate_id into v_id; return v_id;
end $$;

create or replace function public.skillset_get_file_storage(p_file_id text)
returns table(file_id text,logical_skill_id text,content_sha256 text,content_object_key text,content_bytes integer,compressed_bytes integer,content_fidelity text)
language sql stable security definer set search_path='skillset' as $$
 select f.file_id,f.logical_skill_id,f.content_sha256,f.content_object_key,f.content_bytes,f.compressed_bytes,f.content_fidelity
 from skillset.skill_files f where f.file_id=p_file_id and f.active;
$$;

create or replace function public.skillset_object_active_refcount(p_object_key text)
returns integer language sql stable security definer set search_path='skillset','public' as $$
 select ((select count(*) from skillset.skill_files where active and content_object_key=p_object_key)
   +(select count(*) from skillset.skills where retrieval_status='ok' and content_object_key=p_object_key)
   +(select count(*) from skillset.derived_skill_files where content_object_key=p_object_key))::int;
$$;

create or replace function public.skillset_object_other_refcount(p_object_key text,p_file_id text,p_logical_skill_id text)
returns integer language sql stable security definer set search_path='skillset','public' as $$
 select ((select count(*) from skillset.skill_files f where f.active and f.content_object_key=p_object_key and f.file_id<>p_file_id)
   +(select count(*) from skillset.skills s where s.retrieval_status='ok' and s.content_object_key=p_object_key and (p_logical_skill_id is null or s.id<>p_logical_skill_id))
   +(select count(*) from skillset.derived_skill_files d where d.content_object_key=p_object_key))::int;
$$;

create or replace function public.skillset_claim_compaction_candidate_for_validation(p_force boolean default false)
returns table(candidate_id bigint,victim_file_id text,survivor_file_ids text[],action text)
language plpgsql security definer set search_path='skillset','public' as $$
begin
 if not p_force and not public.skillset_compaction_needed() then return; end if;
 return query with picked as (
   select c.candidate_id from skillset.compaction_candidates c
   where c.status='proposed' or (c.status='validating' and c.validating_at<now()-interval '20 minutes')
   order by c.estimated_saved_bytes desc,c.candidate_id for update skip locked limit 1
 ),upd as (
   update skillset.compaction_candidates c set status='validating',validating_at=now() from picked p where c.candidate_id=p.candidate_id
   returning c.candidate_id,c.victim_file_id,c.survivor_file_ids,c.action
 ) select * from upd;
end $$;

create or replace function public.skillset_finish_compaction_validation(p_candidate_id bigint,p_valid boolean,p_validation jsonb,p_estimated_saved bigint,p_error text default null)
returns void language plpgsql security definer set search_path='skillset' as $$
begin
 update skillset.compaction_candidates set status=case when p_valid then 'validated' else 'rejected' end,
   validation=coalesce(validation,'{}'::jsonb)||coalesce(p_validation,'{}'::jsonb)||jsonb_build_object('validation_error',p_error),
   estimated_saved_bytes=greatest(0,coalesce(p_estimated_saved,0)),validated_at=now()
 where candidate_id=p_candidate_id;
end $$;

create or replace function public.skillset_claim_validated_compaction()
returns table(candidate_id bigint,victim_file_id text,survivor_file_ids text[],action text,validation jsonb)
language plpgsql security definer set search_path='skillset','public' as $$
begin
 if not public.skillset_compaction_needed() then return; end if;
 return query with picked as (
   select c.candidate_id from skillset.compaction_candidates c where c.status='validated'
   order by c.estimated_saved_bytes desc,c.candidate_id for update skip locked limit 1
 ),upd as (
   update skillset.compaction_candidates c set status='applying' from picked p where c.candidate_id=p.candidate_id
   returning c.candidate_id,c.victim_file_id,c.survivor_file_ids,c.action,c.validation
 ) select * from upd;
end $$;

create or replace function public.skillset_apply_compaction_metadata(
 p_candidate_id bigint,p_victim_file_id text,p_primary_survivor_file_id text,p_survivor_file_ids text[],p_action text,
 p_derived_file_id text,p_derived_sha text,p_derived_object_key text,p_derived_bytes integer,p_derived_gzip_bytes integer,
 p_manifest jsonb,p_removed_object_key text,p_removed_sha text,p_saved_bytes bigint)
returns void language plpgsql security definer set search_path='skillset','public' as $$
declare v_primary skillset.skill_files%rowtype; v_victim_logical text;begin
 select logical_skill_id into v_victim_logical from skillset.skill_files where file_id=p_victim_file_id;
 select * into v_primary from skillset.skill_files where file_id=p_primary_survivor_file_id for update;
 if not found then raise exception 'primary survivor missing'; end if;
 if p_action='merge_then_delete' then
   insert into skillset.derived_skill_files(derived_file_id,base_file_id,source_file_ids,content_sha256,content_object_key,content_bytes,compressed_bytes,merge_manifest)
   values(p_derived_file_id,p_primary_survivor_file_id,array_append(p_survivor_file_ids,p_victim_file_id),p_derived_sha,p_derived_object_key,p_derived_bytes,p_derived_gzip_bytes,p_manifest)
   on conflict(derived_file_id) do nothing;
   update skillset.skill_files set content_sha256=p_derived_sha,content_object_key=p_derived_object_key,content_bytes=p_derived_bytes,
     compressed_bytes=p_derived_gzip_bytes,content_fidelity='derived',compaction_status='derived-survivor',compaction_manifest=p_manifest,updated_at=now()
   where file_id=p_primary_survivor_file_id;
   if v_primary.logical_skill_id is not null then
     update skillset.skills set content_sha256=p_derived_sha,content_object_key=p_derived_object_key,content_bytes=p_derived_bytes,
       compressed_bytes=p_derived_gzip_bytes,content_fidelity='derived',compaction_status='derived-survivor',updated_at=now()
     where id=v_primary.logical_skill_id;
   end if;
 end if;
 update skillset.skill_files set active=false,compaction_status='compacted',compacted_into_file_id=p_primary_survivor_file_id,
   compaction_manifest=p_manifest,content_object_key=null,updated_at=now() where file_id=p_victim_file_id;
 if v_victim_logical is not null then
   update skillset.skills set retrieval_status='compacted',compaction_status='compacted',compacted_into_file_id=p_primary_survivor_file_id,
     content_object_key=null,updated_at=now() where id=v_victim_logical;
 end if;
 insert into skillset.compaction_provenance(removed_file_id,survivor_file_ids,derived_file_id,action,manifest,removed_object_key,removed_content_sha256)
 values(p_victim_file_id,p_survivor_file_ids,p_derived_file_id,p_action,p_manifest||jsonb_build_object('saved_bytes',p_saved_bytes),p_removed_object_key,p_removed_sha);
 update skillset.compaction_candidates set status='applied',applied_at=now(),estimated_saved_bytes=p_saved_bytes where candidate_id=p_candidate_id;
end $$;

create or replace function public.skillset_mark_physical_deleted(p_file_id text)
returns void language sql security definer set search_path='skillset' as $$
 update skillset.skill_files set physical_deleted_at=now(),updated_at=now() where file_id=p_file_id;
$$;

create or replace function public.skillset_fail_compaction_apply(p_candidate_id bigint,p_error text)
returns void language plpgsql security definer set search_path='skillset' as $$
begin
 update skillset.compaction_candidates set status='rejected',validation=coalesce(validation,'{}'::jsonb)||jsonb_build_object('apply_error',p_error) where candidate_id=p_candidate_id;
end $$;

create or replace function public.skillset_compaction_status()
returns jsonb language sql stable security definer set search_path='skillset','public' as $$
 select jsonb_build_object(
   'enabled',c.enabled,'mode',c.mode,'start_at_bytes',c.start_at_bytes,'aggressive_at_bytes',c.aggressive_at_bytes,'target_bytes',c.target_bytes,
   'active_storage_bytes',coalesce((select sum(compressed_bytes)::bigint from skillset.skill_files where active and content_object_key is not null),0),
   'active_files',(select count(*) from skillset.skill_files where active),
   'compacted_files',(select count(*) from skillset.skill_files where not active and compaction_status='compacted'),
   'profiles',(select count(*) from skillset.compaction_profiles),
   'proposed_candidates',(select count(*) from skillset.compaction_candidates where status='proposed'),
   'validated_candidates',(select count(*) from skillset.compaction_candidates where status='validated'),
   'applied_candidates',(select count(*) from skillset.compaction_candidates where status='applied'),
   'saved_bytes',coalesce((select sum((manifest->>'saved_bytes')::bigint) from skillset.compaction_provenance where manifest ? 'saved_bytes'),0)
 ) from skillset.compaction_state c where c.singleton=true;
$$;

-- Scheduler RPCs use one-time job tokens and are deliberately no-ops below threshold.
create or replace function public.skillset_trigger_compaction_profile(p_force boolean default false)
returns bigint language plpgsql security definer set search_path='public','skillset','extensions' as $$
declare v_token uuid:=gen_random_uuid();v_request_id bigint;v_bytes bigint;v_aggressive bigint;v_limit int;begin
 select coalesce(sum(compressed_bytes)::bigint,0) into v_bytes from skillset.skill_files where active and content_object_key is not null;
 select aggressive_at_bytes into v_aggressive from skillset.compaction_state where singleton=true;
 if not p_force and not public.skillset_compaction_needed() then return 0; end if;
 v_limit:=case when v_bytes>=v_aggressive then 75 else 25 end;
 insert into skillset.job_tokens(token,purpose,expires_at) values(v_token,'compaction-profile',now()+interval '10 minutes');
 select net.http_post(url:='https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-compaction-profile',headers:=jsonb_build_object('Content-Type','application/json'),body:=jsonb_build_object('token',v_token::text,'limit',v_limit,'force',p_force),timeout_milliseconds:=55000) into v_request_id;return v_request_id;end $$;

create or replace function public.skillset_trigger_compaction_analyze(p_force boolean default false)
returns bigint language plpgsql security definer set search_path='public','skillset','extensions' as $$
declare v_token uuid:=gen_random_uuid();v_request_id bigint;v_bytes bigint;v_aggressive bigint;v_limit int;begin
 select coalesce(sum(compressed_bytes)::bigint,0) into v_bytes from skillset.skill_files where active and content_object_key is not null;
 select aggressive_at_bytes into v_aggressive from skillset.compaction_state where singleton=true;
 if not p_force and not public.skillset_compaction_needed() then return 0; end if;
 v_limit:=case when v_bytes>=v_aggressive then 25 else 10 end;
 insert into skillset.job_tokens(token,purpose,expires_at) values(v_token,'compaction-analyze',now()+interval '10 minutes');
 select net.http_post(url:='https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-compaction-analyze',headers:=jsonb_build_object('Content-Type','application/json'),body:=jsonb_build_object('token',v_token::text,'limit',v_limit,'force',p_force),timeout_milliseconds:=55000) into v_request_id;return v_request_id;end $$;

create or replace function public.skillset_trigger_compaction_validate(p_force boolean default false)
returns bigint language plpgsql security definer set search_path='public','skillset','extensions' as $$
declare v_token uuid:=gen_random_uuid();v_request_id bigint;begin
 if not p_force and not public.skillset_compaction_needed() then return 0; end if;
 insert into skillset.job_tokens(token,purpose,expires_at) values(v_token,'compaction-validate',now()+interval '10 minutes');
 select net.http_post(url:='https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-compaction-validate',headers:=jsonb_build_object('Content-Type','application/json'),body:=jsonb_build_object('token',v_token::text,'force',p_force),timeout_milliseconds:=55000) into v_request_id;return v_request_id;end $$;

create or replace function public.skillset_trigger_compaction_apply()
returns bigint language plpgsql security definer set search_path='public','skillset','extensions' as $$
declare v_token uuid:=gen_random_uuid();v_request_id bigint;begin
 if not public.skillset_compaction_needed() then return 0; end if;
 insert into skillset.job_tokens(token,purpose,expires_at) values(v_token,'compaction-apply',now()+interval '10 minutes');
 select net.http_post(url:='https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-compaction-apply',headers:=jsonb_build_object('Content-Type','application/json'),body:=jsonb_build_object('token',v_token::text),timeout_milliseconds:=55000) into v_request_id;return v_request_id;end $$;

do $$ begin
 perform cron.unschedule(jobid) from cron.job where jobname in (
   'skillset-compaction-profile-every-minute','skillset-compaction-analyze-every-minute',
   'skillset-compaction-validate-every-minute','skillset-compaction-apply-every-minute'
 );
end $$;
select cron.schedule('skillset-compaction-profile-every-minute','* * * * *','select public.skillset_trigger_compaction_profile(false);');
select cron.schedule('skillset-compaction-analyze-every-minute','* * * * *','select public.skillset_trigger_compaction_analyze(false);');
select cron.schedule('skillset-compaction-validate-every-minute','* * * * *','select public.skillset_trigger_compaction_validate(false);');
select cron.schedule('skillset-compaction-apply-every-minute','* * * * *','select public.skillset_trigger_compaction_apply();');
