alter table skillset.skills add column if not exists recovery_status text;
alter table skillset.skills add column if not exists recovery_error text;
alter table skillset.skills add column if not exists recovery_attempts integer not null default 0;
alter table skillset.skills add column if not exists reconstructed_sha256 text;
alter table skillset.skills add column if not exists reconstructed_bytes integer;
alter table skillset.skills add column if not exists reconstructed_object_key text;
alter table skillset.skills add column if not exists recovery_source_url text;
alter table skillset.skills add column if not exists recovered_at timestamptz;
create index if not exists skills_recovery_status_idx on skillset.skills(recovery_status) where recovery_status is not null;

update skillset.skills set recovery_status='pending',recovery_error=null
where source not like '%/%' and retrieval_status='failed' and recovery_status is null;

create or replace function public.skillset_claim_page_recovery(p_limit integer default 5)
returns table(id text,source text,skill_name text,skill_url text)
language plpgsql security definer set search_path='skillset','public' as $$
begin
 return query
 with picked as (
  select s.id from skillset.skills s
  where s.source not like '%/%' and s.retrieval_status='failed'
    and (s.recovery_status in ('pending','retry') or (s.recovery_status='processing' and s.updated_at<now()-interval '15 minutes'))
  order by s.recovery_attempts,s.id for update skip locked
  limit greatest(1,least(p_limit,10))
 ),claimed as (
  update skillset.skills s set recovery_status='processing',recovery_attempts=recovery_attempts+1,updated_at=now()
  from picked p where s.id=p.id returning s.id,s.source,s.skill_name,s.skill_url
 ) select * from claimed;
end $$;

create or replace function public.skillset_store_reconstructed_content(p_id text,p_sha text,p_bytes integer,p_object_key text,p_source_url text)
returns void language plpgsql security definer set search_path='skillset','public' as $$
begin
 update skillset.skills set recovery_status='ok',recovery_error=null,reconstructed_sha256=p_sha,reconstructed_bytes=p_bytes,
  reconstructed_object_key=p_object_key,recovery_source_url=p_source_url,recovered_at=now(),updated_at=now()
 where id=p_id;
end $$;

create or replace function public.skillset_mark_recovery_error(p_id text,p_error text)
returns void language plpgsql security definer set search_path='skillset','public' as $$
declare v_attempts integer;begin
 select recovery_attempts into v_attempts from skillset.skills where id=p_id for update;
 update skillset.skills set recovery_status=case when coalesce(v_attempts,0)>=2 then 'failed' else 'retry' end,
  recovery_error=left(coalesce(p_error,'unknown_recovery_error'),1500),updated_at=now() where id=p_id;
end $$;