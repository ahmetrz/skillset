alter table skillset.skills add column if not exists v1_status text;
alter table skillset.skills add column if not exists v1_attempts integer not null default 0;
alter table skillset.skills add column if not exists v1_error text;
alter table skillset.skills add column if not exists v1_checked_at timestamptz;
create index if not exists skills_v1_status_idx on skillset.skills(v1_status) where v1_status is not null;
update skillset.skills set v1_status='pending' where retrieval_status<>'ok' and v1_status is null;

create or replace function public.skillset_claim_v1_detail_jobs(p_limit integer default 250)
returns table(id text) language plpgsql security definer set search_path='skillset','public' as $$
begin return query
 with picked as (
  select s.id from skillset.skills s where s.retrieval_status<>'ok'
    and (s.v1_status is null or s.v1_status in ('pending','retry') or (s.v1_status='processing' and s.updated_at<now()-interval '20 minutes'))
    and s.v1_attempts<3
  order by case when s.retrieval_status='failed' then 0 else 1 end,case when s.recovery_status='ok' then 0 else 1 end,s.v1_attempts,s.first_seen_at,s.id
  for update skip locked limit greatest(1,least(p_limit,300))
 ),claimed as (
  update skillset.skills s set v1_status='processing',v1_attempts=v1_attempts+1,updated_at=now()
  from picked p where s.id=p.id returning s.id
 ) select * from claimed; end $$;

create or replace function public.skillset_mark_v1_success(p_id text)
returns void language sql security definer set search_path='skillset','public' as $$
 update skillset.skills set v1_status='ok',v1_error=null,v1_checked_at=now(),updated_at=now() where id=p_id;
$$;

create or replace function public.skillset_mark_v1_error(p_id text,p_error text,p_status integer default null)
returns void language plpgsql security definer set search_path='skillset','public' as $$
declare a integer;begin
 select v1_attempts into a from skillset.skills where id=p_id for update;
 update skillset.skills set v1_status=case when p_status=404 then 'failed' when coalesce(a,0)>=3 then 'failed' else 'retry' end,
  v1_error=left(coalesce(p_error,'v1_error'),1000),v1_checked_at=now(),updated_at=now() where id=p_id;
end $$;