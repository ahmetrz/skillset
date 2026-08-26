create or replace function public.skillset_claim_v1_detail_jobs(p_limit integer default 250)
returns table(id text)
language plpgsql
security definer
set search_path to skillset, public
as $$
begin
 return query
 with picked as (
  select s.id from skillset.skills s
  where s.retrieval_status<>'ok'
    and (s.install_url is not null or s.source_type is not null or s.recovery_status='ok')
    and (s.v1_status is null or s.v1_status in ('pending','retry') or (s.v1_status='processing' and s.updated_at<now()-interval '20 minutes'))
    and s.v1_attempts<3
  order by case when s.install_url is not null or s.source_type is not null then 0 else 1 end,
           case when s.recovery_status='ok' then 0 else 1 end,
           case when s.retrieval_status='failed' then 0 else 1 end,
           s.v1_attempts,s.first_seen_at,s.id
  for update skip locked limit greatest(1,least(p_limit,300))
 ),claimed as (
  update skillset.skills s set v1_status='processing',v1_attempts=v1_attempts+1,updated_at=now()
  from picked p where s.id=p.id returning s.id
 ) select * from claimed;
end
$$;
