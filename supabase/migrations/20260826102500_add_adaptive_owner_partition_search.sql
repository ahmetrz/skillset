create table if not exists skillset.owner_partition_search (
  owner text not null,
  token text not null,
  depth integer not null default 2,
  status text not null default 'pending',
  result_count integer,
  saturated boolean not null default false,
  attempts integer not null default 0,
  last_error text,
  searched_at timestamptz,
  updated_at timestamptz not null default now(),
  primary key(owner, token)
);
create index if not exists owner_partition_status_idx on skillset.owner_partition_search(status, attempts, owner, token);

create or replace function public.skillset_seed_saturated_partitions()
returns integer language plpgsql security definer set search_path='skillset','public' as $$
declare v_count integer;
begin
  with names as (
    select distinct s.owner, lower(regexp_replace(coalesce(s.skill_name,''),'[^a-z0-9]+','','g')) as n
    from skillset.skills s join skillset.owner_search o on o.owner=s.owner
    where o.saturated=true and length(regexp_replace(coalesce(s.skill_name,''),'[^a-z0-9]+','','g'))>=2
  ), grams as (
    select distinct owner, substr(n,g,2) token
    from names cross join lateral generate_series(1,greatest(length(n)-1,1)) g
    where length(substr(n,g,2))=2
  )
  insert into skillset.owner_partition_search(owner,token,depth)
  select owner,token,2 from grams on conflict(owner,token) do nothing;
  get diagnostics v_count=row_count; return v_count;
end $$;

create or replace function public.skillset_claim_owner_partitions(p_limit integer default 10)
returns table(owner text, token text, depth integer) language plpgsql security definer set search_path='skillset','public' as $$
begin
  return query
  with picked as (
    select q.owner,q.token from skillset.owner_partition_search q
    where q.status in ('pending','retry') or (q.status='processing' and q.updated_at<now()-interval '10 minutes')
    order by q.attempts,q.depth,q.owner,q.token for update skip locked
    limit greatest(1,least(p_limit,25))
  ), claimed as (
    update skillset.owner_partition_search q set status='processing',attempts=attempts+1,updated_at=now()
    from picked p where q.owner=p.owner and q.token=p.token returning q.owner,q.token,q.depth
  ) select * from claimed;
end $$;

create or replace function public.skillset_finish_owner_partition(p_owner text,p_token text,p_count integer,p_error text default null)
returns void language plpgsql security definer set search_path='skillset','public' as $$
begin
  update skillset.owner_partition_search set
    status=case when p_error is null then 'ok' when attempts>=3 then 'failed' else 'retry' end,
    result_count=p_count,saturated=coalesce(p_count,0)>=200,last_error=p_error,searched_at=now(),updated_at=now()
  where owner=p_owner and token=p_token;
end $$;

create or replace function public.skillset_expand_owner_partition(p_owner text,p_parent_token text,p_rows jsonb)
returns integer language plpgsql security definer set search_path='skillset','public' as $$
declare v_count integer:=0; v_parent_len integer:=length(p_parent_token); v_new integer;
begin
  perform public.skillset_seed_search_results(p_rows);
  with names as (
    select distinct lower(regexp_replace(coalesce(r->>'skillId',r->>'name',''),'[^a-z0-9]+','','g')) n
    from jsonb_array_elements(p_rows) r
    where length(regexp_replace(coalesce(r->>'skillId',r->>'name',''),'[^a-z0-9]+','','g'))>=2
  ), grams as (
    select distinct substr(n,g,2) token from names cross join lateral generate_series(1,greatest(length(n)-1,1)) g
    where length(substr(n,g,2))=2
  )
  insert into skillset.owner_partition_search(owner,token,depth)
  select p_owner,token,2 from grams on conflict(owner,token) do nothing;
  get diagnostics v_new=row_count; v_count:=v_count+v_new;
  if jsonb_array_length(p_rows)>=200 and v_parent_len<4 then
    with names as (
      select distinct lower(regexp_replace(coalesce(r->>'skillId',r->>'name',''),'[^a-z0-9]+','','g')) n
      from jsonb_array_elements(p_rows) r
      where length(regexp_replace(coalesce(r->>'skillId',r->>'name',''),'[^a-z0-9]+','','g'))>=v_parent_len+1
    ), grams as (
      select distinct substr(n,g,v_parent_len+1) token from names cross join lateral generate_series(1,greatest(length(n)-v_parent_len,1)) g
      where length(substr(n,g,v_parent_len+1))=v_parent_len+1
    )
    insert into skillset.owner_partition_search(owner,token,depth)
    select p_owner,token,v_parent_len+1 from grams on conflict(owner,token) do nothing;
    get diagnostics v_new=row_count; v_count:=v_count+v_new;
  end if;
  return v_count;
end $$;

select public.skillset_seed_saturated_partitions();