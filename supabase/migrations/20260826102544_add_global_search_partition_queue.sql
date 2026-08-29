create table if not exists skillset.global_search_partitions (
  token text primary key,
  depth integer not null,
  status text not null default 'pending',
  result_count integer,
  saturated boolean not null default false,
  attempts integer not null default 0,
  last_error text,
  searched_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists global_search_partitions_status_idx
  on skillset.global_search_partitions(status, depth, token);

insert into skillset.global_search_partitions(token, depth)
select a.c || b.c, 2
from unnest(regexp_split_to_array('abcdefghijklmnopqrstuvwxyz0123456789','')) as a(c)
cross join unnest(regexp_split_to_array('abcdefghijklmnopqrstuvwxyz0123456789','')) as b(c)
where a.c <> '' and b.c <> ''
on conflict (token) do nothing;

create or replace function public.skillset_claim_global_search(p_limit integer default 25)
returns table(token text, depth integer)
language sql
security definer
set search_path = skillset, public
as $$
  with c as (
    select g.token
    from skillset.global_search_partitions g
    where g.status in ('pending','retry')
    order by g.depth, g.token
    for update skip locked
    limit greatest(1, least(coalesce(p_limit,25),100))
  ), u as (
    update skillset.global_search_partitions g
       set status='processing', attempts=g.attempts+1, updated_at=now()
      from c
     where g.token=c.token
     returning g.token, g.depth
  ) select * from u;
$$;

create or replace function public.skillset_finish_global_search(
  p_token text,
  p_count integer,
  p_error text default null
) returns void
language plpgsql
security definer
set search_path = skillset, public
as $$
declare
  d integer;
  alphabet text := 'abcdefghijklmnopqrstuvwxyz0123456789';
  i integer;
begin
  select depth into d from skillset.global_search_partitions where token=p_token;
  if p_error is null then
    update skillset.global_search_partitions
       set status='ok', result_count=p_count, saturated=(p_count>=200), last_error=null,
           searched_at=now(), updated_at=now()
     where token=p_token;
    if p_count>=200 and d < 4 then
      for i in 1..length(alphabet) loop
        insert into skillset.global_search_partitions(token,depth)
        values (p_token || substr(alphabet,i,1), d+1)
        on conflict (token) do nothing;
      end loop;
    end if;
  else
    update skillset.global_search_partitions
       set status=case when attempts < 4 then 'retry' else 'failed' end,
           last_error=p_error, updated_at=now()
     where token=p_token;
  end if;
end;
$$;
