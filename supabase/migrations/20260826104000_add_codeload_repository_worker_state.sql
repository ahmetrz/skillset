alter table skillset.repositories add column if not exists codeload_status text not null default 'pending';
alter table skillset.repositories add column if not exists codeload_attempts integer not null default 0;
alter table skillset.repositories add column if not exists codeload_error text;
alter table skillset.repositories add column if not exists codeload_scanned_at timestamptz;
create index if not exists repositories_codeload_status_idx on skillset.repositories(codeload_status, codeload_attempts, source);
create index if not exists skills_source_idx on skillset.skills(source);

create or replace function public.skillset_claim_codeload_repositories(p_limit integer default 2)
returns table(source text, owner text, repo text)
language plpgsql
security definer
set search_path to skillset, public
as $$
begin
  return query
  with picked as (
    select r.source
    from skillset.repositories r
    where r.codeload_status in ('pending','retry')
      and r.discovery_status in ('pending','retry')
    order by coalesce((select max(s.installs) from skillset.skills s where s.source=r.source),0) desc,
             r.codeload_attempts,
             r.source
    for update skip locked
    limit greatest(1,least(coalesce(p_limit,2),10))
  ), claimed as (
    update skillset.repositories r
       set codeload_status='processing', codeload_attempts=codeload_attempts+1, updated_at=now()
      from picked p
     where r.source=p.source
     returning r.source,r.owner,r.repo
  ) select * from claimed;
end;
$$;

create or replace function public.skillset_finish_codeload_repository(
  p_source text,
  p_branch text,
  p_files integer,
  p_new integer,
  p_error text default null,
  p_terminal boolean default false
) returns void
language plpgsql
security definer
set search_path to skillset, public
as $$
begin
  if p_error is null then
    update skillset.repositories
       set codeload_status='ok', codeload_error=null, codeload_scanned_at=now(),
           discovery_status='ok', default_branch=coalesce(p_branch,default_branch),
           discovered_skill_files=greatest(discovered_skill_files,coalesce(p_files,0)),
           new_skills_found=new_skills_found+coalesce(p_new,0),
           last_error=null,last_scanned_at=now(),updated_at=now()
     where source=p_source;
  else
    update skillset.repositories
       set codeload_status=case when p_terminal or codeload_attempts>=2 then 'skipped' else 'retry' end,
           codeload_error=p_error,codeload_scanned_at=now(),updated_at=now()
     where source=p_source;
  end if;
end;
$$;
