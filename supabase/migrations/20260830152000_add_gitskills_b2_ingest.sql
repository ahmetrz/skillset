begin;

create or replace function public.skillset_gitskills_b2_ingest_finish_v1(
  p_path text,
  p_representatives integer
) returns void
language plpgsql
security definer
set search_path = pg_catalog, public, skillset
as $$
begin
  if p_path !~ '^gitskills/discovery-b2-v[12]/pack-[0-9-]+\.json\.gz$'
     or p_representatives < 0 then
    raise exception 'invalid_b2_ingest_metadata';
  end if;
  insert into skillset.gitskills_analysis_queue_v1(storage_path,status,representatives,updated_at)
  values (p_path,'pending',p_representatives,now())
  on conflict(storage_path) do update
    set representatives=excluded.representatives,
        status=case when skillset.gitskills_analysis_queue_v1.status='done' then 'done' else 'pending' end,
        error=null,
        updated_at=now();
end
$$;

revoke all on function public.skillset_gitskills_b2_ingest_finish_v1(text,integer) from public, anon, authenticated;
grant execute on function public.skillset_gitskills_b2_ingest_finish_v1(text,integer) to service_role;

commit;
