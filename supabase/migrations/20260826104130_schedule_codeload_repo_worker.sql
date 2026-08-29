create or replace function public.skillset_trigger_codeload_repo()
returns bigint
language plpgsql
security definer
set search_path = skillset, public, extensions
as $$
declare
  t uuid:=gen_random_uuid();
  req_id bigint;
begin
  if not public.skillset_storage_has_capacity() then return 0; end if;
  insert into skillset.job_tokens(token,purpose,expires_at)
  values(t,'codeload-repo',now()+interval '10 minutes');
  select net.http_post(
    url := 'https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-codeload-repo',
    headers := jsonb_build_object('Content-Type','application/json'),
    body := jsonb_build_object('token',t::text,'limit',5),
    timeout_milliseconds := 60000
  ) into req_id;
  return req_id;
end;
$$;

do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname='skillset-codeload-repo-every-minute';
exception when others then null;
end $$;
select cron.schedule('skillset-codeload-repo-every-minute','* * * * *','select public.skillset_trigger_codeload_repo();');
