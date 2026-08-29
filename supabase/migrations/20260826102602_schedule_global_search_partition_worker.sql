create or replace function public.skillset_trigger_global_search()
returns bigint
language plpgsql
security definer
set search_path = skillset, public, extensions
as $$
declare
  t uuid := gen_random_uuid();
  req_id bigint;
begin
  insert into skillset.job_tokens(token,purpose,expires_at)
  values (t,'global-search',now()+interval '10 minutes');

  select net.http_post(
    url := 'https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-global-search',
    headers := jsonb_build_object('Content-Type','application/json'),
    body := jsonb_build_object('token',t::text,'limit',25)
  ) into req_id;
  return req_id;
end;
$$;

do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname='skillset-global-search-every-minute';
exception when others then null;
end $$;
select cron.schedule('skillset-global-search-every-minute','* * * * *','select public.skillset_trigger_global_search();');
