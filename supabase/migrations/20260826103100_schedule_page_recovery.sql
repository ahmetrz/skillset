create or replace function public.skillset_trigger_page_recovery()
returns bigint language plpgsql security definer set search_path='public','skillset','extensions' as $$
declare v_token uuid:=gen_random_uuid();v_request_id bigint;begin
 insert into skillset.job_tokens(token,purpose,expires_at) values(v_token,'page-recovery',now()+interval '10 minutes');
 select net.http_post(
  url:='https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-page-recovery',
  headers:=jsonb_build_object('Content-Type','application/json'),
  body:=jsonb_build_object('token',v_token::text,'limit',5)
 ) into v_request_id;
 return v_request_id;
end $$;

do $$ begin
 perform cron.unschedule(jobid) from cron.job where jobname='skillset-page-recovery-every-minute';
exception when others then null; end $$;
select cron.schedule('skillset-page-recovery-every-minute','* * * * *','select public.skillset_trigger_page_recovery();');