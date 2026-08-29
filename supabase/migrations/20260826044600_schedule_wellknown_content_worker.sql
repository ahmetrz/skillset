create or replace function public.skillset_trigger_wellknown_content()
returns bigint
language plpgsql
security definer
set search_path to 'public','skillset','extensions'
as $function$
declare v_token uuid:=gen_random_uuid();v_request_id bigint;begin
 if not public.skillset_storage_has_capacity() then return 0; end if;
 insert into skillset.job_tokens(token,purpose,expires_at) values(v_token,'wellknown-content',now()+interval '10 minutes');
 select net.http_post(
   url:='https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-wellknown-content',
   headers:=jsonb_build_object('Content-Type','application/json'),
   body:=jsonb_build_object('token',v_token::text,'limit',10)
 ) into v_request_id;
 return v_request_id;
end;
$function$;

do $$ declare v_jobid bigint; begin
 select jobid into v_jobid from cron.job where jobname='skillset-wellknown-content-every-2-minutes';
 if v_jobid is not null then perform cron.unschedule(v_jobid); end if;
end $$;
select cron.schedule('skillset-wellknown-content-every-2-minutes','*/2 * * * *','select public.skillset_trigger_wellknown_content();');