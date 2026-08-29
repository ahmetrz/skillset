create or replace function public.skillset_trigger_content_batch()
returns bigint
language plpgsql
security definer
set search_path to public, skillset, extensions
as $$
declare
  v_token uuid:=gen_random_uuid();
  v_request_id bigint;
begin
  if not public.skillset_storage_has_capacity() then return 0; end if;
  insert into skillset.job_tokens(token,purpose,expires_at)
  values(v_token,'content-batch',now()+interval '10 minutes');
  select net.http_post(
    url:='https://elnsqdpbxjcrudvwdzbs.supabase.co/functions/v1/skillset-content-batch',
    headers:=jsonb_build_object('Content-Type','application/json'),
    body:=jsonb_build_object('token',v_token::text,'limit',40)
  ) into v_request_id;
  return v_request_id;
end;
$$;
