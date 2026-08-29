create or replace function public.skillset_consume_job_token(p_token text,p_purpose text)
returns boolean
language plpgsql
security definer
set search_path to 'public','skillset'
as $function$
declare v_token uuid;v_count integer:=0;begin
  delete from skillset.job_tokens where expires_at < now()-interval '1 day';
  begin v_token:=p_token::uuid; exception when others then return false; end;
  update skillset.job_tokens set used_at=now()
  where token=v_token and purpose=p_purpose and used_at is null and expires_at>now();
  get diagnostics v_count=row_count;
  return v_count=1;
end;$function$;