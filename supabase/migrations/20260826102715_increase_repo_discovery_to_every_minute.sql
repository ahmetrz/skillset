do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname in ('skillset-repo-discovery-every-2-minutes','skillset-repo-discovery-every-minute');
exception when others then null;
end $$;
select cron.schedule('skillset-repo-discovery-every-minute','* * * * *','select public.skillset_trigger_repo_discovery();');
