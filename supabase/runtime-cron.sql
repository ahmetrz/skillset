-- Production crawler schedule. Trigger functions own authentication and HTTP dispatch.
-- Apply only after the corresponding public.skillset_trigger_* functions exist.

do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname in (
    'skillset-storage-migration-every-minute',
    'skillset-content-batch-every-minute',
    'skillset-repo-discovery-every-3-minutes',
    'skillset-leaderboard-seed-every-minute',
    'skillset-owner-search-every-minute'
  );
end $$;

select cron.schedule('skillset-storage-migration-every-minute','* * * * *','select public.skillset_trigger_storage_migration();');
select cron.schedule('skillset-content-batch-every-minute','* * * * *','select public.skillset_trigger_content_batch();');
select cron.schedule('skillset-repo-discovery-every-3-minutes','*/3 * * * *','select public.skillset_trigger_repo_discovery();');
select cron.schedule('skillset-leaderboard-seed-every-minute','* * * * *','select public.skillset_trigger_leaderboard_seed();');
select cron.schedule('skillset-owner-search-every-minute','* * * * *','select public.skillset_trigger_owner_search();');
