-- Production crawler schedule. Trigger functions own authentication and HTTP dispatch.
-- Apply only after the corresponding public.skillset_trigger_* functions exist.

do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname in (
    'skillset-storage-migration-every-minute',
    'skillset-content-batch-every-minute',
    'skillset-repo-discovery-every-3-minutes',
    'skillset-repo-discovery-every-2-minutes',
    'skillset-repo-discovery-every-minute',
    'skillset-codeload-repo-every-minute',
    'skillset-leaderboard-seed-every-minute',
    'skillset-owner-search-every-minute',
    'skillset-owner-partition-search-every-minute',
    'skillset-global-search-every-minute',
    'skillset-wellknown-content-every-2-minutes',
    'skillset-wellknown-content-every-minute',
    'skillset-page-recovery-every-minute'
  );
end $$;

select cron.schedule('skillset-storage-migration-every-minute','* * * * *','select public.skillset_trigger_storage_migration();');
select cron.schedule('skillset-content-batch-every-minute','* * * * *','select public.skillset_trigger_content_batch();');
select cron.schedule('skillset-repo-discovery-every-minute','* * * * *','select public.skillset_trigger_repo_discovery();');
select cron.schedule('skillset-codeload-repo-every-minute','* * * * *','select public.skillset_trigger_codeload_repo();');
select cron.schedule('skillset-leaderboard-seed-every-minute','* * * * *','select public.skillset_trigger_leaderboard_seed();');
select cron.schedule('skillset-owner-search-every-minute','* * * * *','select public.skillset_trigger_owner_search();');
select cron.schedule('skillset-owner-partition-search-every-minute','* * * * *','select public.skillset_trigger_owner_partition_search();');
select cron.schedule('skillset-global-search-every-minute','* * * * *','select public.skillset_trigger_global_search();');
select cron.schedule('skillset-wellknown-content-every-minute','* * * * *','select public.skillset_trigger_wellknown_content();');
select cron.schedule('skillset-page-recovery-every-minute','* * * * *','select public.skillset_trigger_page_recovery();');
