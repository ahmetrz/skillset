create or replace function public.skillset_corpus_status()
returns jsonb
language sql
security definer
set search_path = skillset, public
as $$
select jsonb_build_object(
  'skills_total', (select count(*) from skillset.skills),
  'exact_ok', (select count(*) from skillset.skills where retrieval_status='ok'),
  'exact_unique_hashes', (select count(distinct content_sha256) from skillset.skills where content_sha256 is not null),
  'reconstructed_ok', (select count(*) from skillset.skills where recovery_status='ok'),
  'pending_exact', (select count(*) from skillset.skills where retrieval_status='pending'),
  'failed_exact', (select count(*) from skillset.skills where retrieval_status='failed'),
  'repos_total', (select count(*) from skillset.repositories),
  'repos_ok', (select count(*) from skillset.repositories where discovery_status='ok'),
  'repos_pending', (select count(*) from skillset.repositories where discovery_status='pending'),
  'repos_failed', (select count(*) from skillset.repositories where discovery_status='failed'),
  'codeload_ok', (select count(*) from skillset.repositories where codeload_status='ok'),
  'codeload_pending', (select count(*) from skillset.repositories where codeload_status='pending'),
  'codeload_skipped', (select count(*) from skillset.repositories where codeload_status='skipped'),
  'owners_total', (select count(*) from skillset.owner_search),
  'owners_pending', (select count(*) from skillset.owner_search where status='pending'),
  'global_tokens_total', (select count(*) from skillset.global_search_partitions),
  'global_tokens_ok', (select count(*) from skillset.global_search_partitions where status='ok'),
  'global_tokens_pending', (select count(*) from skillset.global_search_partitions where status='pending'),
  'global_tokens_saturated', (select count(*) from skillset.global_search_partitions where saturated),
  'owner_partition_pending', (select count(*) from skillset.owner_partition_search where status='pending'),
  'exact_storage_bytes', (select coalesce(sum(compressed_bytes),0) from skillset.skills where storage_backend='supabase-storage'),
  'db_bytes', pg_database_size(current_database()),
  'storage_capacity_ok', public.skillset_storage_has_capacity(),
  'generated_at', now()
);
$$;
