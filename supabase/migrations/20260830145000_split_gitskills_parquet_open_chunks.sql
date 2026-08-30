begin;

-- Preserve the original open queue rows so the repair is auditable and
-- reversible. Completed rows and their storage objects are never modified.
create table if not exists skillset.gitskills_parquet_chunks_v1_recovery_20260830
  (like skillset.gitskills_parquet_chunks_v1 including all);

insert into skillset.gitskills_parquet_chunks_v1_recovery_20260830
select *
from skillset.gitskills_parquet_chunks_v1
where status <> 'done'
on conflict (file_idx, row_start) do nothing;

delete from skillset.gitskills_parquet_chunks_v1
where status <> 'done';

-- Exact-content rows can be large. Keep each Edge invocation safely below the
-- hosted 256 MB / 2 s CPU envelope while retaining the original row offsets.
insert into skillset.gitskills_parquet_chunks_v1 (
  file_idx,
  file_url,
  file_size,
  global_start,
  row_start,
  row_end,
  status,
  attempts,
  claimed_at,
  finished_at,
  representatives,
  storage_path,
  error,
  created_at,
  updated_at
)
select
  source.file_idx,
  source.file_url,
  source.file_size,
  source.global_start,
  part.row_start,
  least(part.row_start + 250, source.row_end),
  'pending',
  0,
  null,
  null,
  null,
  null,
  null,
  now(),
  now()
from skillset.gitskills_parquet_chunks_v1_recovery_20260830 as source
cross join lateral generate_series(
  source.row_start,
  source.row_end - 1,
  250
) as part(row_start)
on conflict (file_idx, row_start) do nothing;

commit;
